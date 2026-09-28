//! Round-trip tests against a real PostgreSQL server.
//!
//! Ignored by default — they need a reachable database, so `cargo test` on a
//! bare checkout must not try them. Run explicitly:
//!
//! ```text
//! PGHOST=localhost PGDATABASE=postgres PGUSER=postgres PGPASSWORD=secret \
//!   cargo test --test live_database -- --ignored
//! ```
//!
//! CI's `integration` job covers the full contract (apply both fixture schemas,
//! compare, apply the diff, compare again → empty) across PostgreSQL 14–19 in
//! `.github/workflows/rust.yml`. These are the equivalent local smoke tests.

mod common;

use common::{ScratchDir, assert_no_ddl};
use pgc::comparer::core::Comparer;
use pgc::config::dump_config::DumpConfig;
use pgc::config::grants_mode::GrantsMode;
use pgc::dump::core::Dump;

/// Connection details from the standard `PG*` environment variables, with the
/// same defaults as the `pgc` CLI.
fn env_config(file: &str) -> DumpConfig {
    let var =
        |name: &str, fallback: &str| std::env::var(name).unwrap_or_else(|_| fallback.to_string());
    DumpConfig {
        host: var("PGHOST", "localhost"),
        port: var("PGPORT", "5432"),
        user: var("PGUSER", "postgres"),
        password: var("PGPASSWORD", ""),
        database: var("PGDATABASE", "postgres"),
        scheme: var("PGC_TEST_SCHEME", "public"),
        ssl: false,
        file: file.to_string(),
    }
}

#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn dump_of_a_live_database_round_trips_through_a_file() {
    let dir = ScratchDir::new("live-roundtrip");
    let path = dir.path_str("live.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.process(8).await.expect("dump the live database");

    let reloaded = Dump::read_from_file(&path)
        .await
        .expect("read back the dump pgc just wrote");
    assert_eq!(dump.get_info(), reloaded.get_info());
}

/// The idempotency contract at the database level: a schema compared against
/// itself needs no migration.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn live_database_compared_against_itself_emits_no_ddl() {
    let dir = ScratchDir::new("live-self");
    let path = dir.path_str("live.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.process(8).await.expect("dump the live database");

    let from = Dump::read_from_file(&path).await.expect("read FROM");
    let to = Dump::read_from_file(&path).await.expect("read TO");

    let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Full);
    comparer.compare().await.expect("compare");

    let out = dir.path_str("output.sql");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");

    assert_no_ddl(&script, "self-comparison of a live database");
}

#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn inspect_populates_a_dump_without_writing_a_file() {
    let dir = ScratchDir::new("live-inspect");
    let path = dir.path_str("never-written.dump");

    let mut dump = Dump::new(env_config(&path));
    dump.inspect(8).await.expect("inspect the live database");

    assert!(
        !dump.schemas.is_empty(),
        "inspect must find at least one schema"
    );
    assert!(
        !std::path::Path::new(&path).exists(),
        "inspect must not write the dump file"
    );
    // The clear script is generated from an inspected dump, so it must be
    // buildable straight after.
    assert!(!dump.generate_clear_script(true, true, false).is_empty());
}

/// A database set to `standard_conforming_strings = off` (possible up to
/// PostgreSQL 18) makes the server deparse every backslash doubled. pgc's
/// connections set it back to `on`, like pg_dump, so the dump carries the
/// definition in the only form PostgreSQL 19 accepts.
///
/// Needs a database-creating connection (CREATEDB or superuser); skipped on
/// PostgreSQL 19+, which cannot turn the setting off.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn dump_reads_standard_literals_from_a_database_with_scs_off() {
    const DB: &str = "pgc_live_scs_off";

    let admin = env_config("");
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");
    let version: i32 = sqlx::query_scalar("select current_setting('server_version_num')::int4")
        .fetch_one(&admin_pool)
        .await
        .expect("server version");
    if version >= 190000 {
        eprintln!("skipped: PostgreSQL 19+ cannot set standard_conforming_strings = off");
        return;
    }

    sqlx::query(&format!("drop database if exists {DB} with (force)"))
        .execute(&admin_pool)
        .await
        .expect("drop leftover database");
    sqlx::query(&format!("create database {DB}"))
        .execute(&admin_pool)
        .await
        .expect("create scratch database");

    let dir = ScratchDir::new("live-scs-off");
    let mut config = env_config(&dir.path_str("scs.dump"));
    config.database = DB.to_string();
    config.scheme = "public".to_string();

    // Plain pool, no pgc session setup: shows the server really does double
    // the backslash for this database, so the assertion below is not vacuous.
    let raw_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(config.get_connect_options().expect("scratch configuration"))
        .await
        .expect("connect to the scratch database");
    // E'' literals parse the same under either setting.
    sqlx::query(r"create table public.scs_probe (id integer, path text default E'C:\\temp')")
        .execute(&raw_pool)
        .await
        .expect("create probe table");
    sqlx::query(&format!(
        "alter database {DB} set standard_conforming_strings = off"
    ))
    .execute(&admin_pool)
    .await
    .expect("turn standard_conforming_strings off for the database");
    raw_pool.close().await;
    let raw_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(config.get_connect_options().expect("scratch configuration"))
        .await
        .expect("reconnect to the scratch database");
    let raw_default: String = sqlx::query_scalar(
        "select pg_get_expr(d.adbin, d.adrelid) from pg_attrdef d \
         where d.adrelid = 'public.scs_probe'::regclass",
    )
    .fetch_one(&raw_pool)
    .await
    .expect("deparse the default without pgc's session setup");
    raw_pool.close().await;

    let mut dump = Dump::new(config);
    let result = dump.inspect(4).await;

    // Clean up before asserting, so a failure leaves nothing behind.
    let _ = sqlx::query(&format!("drop database if exists {DB} with (force)"))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;
    result.expect("inspect the scratch database");

    assert_eq!(raw_default, r"'C:\\temp'::text", "server-side off doubles");
    let table = dump
        .tables
        .iter()
        .find(|t| t.name == "scs_probe")
        .expect("probe table dumped");
    let column = table
        .columns
        .iter()
        .find(|c| c.name == "path")
        .expect("path column dumped");
    assert_eq!(column.column_default.as_deref(), Some(r"'C:\temp'::text"));
}

/// The other half of the test above. pgc's dump now holds `'C:\temp'` in
/// standard form, and a migration applied as plain `psql -f` to a PostgreSQL
/// 14–18 database whose clients default to `standard_conforming_strings =
/// off` would read it as an escape string and store `C:`, a tab and `emp`.
/// pgc records that default in the FROM dump and starts the migration with
/// `set standard_conforming_strings = on;`, as a `pg_dump` script does. Here
/// both databases are set to `off`, and the migration is applied over a plain
/// connection that inherits the database setting, as psql's would.
///
/// Needs a database-creating connection (CREATEDB or superuser); skipped on
/// PostgreSQL 19+, which cannot turn the setting off.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn migration_keeps_backslash_literals_on_a_database_with_scs_off() {
    const FROM_DB: &str = "pgc_live_scs_from";
    const TO_DB: &str = "pgc_live_scs_to";

    let admin = env_config("");
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");
    let version: i32 = sqlx::query_scalar("select current_setting('server_version_num')::int4")
        .fetch_one(&admin_pool)
        .await
        .expect("server version");
    if version >= 190000 {
        eprintln!("skipped: PostgreSQL 19+ cannot set standard_conforming_strings = off");
        return;
    }

    let dir = ScratchDir::new("live-scs-apply");
    let config_for = |db: &str| {
        let mut config = env_config(&dir.path_str(&format!("{db}.dump")));
        config.database = db.to_string();
        config.scheme = "public".to_string();
        config
    };
    // A plain pool without pgc's session setup: what psql would connect with.
    let raw_pool = |db: &str| {
        let options = config_for(db)
            .get_connect_options()
            .expect("scratch configuration");
        async move {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .expect("connect to a scratch database")
        }
    };

    for (db, table) in [
        (
            FROM_DB,
            "create table public.scs_probe (id integer primary key, path text)",
        ),
        (
            TO_DB,
            // E'' parses the same under either setting.
            r"create table public.scs_probe (id integer primary key, path text default E'C:\\temp')",
        ),
    ] {
        for statement in [
            format!("drop database if exists {db} with (force)"),
            format!("create database {db}"),
            format!("alter database {db} set standard_conforming_strings = off"),
        ] {
            sqlx::query(&statement)
                .execute(&admin_pool)
                .await
                .expect("prepare a scratch database");
        }
        let pool = raw_pool(db).await;
        sqlx::query(table)
            .execute(&pool)
            .await
            .expect("create the probe table");
        pool.close().await;
    }

    // Everything that can fail runs before the cleanup and reports an Err,
    // so a failure still drops the scratch databases.
    let outcome: Result<(String, String, String), String> = async {
        let err = |what: &str| {
            let what = what.to_string();
            move |e: std::io::Error| format!("{what}: {e}")
        };
        let mut from = Dump::new(config_for(FROM_DB));
        from.inspect(4).await.map_err(err("inspect FROM"))?;
        let mut to = Dump::new(config_for(TO_DB));
        to.inspect(4).await.map_err(err("inspect TO"))?;
        if !from.standard_conforming_strings_off {
            return Err("the FROM default was not recorded as off".to_string());
        }

        let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
        comparer.compare().await.map_err(err("compare"))?;
        let out = dir.path_str("output.sql");
        comparer.save_script(&out).await.map_err(err("save"))?;
        let script = std::fs::read_to_string(&out).map_err(err("read"))?;

        // One statement at a time on one session, as psql sends a file. The
        // server lexes a multi-statement query string as a whole, before its
        // leading SET has run, so sending the file as one string would not
        // show what psql does. This script has no dollar quotes, so a split
        // at each line-ending `;` is exact.
        let pool = raw_pool(FROM_DB).await;
        let mut conn = pool.acquire().await.map_err(|e| format!("acquire: {e}"))?;
        for statement in script.split_inclusive(";\n") {
            if statement.trim().is_empty() {
                continue;
            }
            sqlx::raw_sql(statement)
                .execute(&mut *conn)
                .await
                .map_err(|e| format!("apply `{statement}`: {e}"))?;
        }
        let stored: String =
            sqlx::query_scalar("insert into public.scs_probe (id) values (1) returning path")
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| format!("insert a default: {e}"))?;
        drop(conn);
        pool.close().await;

        // Converged: the migrated database compares equal to TO.
        let mut from = Dump::new(config_for(FROM_DB));
        from.inspect(4).await.map_err(err("re-inspect FROM"))?;
        let mut to = Dump::new(config_for(TO_DB));
        to.inspect(4).await.map_err(err("re-inspect TO"))?;
        let mut comparer = Comparer::new(from, to, true, false, false, GrantsMode::Ignore);
        comparer.compare().await.map_err(err("compare again"))?;
        comparer.save_script(&out).await.map_err(err("save"))?;
        let second = std::fs::read_to_string(&out).map_err(err("read"))?;
        Ok((script, stored, second))
    }
    .await;

    // Clean up before asserting, so a failure leaves nothing behind.
    for db in [FROM_DB, TO_DB] {
        let _ = sqlx::query(&format!("drop database if exists {db} with (force)"))
            .execute(&admin_pool)
            .await;
    }
    admin_pool.close().await;

    let (script, stored, second) = outcome.expect("migrate the scs=off database");
    assert!(
        script.starts_with("set standard_conforming_strings = on;"),
        "{script}"
    );
    assert!(script.contains(r"'C:\temp'::text"), "{script}");
    assert_eq!(
        stored, r"C:\temp",
        "a tab here means the literal was misread"
    );
    assert_no_ddl(&second, "a second compare after the migration");
}

/// Issue #244: a password full of URL-reserved characters must reach the
/// server as typed.
///
/// The unit tests can only show that the credential is not rewritten on its
/// way into `PgConnectOptions`. Whether the server accepts it is a question
/// only the server can answer, and it is the question that was actually
/// wrong: `#`, `/` and `?` used to fail as `invalid port number` before any
/// socket was opened, and `pass%41word` used to authenticate as `passAword`.
///
/// Needs a role-creating connection (the `PG*` user must have CREATEROLE or
/// be a superuser), which is what the documented local and CI setups use.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn a_password_of_reserved_characters_authenticates() {
    // Every character the URL form mangled, in one password: the three that
    // ended the authority section, the escape that was silently decoded, and
    // the two that survived by luck.
    const PASSWORD: &str = "p#ss/w?rd%41@x%";
    const ROLE: &str = "pgc_issue244_role";

    let admin = env_config("");
    let mut admin_dump = Dump::new(admin.clone());
    admin_dump
        .inspect(4)
        .await
        .expect("connect to the live database as the configured user");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");

    // `CREATE ROLE` takes the password as a plain SQL literal, so only the
    // single quotes need escaping — this is not a URL either.
    sqlx::query(&format!(r#"drop role if exists {ROLE}"#))
        .execute(&pool)
        .await
        .expect("clean up any leftover role");
    sqlx::query(&format!(
        r#"create role {ROLE} login password '{}'"#,
        PASSWORD.replace('\'', "''")
    ))
    .execute(&pool)
    .await
    .expect("create the test role");
    let dir = ScratchDir::new("live-issue244");
    let mut as_role = env_config(&dir.path_str("issue244.dump"));
    as_role.user = ROLE.to_string();
    as_role.password = PASSWORD.to_string();

    let result = Dump::new(as_role).inspect(4).await;

    // Drop the role before asserting, so a failure does not leave it behind.
    let _ = sqlx::query(&format!(r#"drop role if exists {ROLE}"#))
        .execute(&pool)
        .await;
    pool.close().await;

    result.expect("a password of URL-reserved characters must authenticate");
}

// ==== pre-clear tests ====

/// `pgc --command clear` without `--use-cascade`, on a schema whose drops
/// cross the script's sections: a domain whose CHECK calls a function, a
/// domain over a domain, an `INHERITS` child listed after its parent, an
/// aggregate over a plpgsql state function, a cast and an event trigger on
/// in-schema functions, and two dependency cycles (a CHECK calling a
/// SQL-standard function that reads its own table, a rule reading a view over
/// its own table). `inspect` reads the order from `pg_depend`, and the script
/// applies in one transaction and leaves nothing behind.
///
/// Needs a database-creating connection (CREATEDB or superuser; the event
/// trigger needs a superuser).
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn clear_script_drops_cross_section_dependencies_without_cascade() {
    const DB: &str = "pgc_live_clear";

    let admin = env_config("");
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");
    sqlx::query(&format!("drop database if exists {DB} with (force)"))
        .execute(&admin_pool)
        .await
        .expect("drop leftover database");
    sqlx::query(&format!("create database {DB}"))
        .execute(&admin_pool)
        .await
        .expect("create scratch database");

    let mut config = env_config("");
    config.database = DB.to_string();
    config.scheme = "live_clear".to_string();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(config.get_connect_options().expect("scratch configuration"))
        .await
        .expect("connect to the scratch database");
    sqlx::raw_sql(
        "create schema live_clear;
         create function live_clear.is_sku(t text) returns boolean
             language sql immutable as $$ select t ~ '^[A-Z]{3}-[0-9]+$' $$;
         create domain live_clear.sku as text check (live_clear.is_sku(value));
         create domain live_clear.base_txt as text check (length(value) < 100);
         create domain live_clear.code as live_clear.base_txt check (value <> '');
         create table live_clear.a_parent (id integer);
         create table live_clear.b_child (extra integer) inherits (live_clear.a_parent);
         create function live_clear.sfunc(state bigint, v integer) returns bigint
             language plpgsql immutable as $$ begin return state + v; end $$;
         create aggregate live_clear.total(integer)
             (sfunc = live_clear.sfunc, stype = bigint, initcond = '0');
         create type live_clear.mood as enum ('ok', 'meh');
         create function live_clear.mood_text(live_clear.mood) returns text
             language sql immutable as $$ select $1::text $$;
         create cast (live_clear.mood as text) with function live_clear.mood_text(live_clear.mood);
         create function live_clear.on_ddl() returns event_trigger
             language plpgsql as $$ begin end $$;
         create event trigger live_clear_on_ddl on ddl_command_start
             execute function live_clear.on_ddl();
         create table live_clear.cycle (x integer);
         create function live_clear.cycle_max() returns integer language sql stable
             begin atomic select max(x) from live_clear.cycle; end;
         alter table live_clear.cycle add constraint cycle_chk
             check (x is null or live_clear.cycle_max() is null or x > -1000);
         create table live_clear.src (id integer);
         create table live_clear.log (n bigint);
         create view live_clear.src_count as select count(*) as n from live_clear.src;
         create rule src_log as on insert to live_clear.src
             do also insert into live_clear.log select n from live_clear.src_count;",
    )
    .execute(&pool)
    .await
    .expect("create the fixture");

    let mut dump = Dump::new(config);
    let inspected = dump.inspect(4).await;
    let script = dump.generate_clear_script(true, false, false);
    let applied = sqlx::raw_sql(&script).execute(&pool).await;
    let left: Option<i64> =
        sqlx::query_scalar("select count(*) from pg_namespace where nspname = 'live_clear'")
            .fetch_one(&pool)
            .await
            .ok();
    let event_triggers: Option<i64> = sqlx::query_scalar(
        "select count(*) from pg_event_trigger where evtname = 'live_clear_on_ddl'",
    )
    .fetch_one(&pool)
    .await
    .ok();

    // Clean up before asserting, so a failure leaves nothing behind.
    pool.close().await;
    let _ = sqlx::query(&format!("drop database if exists {DB} with (force)"))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;

    inspected.expect("inspect the scratch database");
    assert!(!dump.drop_dependencies.is_empty(), "no dependencies read");
    applied.unwrap_or_else(|e| panic!("clear script failed: {e}\n{script}"));
    assert_eq!(left, Some(0), "schema left behind:\n{script}");
    assert_eq!(
        event_triggers,
        Some(0),
        "event trigger left behind:\n{script}"
    );
    assert!(!script.contains(" cascade;"), "{script}");
    assert!(
        script.contains(
            "alter table if exists live_clear.cycle drop constraint if exists cycle_chk;"
        ),
        "{script}"
    );
    assert!(
        script.contains("drop rule if exists src_log on live_clear.src;"),
        "{script}"
    );
}

/// A fresh database `db` built from `fixture`, for a clear test: the admin
/// pool that created it (to drop it afterwards) and a pool on it.
async fn clear_scratch_database(
    db: &str,
    fixture: &str,
) -> (sqlx::PgPool, sqlx::PgPool, DumpConfig) {
    let admin = env_config("");
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");
    sqlx::query(&format!("drop database if exists {db} with (force)"))
        .execute(&admin_pool)
        .await
        .expect("drop leftover database");
    sqlx::query(&format!("create database {db}"))
        .execute(&admin_pool)
        .await
        .expect("create scratch database");
    let mut config = env_config("");
    config.database = db.to_string();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(config.get_connect_options().expect("scratch configuration"))
        .await
        .expect("connect to the scratch database");
    sqlx::raw_sql(fixture)
        .execute(&pool)
        .await
        .expect("create the fixture");
    (admin_pool, pool, config)
}

/// Close `pool` and drop its database `db` through `admin_pool`.
async fn drop_clear_scratch_database(admin_pool: sqlx::PgPool, pool: sqlx::PgPool, db: &str) {
    pool.close().await;
    let _ = sqlx::query(&format!("drop database if exists {db} with (force)"))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;
}

/// `pgc --command clear --scheme live_scope` on a database whose
/// `postgres_fdw` lives in `public`, with a server and a user mapping on it:
/// the extensions query reads the `public` extension although `public` is
/// not selected, and the script used to drop the server and the mapping (with
/// its password) database-wide. It now leaves them, and its `drop extension`
/// fails on them as it did in 1.0.x. With `public` selected, a foreign table
/// in the unselected `live_other` still uses the server, which `inspect`
/// reports, so the server and its mapping stay too (the mapping used to be
/// dropped ahead of a `drop server` that could only fail).
///
/// Needs a database-creating connection and the `postgres_fdw` contrib
/// module.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn clear_script_leaves_database_wide_objects_outside_the_selection_alone() {
    const DB: &str = "pgc_live_clear_scope";
    let (admin_pool, pool, mut config) = clear_scratch_database(
        DB,
        "create extension postgres_fdw schema public;
         create server live_scope_srv foreign data wrapper postgres_fdw
             options (host '127.0.0.1', port '1', dbname 'none');
         create user mapping for current_user server live_scope_srv
             options (user 'u', password 'secret');
         create schema live_scope;
         create table live_scope.t (id integer);
         create schema live_other;
         create foreign table live_other.ft (id integer) server live_scope_srv;",
    )
    .await;

    config.scheme = "live_scope".to_string();
    let mut scoped = Dump::new(config.clone());
    let scoped_inspected = scoped.inspect(4).await;
    let scoped_script = scoped.generate_clear_script(false, false, false);

    config.scheme = "live_scope|public".to_string();
    let mut with_public = Dump::new(config);
    let with_public_inspected = with_public.inspect(4).await;
    let with_public_script = with_public.generate_clear_script(false, false, false);

    drop_clear_scratch_database(admin_pool, pool, DB).await;

    scoped_inspected.expect("inspect live_scope");
    with_public_inspected.expect("inspect live_scope|public");
    for script in [&scoped_script, &with_public_script] {
        assert!(!script.contains("drop user mapping"), "{script}");
        assert!(!script.contains("drop server"), "{script}");
        assert!(
            script.contains("drop extension if exists postgres_fdw;"),
            "{script}"
        );
        assert!(
            script.contains("drop table if exists live_scope.t;"),
            "{script}"
        );
    }
    assert!(
        with_public
            .drop_dependencies
            .iter()
            .any(|d| d.before == pgc::dump::drop_order::OUTSIDE_KEY
                && d.after == "srv:live_scope_srv"),
        "{:?}",
        with_public.drop_dependencies
    );
}

/// `pgc --command clear` without `--use-cascade` on two schemas 1.1.0
/// previews failed on:
///
/// - a stored generated column calling a function whose SQL-standard body
///   reads the column's table (a cycle on PostgreSQL 15+), under a view that
///   reads the column. The cycle used to be broken by dropping the column,
///   which the view blocks; `DROP EXPRESSION` keeps the column. PostgreSQL 14
///   records the column's use of the function as an automatic dependency
///   (no cycle), and `drop function` drops the column with it, so the view
///   has to go first.
/// - a DDL-audit event trigger inserting into a table of the schema: it ran
///   after `drop table` of that table and failed it, and every later drop.
///
/// Needs a superuser (the event trigger).
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn clear_script_breaks_a_generated_column_cycle_and_drops_event_triggers_first() {
    const DB: &str = "pgc_live_clear_generated";
    let (admin_pool, pool, mut config) = clear_scratch_database(
        DB,
        "create schema live_gen;
         create table live_gen.t (a integer);
         create function live_gen.f(x integer) returns integer language sql immutable
             begin atomic select x + (select count(*) from live_gen.t)::integer; end;
         alter table live_gen.t add column g integer generated always as (live_gen.f(a)) stored;
         create view live_gen.w as select a, g from live_gen.t;
         create function live_gen.h() returns bigint language sql stable
             begin atomic select count(*) from live_gen.w; end;
         create table live_gen.ddl_log (id serial primary key, tag text);
         create function live_gen.log_ddl() returns event_trigger language plpgsql
             as $$ begin insert into live_gen.ddl_log (tag) values (tg_tag); end $$;
         create event trigger live_gen_log_ddl on ddl_command_end
             execute function live_gen.log_ddl();
         create type live_gen.mood as enum ('ok', 'meh');",
    )
    .await;

    config.scheme = "live_gen".to_string();
    let mut dump = Dump::new(config);
    let inspected = dump.inspect(4).await;
    let script = dump.generate_clear_script(true, false, false);
    let applied = sqlx::raw_sql(&script).execute(&pool).await;
    let left: Option<i64> =
        sqlx::query_scalar("select count(*) from pg_namespace where nspname = 'live_gen'")
            .fetch_one(&pool)
            .await
            .ok();
    // An event trigger survives a failed transaction; drop it for the
    // cleanup either way.
    let _ = sqlx::query("drop event trigger if exists live_gen_log_ddl")
        .execute(&pool)
        .await;

    drop_clear_scratch_database(admin_pool, pool, DB).await;

    inspected.expect("inspect the scratch database");
    applied.unwrap_or_else(|e| panic!("clear script failed: {e}\n{script}"));
    assert_eq!(left, Some(0), "schema left behind:\n{script}");
    assert!(!script.contains(" cascade;"), "{script}");
    assert!(!script.contains("drop column"), "{script}");
    let first_drop = script
        .lines()
        .find(|l| l.starts_with("drop ") || l.starts_with("alter "))
        .unwrap_or_default();
    assert_eq!(
        first_drop, "drop event trigger if exists live_gen_log_ddl;",
        "{script}"
    );
}

// ==== r3-compat tests ====

/// The `server_version_num` of the server the `PG*` variables point at.
async fn r3_compat_server_version() -> i32 {
    let admin = env_config("");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");
    let version = sqlx::query_scalar("select current_setting('server_version_num')::int4")
        .fetch_one(&pool)
        .await
        .expect("server version");
    pool.close().await;
    version
}

/// A `--scheme` pattern that matches no schema (the FROM of a first
/// deployment into a fresh database) still writes a pgc 1.1+ dump: it
/// records the server's version and installable extensions, so a compare
/// with it in either position is neither refused nor treated as pgc 1.0.x.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn a_dump_whose_pattern_matches_no_schema_still_records_its_server() {
    let version = r3_compat_server_version().await;
    let dir = ScratchDir::new("live-empty-pattern");
    let path = dir.path_str("empty.dump");
    let mut config = env_config(&path);
    config.scheme = "pgc_r3_compat_no_such_schema".to_string();

    let mut dump = Dump::new(config);
    dump.process(4)
        .await
        .expect("dump a pattern matching nothing");
    let empty = Dump::read_from_file(&path).await.expect("read it back");

    assert!(empty.schemas.is_empty(), "the pattern must match nothing");
    assert_eq!(empty.server_version_num, Some(version));
    assert!(
        empty
            .available_extensions
            .iter()
            .any(|e| e.name == "plpgsql"),
        "installable extensions must be recorded: {:?}",
        empty.available_extensions
    );

    let current_path = dir.path_str("current.dump");
    let mut current = Dump::new(env_config(&current_path));
    current
        .process(4)
        .await
        .expect("dump the configured schemas");
    let current = Dump::read_from_file(&current_path)
        .await
        .expect("read it back");
    Comparer::check_dump_versions(&current, &empty).expect("an empty TO is compared");
    Comparer::check_dump_versions(&empty, &current).expect("an empty FROM is compared");

    let from_is_off = empty.standard_conforming_strings_off;
    let mut comparer = Comparer::new(empty, current, true, false, true, GrantsMode::Full);
    comparer.compare().await.expect("compare");
    let out = dir.path_str("output.sql");
    comparer.save_script(&out).await.expect("save script");
    let script = std::fs::read_to_string(&out).expect("read script");
    assert!(!script.contains("pgc 1.0.x"), "{script}");
    assert_eq!(
        script.contains("set standard_conforming_strings = on;"),
        from_is_off,
        "{script}"
    );
}

/// The same empty dump also records a `standard_conforming_strings = off`
/// default (PostgreSQL 14–18), so a migration into that fresh database still
/// starts with the guard. Goes through `inspect`, the clear command's path.
///
/// Needs a database-creating connection (CREATEDB or superuser); skipped on
/// PostgreSQL 19+, which cannot turn the setting off.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn an_empty_dump_of_a_database_with_scs_off_records_the_setting() {
    const DB: &str = "pgc_live_r3_compat_scs_off";

    if r3_compat_server_version().await >= 190000 {
        eprintln!("skipped: PostgreSQL 19+ cannot set standard_conforming_strings = off");
        return;
    }
    let admin = env_config("");
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.get_connect_options().expect("admin configuration"))
        .await
        .expect("open an admin connection");
    for statement in [
        format!("drop database if exists {DB} with (force)"),
        format!("create database {DB}"),
        format!("alter database {DB} set standard_conforming_strings = off"),
    ] {
        sqlx::query(&statement)
            .execute(&admin_pool)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }

    let mut config = env_config("");
    config.database = DB.to_string();
    config.scheme = "pgc_r3_compat_no_such_schema".to_string();
    let mut dump = Dump::new(config);
    let result = dump.inspect(4).await;

    let _ = sqlx::query(&format!("drop database if exists {DB} with (force)"))
        .execute(&admin_pool)
        .await;
    admin_pool.close().await;
    result.expect("inspect the scratch database");

    assert!(dump.schemas.is_empty());
    assert!(dump.server_version_num.is_some());
    assert!(dump.standard_conforming_strings_off);
}

/// A `--scheme` pattern matching no schema still dumps the database-wide
/// objects: the event triggers, publications, subscriptions, foreign-data
/// wrappers, servers, user mappings and `public` extensions that a dump of
/// any schema of the same database holds. An empty dump used to hold none,
/// so a compare against it as TO dropped every one of them (a subscription's
/// replication slot with it), and one from it as FROM created them again.
/// The clear command still dumps nothing for such a pattern, so its script
/// drops nothing.
///
/// Needs a database-creating superuser connection (event triggers and
/// subscriptions); `pgcrypto` is used when the server can install it.
#[tokio::test]
#[ignore = "needs a reachable PostgreSQL server; run with --ignored"]
async fn an_empty_dump_reads_the_database_wide_objects_its_database_holds() {
    const DB: &str = "pgc_live_r3_compat_wide";
    let (admin_pool, pool, config) = clear_scratch_database(
        DB,
        "create schema live_app;
         create table live_app.t (id integer primary key);
         create function public.live_wide_et() returns event_trigger
             language plpgsql as $$ begin end $$;
         create event trigger live_wide_et on ddl_command_start
             execute function public.live_wide_et();
         create publication live_wide_pub for all tables;
         create foreign data wrapper live_wide_fdw;
         create server live_wide_srv foreign data wrapper live_wide_fdw;
         create user mapping for current_user server live_wide_srv;
         create subscription live_wide_sub
             connection 'host=127.0.0.1 port=1 dbname=none'
             publication live_wide_pub with (connect = false, slot_name = none);
         do $$ begin
             if exists (select from pg_available_extensions where name = 'pgcrypto') then
                 create extension pgcrypto schema public;
             end if;
         end $$;",
    )
    .await;

    let dir = ScratchDir::new("live-empty-wide");
    let dump = |scheme: &str, file: &str| {
        let mut config = config.clone();
        config.scheme = scheme.to_string();
        config.file = dir.path_str(file);
        Dump::new(config)
    };
    let mut full = dump("live_app", "full.dump");
    let full_result = full.process(4).await;
    let mut empty = dump("pgc_r3_compat_no_such_schema", "empty.dump");
    let empty_result = empty.process(4).await;
    let mut cleared = dump("pgc_r3_compat_no_such_schema", "");
    let cleared_result = cleared.inspect(4).await;

    // A database holding a subscription cannot be dropped.
    let _ = sqlx::query("drop subscription if exists live_wide_sub")
        .execute(&pool)
        .await;
    drop_clear_scratch_database(admin_pool, pool, DB).await;
    full_result.expect("dump live_app");
    empty_result.expect("dump a pattern matching nothing");
    cleared_result.expect("inspect a pattern matching nothing");

    assert!(empty.schemas.is_empty() && empty.tables.is_empty());
    let names = |dump: &Dump| {
        (
            dump.event_triggers
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>(),
            dump.publications
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>(),
            dump.subscriptions
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>(),
            dump.foreign_data_wrappers
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>(),
            dump.foreign_servers
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>(),
            dump.user_mappings.len(),
            dump.extensions
                .iter()
                .map(|o| o.name.clone())
                .collect::<Vec<_>>(),
        )
    };
    let wide = names(&full);
    assert!(wide.0.contains(&"live_wide_et".to_string()), "{wide:?}");
    assert!(wide.1.contains(&"live_wide_pub".to_string()), "{wide:?}");
    assert!(wide.2.contains(&"live_wide_sub".to_string()), "{wide:?}");
    assert!(wide.3.contains(&"live_wide_fdw".to_string()), "{wide:?}");
    assert!(wide.4.contains(&"live_wide_srv".to_string()), "{wide:?}");
    assert_eq!(wide.5, 1, "{wide:?}");
    assert_eq!(
        names(&empty),
        wide,
        "an empty dump must read the database-wide objects a dump of live_app reads"
    );

    // Compared in either position with the dump of live_app, only live_app.t
    // differs.
    let compare = |from: &Dump, to: &Dump, file: &str| {
        let (from, to) = (from.clone(), to.clone());
        let out = dir.path_str(file);
        async move {
            let mut comparer = Comparer::new(from, to, true, false, true, GrantsMode::Ignore);
            comparer.compare().await.expect("compare");
            comparer.save_script(&out).await.expect("save script");
            std::fs::read_to_string(&out).expect("read script")
        }
    };
    let drop_script = compare(&full, &empty, "drop.sql").await;
    let create_script = compare(&empty, &full, "create.sql").await;
    assert!(
        drop_script.contains("drop table if exists live_app.t;"),
        "{drop_script}"
    );
    assert!(
        create_script.contains("create table live_app.t"),
        "{create_script}"
    );
    for script in [&drop_script, &create_script] {
        let statements = common::strip_sql_comments(script).to_lowercase();
        for kind in [
            "event trigger",
            "publication",
            "subscription",
            "foreign data wrapper",
            "server",
            "user mapping",
            "extension",
        ] {
            for verb in ["create", "drop", "alter"] {
                assert!(
                    !statements.contains(&format!("{verb} {kind}")),
                    "{verb} {kind}:\n{script}"
                );
            }
        }
    }

    // The clear command's `inspect` reads no object for such a pattern.
    assert!(cleared.schemas.is_empty() && cleared.extensions.is_empty());
    assert!(cleared.publications.is_empty() && cleared.event_triggers.is_empty());
    let clear_script = cleared.generate_clear_script(false, false, false);
    assert!(
        !common::strip_sql_comments(&clear_script)
            .to_lowercase()
            .contains("drop "),
        "{clear_script}"
    );
}
