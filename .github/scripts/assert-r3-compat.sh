#!/usr/bin/env bash
# ============================================================================
# r3-compat - "Assert empty dumps are pgc 1.1 dumps, and retain_dead_tuples
# changes on enabled subscriptions apply"
# - standalone, any time after "Build pgc (release)" (env PGPASSWORD=postgres).
# Uses its own databases pgc_r3c_*; part 1 runs on PG14-19, part 2 on PG19+.
# ============================================================================
#   1. A dump whose --scheme pattern matches no schema (the FROM of a first
#      deployment into a fresh database) records server_version_num like any
#      other dump, and the database-wide objects (event triggers,
#      publications, subscriptions, foreign-data wrappers, servers, user
#      mappings, extensions in public) like a dump of any schema. As TO it is
#      compared, not refused as "written by pgc 1.0.x", and the migration
#      drops only the schema's objects, none of the database-wide ones both
#      databases hold; as FROM it gets no 1.0.x warning and no
#      standard_conforming_strings guard, creates none of those objects
#      again, and on PostgreSQL 19 it keeps the 19-only SET search_path TO
#      NULL, so one round converges.
#   2. PostgreSQL 19: an enabled subscription whose retain_dead_tuples
#      changes together with its publication list or its connection (each of
#      which restarts the apply worker when made while it is enabled) is
#      migrated in one round, with ON_ERROR_STOP=1, three times per case: the
#      subscription is disabled first and the migration waits for its workers
#      to exit before the SET. Afterwards pg_subscription and
#      pg_subscription_rel of 'a' match 'b', and a second compare is empty.
set -u
port=${PGPORT:-5432}
pgc=${PGC:-./app/target/release/pgc}
work=${R3C_WORK:-/tmp/pgc_r3c}
mkdir -p "$work"
rm -f "$work"/*.sql "$work"/*.dump "$work"/*.log "$work"/*.err
export PGOPTIONS="-c client_min_messages=warning"
P="psql -h 127.0.0.1 -p $port -U postgres -X -v ON_ERROR_STOP=1 -q"
ver=$($P -d postgres -tAc "select current_setting('server_version_num')::int")
fail=0
# The round-1 SQL of the fixture round trip carries no bounce: every fixture
# subscription is disabled.
if [ -f "${OUTPUT_SQL:-/tmp/output.sql}" ] && grep -q 'pg_stat_subscription' "${OUTPUT_SQL:-/tmp/output.sql}"; then
  echo "ERROR: the fixture migration waits for a subscription worker"; fail=1
fi

dump() { # database scheme file
  "$pgc" --command dump --server 127.0.0.1 --port "$port" --user postgres \
    --password "${PGPASSWORD:-postgres}" --database "$1" --scheme "$2" --output "$3" \
    > "$3.log" 2>&1 || { echo "ERROR: dump of $1 ($2) failed"; cat "$3.log"; exit 1; }
}
compare() { # from to output
  "$pgc" --command compare --from "$1" --to "$2" --output "$3" --use-drop true \
    --use-comments true > "$3.log" 2> "$3.err"
}
dump_key() { # file key
  python3 - "$1" "$2" <<'PY'
import json, sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    d = json.loads(z.read("dump.io"))
v = d.get(sys.argv[2])
print("" if v is None else (len(v) if isinstance(v, list) else v))
PY
}

# ---- 1. empty dumps --------------------------------------------------------
# A database holding a subscription cannot be dropped.
wide_gone() {
  for db in pgc_r3c_full pgc_r3c_fresh; do
    if [ -n "$($P -d postgres -tAc "select 1 from pg_database where datname = '$db'")" ]; then
      $P -d "$db" -c "drop subscription if exists pgc_r3c_wide_sub" > /dev/null 2>&1 || true
      $P -d postgres -c "drop database if exists ${db} with (force)"
    fi
  done
}
wide_gone
for db in pgc_r3c_full pgc_r3c_fresh; do
  $P -d postgres -c "create database ${db}"
  # The same database-wide objects in both.
  $P -d "$db" -c "create function public.pgc_r3c_et() returns event_trigger language plpgsql as \$\$ begin end \$\$" \
    -c "create event trigger pgc_r3c_et on ddl_command_start execute function public.pgc_r3c_et()" \
    -c "create publication pgc_r3c_wide_pub for all tables" \
    -c "create foreign data wrapper pgc_r3c_fdw" \
    -c "create server pgc_r3c_srv foreign data wrapper pgc_r3c_fdw" \
    -c "create user mapping for postgres server pgc_r3c_srv" \
    -c "create subscription pgc_r3c_wide_sub connection 'host=127.0.0.1 port=1 dbname=none'
          publication pgc_r3c_wide_pub with (connect = false, slot_name = none)" \
    > "$work/wide_$db.log" 2>&1 || { echo "ERROR: creating the database-wide objects in $db failed"; cat "$work/wide_$db.log"; fail=1; }
  if [ -n "$($P -d "$db" -tAc "select 1 from pg_available_extensions where name = 'pgcrypto'")" ]; then
    $P -d "$db" -c "create extension pgcrypto schema public"
  fi
done
$P -d pgc_r3c_full -c "create schema r3c_app" -c "create table r3c_app.t (id integer primary key)"
if [ "$ver" -ge 190000 ]; then
  $P -d pgc_r3c_full -c "create function r3c_app.f() returns integer language sql set search_path to null as 'select 1'"
fi
dump pgc_r3c_full r3c_app "$work/full.dump"
dump pgc_r3c_fresh r3c_app "$work/fresh.dump"
recorded=$(dump_key "$work/fresh.dump" server_version_num)
if [ "$recorded" != "$ver" ]; then
  echo "ERROR: empty dump records server_version_num '$recorded', expected $ver"; fail=1
fi
if [ "$(dump_key "$work/fresh.dump" available_extensions)" = "" ]; then
  echo "ERROR: empty dump records no installable extensions"; fail=1
fi
for key in event_triggers publications subscriptions foreign_data_wrappers foreign_servers user_mappings extensions; do
  if [ "$(dump_key "$work/fresh.dump" "$key")" != "$(dump_key "$work/full.dump" "$key")" ]; then
    echo "ERROR: empty dump holds $(dump_key "$work/fresh.dump" "$key") $key, a dump of r3c_app $(dump_key "$work/full.dump" "$key")"; fail=1
  fi
done
# Statements on the database-wide objects both databases hold.
wide_ddl() {
  grep -v '^--' "$1" | grep -iE "(create|drop|alter) (event trigger|publication|subscription|foreign data wrapper|server|user mapping|extension)"
}

# Empty TO: compared, and the migration drops what FROM has.
if ! compare "$work/full.dump" "$work/fresh.dump" "$work/drop.sql"; then
  echo "ERROR: compare against an empty TO dump failed:"; cat "$work/drop.sql.err"; fail=1
elif ! grep -q "drop table if exists r3c_app.t" "$work/drop.sql"; then
  echo "ERROR: compare against an empty TO dump does not drop r3c_app.t"; fail=1
elif wide_ddl "$work/drop.sql"; then
  echo "ERROR: compare against an empty TO dump changes the database-wide objects above"; fail=1
fi
# Empty FROM: no pgc 1.0.x handling.
if ! compare "$work/fresh.dump" "$work/full.dump" "$work/create.sql"; then
  echo "ERROR: compare from an empty FROM dump failed:"; cat "$work/create.sql.err"; fail=1
fi
if grep -qs "pgc 1.0.x" "$work/create.sql" "$work/create.sql.err" "$work/drop.sql" "$work/drop.sql.err"; then
  echo "ERROR: an empty dump is treated as written by pgc 1.0.x"; fail=1
fi
if wide_ddl "$work/create.sql"; then
  echo "ERROR: compare from an empty FROM dump changes the database-wide objects above"; fail=1
fi
if grep -q "standard_conforming_strings" "$work/create.sql"; then
  echo "ERROR: migration from an empty FROM dump carries the standard_conforming_strings guard"; fail=1
fi
if [ "$ver" -ge 190000 ] && ! grep -q "SET search_path TO NULL" "$work/create.sql"; then
  echo "ERROR: migration from an empty PG19 FROM dump falls back from SET search_path TO NULL"; fail=1
fi
$P -d pgc_r3c_fresh -f "$work/create.sql" > "$work/create.apply.log" 2>&1 \
  || { echo "ERROR: applying the migration into the fresh database failed"; cat "$work/create.apply.log"; fail=1; }
if [ "$ver" -ge 190000 ]; then
  cfg_a=$($P -d pgc_r3c_fresh -tAc "select proconfig::text from pg_proc where oid = 'r3c_app.f()'::regprocedure")
  cfg_b=$($P -d pgc_r3c_full -tAc "select proconfig::text from pg_proc where oid = 'r3c_app.f()'::regprocedure")
  if [ "$cfg_a" != "$cfg_b" ]; then
    echo "ERROR: r3c_app.f() proconfig '$cfg_a' after the migration, expected '$cfg_b'"; fail=1
  fi
fi
dump pgc_r3c_fresh r3c_app "$work/fresh2.dump"
"$pgc" --command compare --from "$work/fresh2.dump" --to "$work/full.dump" \
  --output "$work/create2.sql" --use-drop true --use-comments false > /dev/null 2>&1
if [ -n "$(tr -d '[:space:]' < "$work/create2.sql")" ]; then
  echo "ERROR: second compare after the first deployment is not empty:"; cat "$work/create2.sql"; fail=1
fi
wide_gone

# ---- 2. retain_dead_tuples on an enabled subscription (PG19+) --------------
if [ "$ver" -ge 190000 ]; then
  # The subscriptions are dropped without their slots (disabled, slot_name
  # = NONE): the migration moves 'a' onto the slot 'b' streams from, and a
  # plain DROP SUBSCRIPTION waits (DROP_REPLICATION_SLOT ... WAIT) for a
  # slot another subscription holds, forever. The slots are dropped below
  # once their walsenders are gone. A database holding a slot cannot be
  # dropped.
  subs_gone() {
    for db in pgc_r3c_a pgc_r3c_b; do
      if [ -n "$($P -d postgres -tAc "select 1 from pg_database where datname = '$db'")" ]; then
        psql -h 127.0.0.1 -p "$port" -U postgres -X -q -d "$db" \
          -c "alter subscription pgc_r3c_sub disable" \
          -c "alter subscription pgc_r3c_sub set (slot_name = none)" \
          -c "drop subscription pgc_r3c_sub" > /dev/null 2>&1 || true
      fi
    done
    for _ in $(seq 1 50); do
      [ -z "$($P -d postgres -tAc "select 1 from pg_replication_slots where slot_name like 'pgc\_r3c\_slot\__'")" ] && break
      $P -d postgres -c "select pg_drop_replication_slot(slot_name) from pg_replication_slots
        where slot_name like 'pgc\_r3c\_slot\__' and not active" > /dev/null 2>&1 || true
      sleep 0.2
    done
  }
  trap subs_gone EXIT
  # Inside the server the publisher is on its own port 5432.
  conn="host=127.0.0.1 port=5432 dbname=pgc_r3c_pub user=postgres password=${PGPASSWORD:-postgres}"
  opts_a="create_slot = false, slot_name = 'pgc_r3c_slot_a', copy_data = false"
  opts_b="create_slot = false, slot_name = 'pgc_r3c_slot_b', copy_data = false"
  state() {
    $P -d "$1" -tAc "select s.subenabled, s.subretaindeadtuples, s.submaxretention,
        s.subpublications::text, s.subconninfo, s.subslotname,
        (select string_agg(r.srrelid::regclass::text, ',' order by 1)
           from pg_subscription_rel r where r.srsubid = s.oid)
      from pg_subscription s
      where s.subdbid = (select oid from pg_database where datname = current_database())
        and s.subname = 'pgc_r3c_sub'"
  }
  run_case() { # name from-clause to-clause
    for i in 1 2 3; do
      subs_gone
      for db in pgc_r3c_pub pgc_r3c_a pgc_r3c_b; do
        $P -d postgres -c "drop database if exists ${db} with (force)" -c "create database ${db}"
      done
      $P -d pgc_r3c_pub -c "create table public.x (id integer primary key)" \
        -c "create table public.y (id integer primary key)" \
        -c "create publication pgc_r3c_pp for table public.x" \
        -c "create publication pgc_r3c_pq for table public.y" \
        -c "select pg_create_logical_replication_slot('pgc_r3c_slot_a', 'pgoutput')" \
        -c "select pg_create_logical_replication_slot('pgc_r3c_slot_b', 'pgoutput')" > /dev/null
      for db in pgc_r3c_a pgc_r3c_b; do
        $P -d "$db" -c "create table public.x (id integer primary key)" \
          -c "create table public.y (id integer primary key)"
      done
      $P -d pgc_r3c_a -c "create subscription pgc_r3c_sub $2" > /dev/null 2>&1 \
        || { echo "ERROR: $1: creating the FROM subscription failed"; fail=1; }
      $P -d pgc_r3c_b -c "create subscription pgc_r3c_sub $3" > /dev/null 2>&1 \
        || { echo "ERROR: $1: creating the TO subscription failed"; fail=1; }
      sleep 1 # let the FROM apply worker start
      dump pgc_r3c_a public "$work/sub_a.dump"
      dump pgc_r3c_b public "$work/sub_b.dump"
      "$pgc" --command compare --from "$work/sub_a.dump" --to "$work/sub_b.dump" \
        --output "$work/sub_$1.sql" --use-drop true --use-comments false > /dev/null 2>&1
      if ! grep -q "pg_stat_subscription" "$work/sub_$1.sql"; then
        echo "ERROR: $1: the bounce does not wait for the worker"; cat "$work/sub_$1.sql"; fail=1
      fi
      if ! $P -d pgc_r3c_a -f "$work/sub_$1.sql" > "$work/sub_$1.apply.log" 2>&1; then
        echo "ERROR: $1 (run $i): applying the migration failed:"; cat "$work/sub_$1.apply.log"; fail=1
      fi
      sa=$(state pgc_r3c_a); sb=$(state pgc_r3c_b)
      if [ "$sa" != "$sb" ]; then
        echo "ERROR: $1 (run $i): subscription 'a' is '$sa', 'b' is '$sb'"; fail=1
      fi
      dump pgc_r3c_a public "$work/sub_a2.dump"
      "$pgc" --command compare --from "$work/sub_a2.dump" --to "$work/sub_b.dump" \
        --output "$work/sub_$1_2.sql" --use-drop true --use-comments false > /dev/null 2>&1
      if [ -n "$(tr -d '[:space:]' < "$work/sub_$1_2.sql")" ]; then
        echo "ERROR: $1 (run $i): second compare is not empty:"; cat "$work/sub_$1_2.sql"; fail=1
      fi
    done
  }
  # Turning it on while the publication list changes (the SET PUBLICATION
  # used to restart the worker right before the SET).
  run_case setpub "connection '$conn' publication pgc_r3c_pp with ($opts_a)" \
    "connection '$conn' publication pgc_r3c_pp, pgc_r3c_pq with ($opts_b, retain_dead_tuples = true, max_retention_duration = 1000)"
  # Turning it on while the connection changes.
  run_case conn "connection '$conn' publication pgc_r3c_pp with ($opts_a)" \
    "connection '$conn application_name=pgc_r3c' publication pgc_r3c_pp with ($opts_b, retain_dead_tuples = true)"
  # Turning it off, publications change, and the target is disabled: the
  # subscription is enabled once more for the refresh.
  run_case off_disabled "connection '$conn' publication pgc_r3c_pp with ($opts_a, retain_dead_tuples = true)" \
    "connection '$conn' publication pgc_r3c_pp, pgc_r3c_pq with ($opts_b, enabled = false)"
  subs_gone
  for db in pgc_r3c_pub pgc_r3c_a pgc_r3c_b; do
    $P -d postgres -c "drop database if exists ${db} with (force)"
  done
fi

[ $fail -eq 0 ] && echo "r3-compat: OK (server_version_num $ver)"
exit $fail