#!/usr/bin/env bash
# pre-clear: `pgc --command clear` drops everything the round-trip fixtures
# create — in dependency order, breaking the cycles a droppable constraint,
# default, stored generated expression, trigger, rule, policy or index
# closes, event triggers first — with no CASCADE and no error, inside one
# transaction. Each fixture is loaded into a scratch database of its own
# (round 1's a and b are left alone). A clear of test_schema alone must not
# drop a foreign-data wrapper, server, user mapping or subscription: they
# all hang off the postgres_fdw in the unselected public schema.
set -uo pipefail
PGC="${PGC:-./app/target/release/pgc}"
export PGHOST=127.0.0.1 PGUSER=postgres
SCHEME='test_schema|shared_schema|new_reporting_schema|data|test_order|public'
work=$(mktemp -d)
fail=0

drop_scratch_db() {
  local db=$1
  if [ "$(psql -X -d postgres -tAc "select count(*) from pg_database where datname = '$db'")" = "1" ]; then
    # A subscription the clear left alone (it depends on nothing it drops)
    # blocks DROP DATABASE; detach it from its (never created) slot first.
    for s in $(psql -X -d "$db" -tAc "select quote_ident(subname) from pg_subscription where subdbid = (select oid from pg_database where datname = current_database())"); do
      psql -X -q -d "$db" -c "alter subscription $s disable" \
        -c "alter subscription $s set (slot_name = none)" -c "drop subscription $s" >/dev/null 2>&1
    done
    psql -X -q -d postgres -c "drop database if exists $db with (force)" >/dev/null
  fi
}

for side in a b; do
  db="pgc_preclear_$side"
  drop_scratch_db "$db"
  psql -X -q -v ON_ERROR_STOP=1 -d postgres -c "create database $db" || { fail=1; continue; }
  # Version-specific fixture statements fail on older majors, as in round 1.
  psql -X -q -v ON_ERROR_STOP=0 -d "$db" -f "data/test/schema_$side.sql" > "$work/load_$side.log" 2>&1
  if ! "$PGC" --command clear --server 127.0.0.1 --port "${PGPORT:-5432}" --user postgres \
      --password "${PGPASSWORD:-}" --database "$db" --scheme test_schema \
      --output "$work/scoped_$side.sql" > "$work/pgc_scoped_$side.log" 2>&1; then
    echo "FAIL: pgc --command clear --scheme test_schema on fixture $side"; tail -5 "$work/pgc_scoped_$side.log"; fail=1
  elif grep -Eq '^drop (server|user mapping|subscription|foreign data wrapper) ' "$work/scoped_$side.sql"; then
    echo "FAIL: a clear of test_schema alone drops database-wide objects outside it (fixture $side):"
    grep -E '^drop (server|user mapping|subscription|foreign data wrapper) ' "$work/scoped_$side.sql" | head -5; fail=1
  fi
  if ! "$PGC" --command clear --server 127.0.0.1 --port "${PGPORT:-5432}" --user postgres \
      --password "${PGPASSWORD:-}" --database "$db" --scheme "$SCHEME" \
      --output "$work/clear_$side.sql" --use-single-transaction true > "$work/pgc_$side.log" 2>&1; then
    echo "FAIL: pgc --command clear on fixture $side"; tail -5 "$work/pgc_$side.log"; fail=1
    drop_scratch_db "$db"; continue
  fi
  if grep -q "cannot order" "$work/pgc_$side.log"; then
    echo "FAIL: the clear script of fixture $side has a dependency cycle it cannot break:"
    grep "cannot order" "$work/pgc_$side.log"; fail=1
  fi
  if grep -v '^/\*\|^ \|^\*/' "$work/clear_$side.sql" | grep -qi ' cascade;'; then
    echo "FAIL: the clear script of fixture $side uses CASCADE without --use-cascade"; fail=1
  fi
  if ! psql -X -q -v ON_ERROR_STOP=1 -d "$db" -f "$work/clear_$side.sql" > "$work/apply_$side.log" 2>&1; then
    echo "FAIL: the clear script of fixture $side does not apply:"
    grep -m5 -A2 ERROR "$work/apply_$side.log"; fail=1
  fi
  left=$(psql -X -d "$db" -tAc "select string_agg(nspname, ', ' order by nspname) from pg_namespace where nspname similar to '$SCHEME'")
  if [ -n "$left" ]; then
    echo "FAIL: the clear script of fixture $side leaves schema(s) $left"; fail=1
  fi
  echo "fixture $side: clear script of $(grep -c '^drop \|^alter ' "$work/clear_$side.sql") statements checked"
  drop_scratch_db "$db"
done
rm -rf "$work"
exit $fail