#!/usr/bin/env bash
# ============================================================================
# r3-ddl - catalog names in emitted DDL. Run after "Apply diff to database
# `a`" (env PGPASSWORD=postgres, PGPORT) and before round 2 (which overwrites
# the round-1 script). Works on PG14-19.
# ============================================================================
# The rv_r3d_* fixtures (pg19:misc block) hold collations, event triggers and
# primary-key columns whose names need quoting (one collation name carries
# SQL after a `"`), foreign keys next to a dropped table whose name is a
# substring of theirs, a collation only TO has that a column, a table and a
# domain use, and policies and triggers in the way of an in-place retype. A column collation change was never part of the table
# hash, so round 2 stays empty even when it is lost on both sides; check the
# catalogs and the round-1 script directly.
set -u
fail=0
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -v ON_ERROR_STOP=1 -XtAc "$2"; }
same() {
  local name=$1 sql=$2 va vb
  va=$(q a "$sql") || { echo "ERROR: query for ${name} failed on a"; fail=1; return; }
  vb=$(q b "$sql") || { echo "ERROR: query for ${name} failed on b"; fail=1; return; }
  if [ -z "$vb" ]; then
    echo "ERROR: ${name}: nothing found in b (fixture not applied?)"
    fail=1
  elif [ "$va" != "$vb" ]; then
    echo "ERROR: ${name} differs between a and b"
    diff <(printf '%s\n' "$va") <(printf '%s\n' "$vb") || true
    fail=1
  else
    echo "OK: ${name} ($(printf '%s' "$va" | grep -c '') rows)"
  fi
}

# 1. The SQL inside the hostile collation name never ran: pgc wrote the
#    collation as `collate "<name>"` without doubling its `"`.
pwned=$(q a "select coalesce(to_regclass('public.rv_r3d_pwned')::text, '')") \
  || { echo "ERROR: to_regclass query failed on a"; fail=1; pwned=''; }
if [ -n "$pwned" ]; then
  echo "ERROR: public.rv_r3d_pwned exists in a: a collation name ran as SQL"
  fail=1
else
  echo "OK: no SQL ran out of a collation name"
fi

# 2. Every column of the rv_r3d_* tables has b's collation, schema included
#    (an unqualified name bound public.rv_r3d_ci instead of
#    test_schema.rv_r3d_ci), and every such table exists.
same "rv_r3d column collations" \
  "select c.relname, a.attnum, a.attname,
          coalesce(quote_ident(cn.nspname) || '.' || quote_ident(co.collname), '-')
     from pg_class c
     join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
     left join pg_collation co on co.oid = a.attcollation
     left join pg_namespace cn on cn.oid = co.collnamespace
    where c.relnamespace = 'test_schema'::regnamespace and c.relkind in ('r', 'p')
      and c.relname like 'rv\\_r3d\\_%'
    order by 1, 2"

# 3. Event triggers: named exactly as in b, in b's state, with b's comment
#    (the name was quoted twice: a "rv_r3d_Keep Trg" came out as
#    """rv_r3d_Keep Trg""").
same "rv_r3d event triggers" \
  "select e.evtname, e.evtevent, e.evtenabled, coalesce(array_to_string(e.evttags, ','), ''),
          coalesce(obj_description(e.oid, 'pg_event_trigger'), '')
     from pg_event_trigger e
    where e.evtname like '%rv\\_r3d\\_%'
    order by 1"

# 4. Primary keys (key columns holding `)`, `,` and `"` were cut apart, and
#    INCLUDE / DEFERRABLE were left out of CREATE TABLE), and every foreign
#    key of the rv_r3d_* tables (the ones PostgreSQL copies onto partitions
#    left out).
same "rv_r3d primary keys and foreign keys" \
  "select c.relname, k.conname, k.contype, pg_get_constraintdef(k.oid)
     from pg_constraint k join pg_class c on c.oid = k.conrelid
    where c.relnamespace = 'test_schema'::regnamespace and c.relname like 'rv\\_r3d\\_%'
      and k.contype in ('p', 'f') and k.conparentid = 0
    order by 1, 2"

# 5. Column types (a retype in place: varchar(10) -> varchar(20)), the
#    collation of the rv_r3d_* domains (one uses a collation only TO has,
#    which used to be created after the types and tables), and the policies
#    and triggers of the rv_r3d_* tables (the ones in the way of a retype are
#    dropped ahead of it and put back from TO).
same "rv_r3d column types" \
  "select c.relname, a.attnum, format_type(a.atttypid, a.atttypmod)
     from pg_class c
     join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
    where c.relnamespace = 'test_schema'::regnamespace and c.relkind in ('r', 'p')
      and c.relname like 'rv\\_r3d\\_%'
    order by 1, 2"
same "rv_r3d domain collations" \
  "select t.typname, coalesce(quote_ident(cn.nspname) || '.' || quote_ident(co.collname), '-')
     from pg_type t
     left join pg_collation co on co.oid = t.typcollation
     left join pg_namespace cn on cn.oid = co.collnamespace
    where t.typnamespace = 'test_schema'::regnamespace and t.typtype = 'd'
      and t.typname like 'rv\\_r3d\\_%'
    order by 1"
same "rv_r3d policies" \
  "select c.relname, p.polname, p.polcmd, p.polpermissive,
          coalesce(pg_get_expr(p.polqual, p.polrelid), ''),
          coalesce(pg_get_expr(p.polwithcheck, p.polrelid), '')
     from pg_policy p join pg_class c on c.oid = p.polrelid
    where c.relnamespace = 'test_schema'::regnamespace and c.relname like 'rv\\_r3d\\_%'
    order by 1, 2"
same "rv_r3d triggers" \
  "select c.relname, t.tgname, t.tgenabled, pg_get_triggerdef(t.oid)
     from pg_trigger t join pg_class c on c.oid = t.tgrelid
    where c.relnamespace = 'test_schema'::regnamespace and c.relname like 'rv\\_r3d\\_%'
      and not t.tgisinternal
    order by 1, 2"

# 6. The round-1 script: an unrelated foreign key is not dropped ahead of a
#    table whose name is a substring of its definition, and a changed one is
#    dropped once.
if [ ! -f "$OUT" ]; then
  echo "ERROR: round-1 script $OUT not found"
  fail=1
else
  # Production output spells the drop `drop constraint if exists`.
  if grep -qiE "drop constraint (if exists )?rv_r3d_payments_account_fk" "$OUT"; then
    echo "ERROR: rv_r3d_payments_account_fk is dropped, though it does not reference a dropped table"
    fail=1
  else
    echo "OK: rv_r3d_payments_account_fk is left alone"
  fi
  n=$(grep -ciE "drop constraint (if exists )?rv_r3d_products_catalog_fk" "$OUT")
  if [ "$n" != 1 ]; then
    echo "ERROR: rv_r3d_products_catalog_fk is dropped ${n} times, expected once"
    fail=1
  else
    echo "OK: rv_r3d_products_catalog_fk is dropped once"
  fi
  if grep -q 'event trigger """' "$OUT"; then
    echo "ERROR: an event trigger name is quoted twice"
    fail=1
  fi
  # The collation only TO has is created before the column that moves to it
  # is retyped, and the WHEN trigger in the way of a retype is dropped before
  # it. First occurrence of each, by line number.
  line_of() { grep -niE "$1" "$OUT" | head -1 | cut -d: -f1; }
  created=$(line_of "create collation (if not exists )?test_schema\.rv_r3d_new_ci")
  moved=$(line_of "rv_r3d_to_new_coll alter column c type")
  if [ -z "$created" ] || [ -z "$moved" ] || [ "$created" -ge "$moved" ]; then
    echo "ERROR: test_schema.rv_r3d_new_ci is not created before rv_r3d_to_new_coll.c moves to it (lines '${created}' / '${moved}')"
    fail=1
  else
    echo "OK: the new collation is created before the column moves to it"
  fi
  dropped=$(line_of "drop trigger (if exists )?rv_r3d_retype_when")
  retyped=$(line_of "rv_r3d_retype alter column status type")
  if [ -z "$dropped" ] || [ -z "$retyped" ] || [ "$dropped" -ge "$retyped" ]; then
    echo "ERROR: rv_r3d_retype_when is not dropped before rv_r3d_retype.status is retyped (lines '${dropped}' / '${retyped}')"
    fail=1
  else
    echo "OK: the trigger in the way of a retype is dropped before it"
  fi
fi

exit $fail
