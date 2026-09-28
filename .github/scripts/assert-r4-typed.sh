#!/usr/bin/env bash
# ============================================================================
# r4:typed - "Assert typed tables, composite attribute collations and retype
# blockers" - run after "Apply diff to database `a`" (env PGPASSWORD=postgres,
# PGPORT set). Works on PG14-19.
# ============================================================================
# After round 1 is applied, `a` must match `b` for every rv_r4t_* object:
#   - the attributes of the rv_r4t_* composite types, with their collations
#     (rv_r4t_ct changes two, rv_r4t_newc is created with one);
#   - the columns (type, collation, NOT NULL, default, generation) of the
#     rv_r4t_* tables: the partition rv_r4t_tp1 and the INHERITS child
#     rv_r4t_ti (with its generated column g) of typed tables, retyped by
#     their type's ALTER ATTRIBUTE ... CASCADE; rv_r4t_z and rv_r4t_a_t under
#     one policy reading both;
#   - which rv_r4t_* tables exist, typed by what, partitioned how: the typed
#     rv_r4t_gone is dropped, rv_r4t_rp recreated (LIST -> RANGE) and the new
#     typed tables rv_r4t_new / rv_r4t_bare / rv_r4t_nn created
#     (CREATE TABLE ... OF used to list column types, a syntax error);
#   - their policies, triggers and constraints, which were in the way of the
#     retype, and their rows;
#   - attributes that need quoting (rv_r4t_qt: "order", "Amt") reach their
#     typed table and its INHERITS child; rv_r4t_lti, which leaves typed
#     table rv_r4t_ltt's family, retypes itself; the generated column that
#     dropped descendants inherit (rv_r4t_3c2, rv_r4t_5c2) is left to their
#     ancestor;
#   - rv_r4t_xtt, typed by a type outside the compared schemas, keeps its
#     view and publication member: nothing retypes its column, so round 1
#     names neither (it is left out of the column comparison, since its type
#     is not migrated by design).
# Round 2 cannot see these: an ALTER TYPE that fails leaves FROM and the next
# dump alike, and a collation the dump did not record never differed.
set -u
fail=0
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
same() {
  local name=$1 sql=$2 va vb
  va=$(q a "$sql") || { echo "ERROR: query for ${name} failed on a"; fail=1; return; }
  vb=$(q b "$sql") || { echo "ERROR: query for ${name} failed on b"; fail=1; return; }
  if [ "$va" != "$vb" ]; then
    echo "ERROR: ${name} differs between a and b"
    diff <(printf '%s\n' "$va") <(printf '%s\n' "$vb") || true
    fail=1
  else
    echo "OK: ${name} ($(printf '%s' "$va" | grep -c '') rows)"
  fi
}
expect_in_b() {
  local name=$1 sql=$2 want=$3 got
  got=$(q b "$sql") || got=error
  if [ "$got" != "$want" ]; then
    echo "ERROR: fixture check '${name}' on b: expected '${want}', got '${got}'"
    fail=1
  fi
}
ver=$(q b "select current_setting('server_version_num')::int")
if [ "$ver" -lt 140000 ]; then
  echo "SKIP: server_version_num ${ver} < 140000"
  exit 0
fi
if [ ! -f "$OUT" ]; then
  echo "ERROR: round-1 SQL not found at ${OUT}"
  exit 1
fi
R4T="c.relnamespace = 'test_schema'::regnamespace and c.relname like 'rv\\_r4t\\_%'"
# rv_r4t_xtt's column is not migrated: its type is outside the compared schemas.
R4T_MIGRATED="${R4T} and c.relname <> 'rv_r4t_xtt'"

same "rv_r4t composite type attributes (name, type, collation)" \
  "select t.typname,
          string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod)
                     || coalesce(' collate ' || co.collname, ''), ', ' order by a.attnum)
     from pg_type t
     join pg_class c on c.oid = t.typrelid and c.relkind = 'c'
     join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
     left join pg_collation co on co.oid = a.attcollation and a.attcollation <> 100
    where ${R4T}
    group by 1 order by 1"
expect_in_b "rv_r4t_ct collations" \
  "select string_agg(a.attname || ':' || coalesce(co.collname, '-'), ',' order by a.attnum)
     from pg_attribute a
     left join pg_collation co on co.oid = a.attcollation and a.attcollation <> 100
    where a.attrelid = 'test_schema.rv_r4t_ct'::regclass and a.attnum > 0" \
  "id:-,note:C,memo:-"

same "rv_r4t tables (kind, type, partitioning, bound)" \
  "select c.relname, c.relkind, coalesce(c.reloftype::regtype::text, '-'),
          coalesce(p.partstrat::text, '-'), coalesce(pg_get_expr(c.relpartbound, c.oid), '-')
     from pg_class c
     left join pg_partitioned_table p on p.partrelid = c.oid
    where ${R4T} and c.relkind in ('r', 'p')
    order by 1"
expect_in_b "rv_r4t typed tables" \
  "select string_agg(c.relname || ':' || c.reloftype::regtype::text, ',' order by c.relname)
     from pg_class c where ${R4T} and c.reloftype <> 0" \
  "rv_r4t_3tt:test_schema.rv_r4t_3t,rv_r4t_5tt:test_schema.rv_r4t_5t,rv_r4t_bare:test_schema.rv_r4t_it,rv_r4t_coll:test_schema.rv_r4t_ct,rv_r4t_ltt:test_schema.rv_r4t_lt,rv_r4t_new:test_schema.rv_r4t_it,rv_r4t_nn:test_schema.rv_r4t_it,rv_r4t_qtt:test_schema.rv_r4t_qt,rv_r4t_rp:test_schema.rv_r4t_rt,rv_r4t_tp:test_schema.rv_r4t_pt,rv_r4t_tt:test_schema.rv_r4t_it,rv_r4t_xtt:rv_r4t_outside.rv_r4t_xt"

same "rv_r4t table columns (type, collation, NOT NULL, default, generation)" \
  "select c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
          coalesce(co.collname, '-'), a.attnotnull, a.attgenerated,
          coalesce(pg_get_expr(ad.adbin, ad.adrelid), '-')
     from pg_class c
     join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
     left join pg_collation co on co.oid = a.attcollation and a.attcollation <> 100
     left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
    where ${R4T_MIGRATED} and c.relkind in ('r', 'p')
    order by 1, a.attname"
expect_in_b "rv_r4t_ti retyped with its generated column" \
  "select string_agg(a.attname || ':' || format_type(a.atttypid, a.atttypmod), ',' order by a.attname)
     from pg_attribute a
    where a.attrelid = 'test_schema.rv_r4t_ti'::regclass and a.attname in ('amount', 'g')" \
  "amount:bigint,g:bigint"
expect_in_b "rv_r4t_qti and rv_r4t_lti retyped" \
  "select string_agg(a.attrelid::regclass::text || '.' || a.attname || ':' || format_type(a.atttypid, a.atttypmod), ',' order by a.attrelid::regclass::text, a.attname)
     from pg_attribute a
    where a.attrelid in ('test_schema.rv_r4t_qti'::regclass, 'test_schema.rv_r4t_lti'::regclass)
      and a.attname in ('order', 'Amt', 'g', 'amount')" \
  "test_schema.rv_r4t_lti.amount:bigint,test_schema.rv_r4t_qti.Amt:bigint,test_schema.rv_r4t_qti.g:bigint,test_schema.rv_r4t_qti.order:bigint"

same "rv_r4t policies" \
  "select tablename, policyname, coalesce(qual, '-') from pg_policies
    where schemaname = 'test_schema' and tablename like 'rv\\_r4t\\_%'
    order by 1, 2"
expect_in_b "rv_r4t policy count" \
  "select count(*) from pg_policies
    where schemaname = 'test_schema' and tablename like 'rv\\_r4t\\_%'" \
  "10"

same "rv_r4t triggers" \
  "select c.relname, t.tgname, pg_get_triggerdef(t.oid)
     from pg_trigger t join pg_class c on c.oid = t.tgrelid
    where ${R4T} and not t.tgisinternal
    order by 1, 2"
same "rv_r4t constraints" \
  "select c.relname, k.conname, k.contype, pg_get_constraintdef(k.oid)
     from pg_constraint k join pg_class c on c.oid = k.conrelid
    where ${R4T}
    order by 1, 2"
same "rv_r4t rows" \
  "select 'tp', id::text, amount::text from test_schema.rv_r4t_tp
   union all select 'ti', id::text, amount || '/' || g from test_schema.rv_r4t_ti
   union all select 'coll', id::text, note || '/' || memo from test_schema.rv_r4t_coll
   union all select 'a_t', id::text, n || '/' || g from test_schema.rv_r4t_a_t
   union all select 'z', id::text, n::text from test_schema.rv_r4t_z
   union all select 'qti', id::text, \"order\" || '/' || \"Amt\" || '/' || g from test_schema.rv_r4t_qti
   union all select 'lti', id::text, amount || '/' || extra from test_schema.rv_r4t_lti
   order by 1, 2"

# The collation change is migrated by the type, not left behind.
if grep -Eiq '^alter type test_schema\.rv_r4t_ct alter attribute "note" set data type text collate pg_catalog\."C" cascade;' "$OUT"; then
  echo "OK: rv_r4t_ct.note gets its collation through ALTER TYPE"
else
  echo "ERROR: round 1 does not retype rv_r4t_ct.note to collate \"C\""
  fail=1
fi
# What is in the way of rv_r4t_it's retype goes before it.
retype=$(grep -n -m1 -Ei '^alter type test_schema\.rv_r4t_it alter attribute "amount"' "$OUT" | cut -d: -f1)
gdrop=$(grep -n -m1 -Ei '^alter table test_schema\.rv_r4t_ti drop column (if exists )?g;' "$OUT" | cut -d: -f1)
pdrop=$(grep -n -m1 -Ei '^drop policy if exists rv_r4t_ti_pol on test_schema\.rv_r4t_ti;' "$OUT" | cut -d: -f1)
if [ -n "$retype" ] && [ -n "$gdrop" ] && [ -n "$pdrop" ] && [ "$gdrop" -lt "$retype" ] && [ "$pdrop" -lt "$retype" ]; then
  echo "OK: rv_r4t_ti's policy and generated column are out of the way of the retype"
else
  echo "ERROR: rv_r4t_it retype at line '${retype}', rv_r4t_ti.g drop at '${gdrop}', rv_r4t_ti_pol drop at '${pdrop}'"
  fail=1
fi

# Attributes that need quoting: their blockers go before their retypes too.
qorder=$(grep -n -m1 -Ei '^alter type test_schema\.rv_r4t_qt alter attribute "order"' "$OUT" | cut -d: -f1)
qamt=$(grep -n -m1 -Ei '^alter type test_schema\.rv_r4t_qt alter attribute "Amt"' "$OUT" | cut -d: -f1)
qpol=$(grep -n -m1 -Ei '^drop policy if exists rv_r4t_qtt_pol on test_schema\.rv_r4t_qtt;' "$OUT" | cut -d: -f1)
qgen=$(grep -n -m1 -Ei '^alter table test_schema\.rv_r4t_qti drop column (if exists )?g;' "$OUT" | cut -d: -f1)
if [ -n "$qorder" ] && [ -n "$qamt" ] && [ -n "$qpol" ] && [ -n "$qgen" ] && [ "$qpol" -lt "$qorder" ] && [ "$qgen" -lt "$qamt" ]; then
  echo "OK: rv_r4t_qt's quoted attributes have their blockers out of the way"
else
  echo "ERROR: rv_r4t_qt retypes at '${qorder}'/'${qamt}', rv_r4t_qtt_pol drop at '${qpol}', rv_r4t_qti.g drop at '${qgen}'"
  fail=1
fi
# A dropped descendant's inherited generated column is its ancestor's to drop:
# rv_r4t_3c1's DROP COLUMN takes rv_r4t_3c2's copy (whose own drop, if any,
# says IF EXISTS), rv_r4t_5c1's DROP EXPRESSION reaches rv_r4t_5c2's.
if grep -Eiq '^alter table test_schema\.rv_r4t_3c2 drop column g;|^alter table test_schema\.rv_r4t_5c2 drop column (if exists )?g;' "$OUT"; then
  echo "ERROR: round 1 drops an inherited generated column of rv_r4t_3c2 / rv_r4t_5c2 on its own"
  fail=1
else
  echo "OK: rv_r4t_3c2 / rv_r4t_5c2 leave their inherited generated column to their ancestor"
fi
# Nothing retypes rv_r4t_xtt's column, so what reads it stays.
xchurn='(drop view|create or replace view)[^;]*rv_r4t_xv|alter publication rv_r4t_xpub'
if grep -Eiq "$xchurn" "$OUT"; then
  echo "ERROR: round 1 takes rv_r4t_xv or rv_r4t_xpub out of the way of a retype that never comes"
  grep -Ei "$xchurn" "$OUT"
  fail=1
else
  echo "OK: rv_r4t_xtt's view and publication member are left alone"
fi
same "rv_r4t_xv and rv_r4t_xpub" \
  "select 'view', pg_get_viewdef('test_schema.rv_r4t_xv'::regclass)
   union all select 'pub', count(*)::text from pg_publication_rel r join pg_publication p on p.oid = r.prpubid
    where p.pubname = 'rv_r4t_xpub'"

exit $fail
