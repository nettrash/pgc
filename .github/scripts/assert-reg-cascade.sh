#!/usr/bin/env bash
# CI step (reg-cascade), run after "Apply diff to database a" (round 1).
# What a routine's DROP ... CASCADE takes along beyond the table: pgc drops a
# SQL-standard routine reading a retyped column ahead of the retype, and a
# routine whose return type changes in the routine phase, both with CASCADE.
# Before the fix the migration applied cleanly but lost, on both sides of the
# second compare alike for some of them, the domain CHECKs, the WHEN-clause
# triggers and the expression statistics calling those routines, the
# functions whose argument defaults call them, and - through a domain DEFAULT
# calling one - the domain itself with every column of it and its data.
# Also indexes on materialized views and defaults of foreign-table columns
# calling them, partition clones of triggers calling them (their own enabled
# state and comment), and what the table changes create calling a routine
# dropped ahead of them (created once it is back).
# Compare a and b directly, and check the objects work in a.
# Env: PGPASSWORD (PGPORT optional, default 5432); OUTPUT_SQL = round-1 SQL.
set -euo pipefail
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
ver=$($PSQL -d a -c "show server_version_num")
fail=0
if [ "$ver" -lt 140000 ]; then
  echo "reg-cascade: SQL-standard bodies and expression statistics need PostgreSQL 14+ (server_version_num $ver), skipped"
  exit 0
fi

# 1. The fixture's domains, triggers, statistics, columns, column defaults,
#    constraints, indexes and routines are the same in a and b after the
#    migration.
q_objects="select coalesce(string_agg(x, E'\n' order by x), '') from (
    select 'domain check ' || t.typname || '.' || c.conname || ': ' || pg_get_constraintdef(c.oid) as x
      from pg_constraint c join pg_type t on t.oid = c.contypid
      where t.typnamespace = 'test_schema'::regnamespace and t.typname like 'rv\_cascade\_%'
    union all
    select 'domain default ' || typname || ': ' || coalesce(typdefault, '(none)')
      from pg_type where typnamespace = 'test_schema'::regnamespace and typtype = 'd' and typname like 'rv\_cascade\_%'
    union all
    select 'trigger ' || tgrelid::regclass::text || ': ' || pg_get_triggerdef(oid) || ' [' || tgenabled::text || '] '
           || coalesce(obj_description(oid, 'pg_trigger'), '(no comment)')
      from pg_trigger where not tgisinternal and tgname like 'rv\_cascade\_%'
    union all
    select 'statistics ' || stxname || ': ' || pg_get_statisticsobjdef(oid)
      from pg_statistic_ext where stxnamespace = 'test_schema'::regnamespace and stxname like 'rv\_cascade\_%'
    union all
    select 'column ' || attrelid::regclass::text || '.' || attname || ': ' || format_type(atttypid, atttypmod)
      from pg_attribute
      where attrelid in (select oid from pg_class where relnamespace = 'test_schema'::regnamespace and relkind in ('r', 'p', 'f', 'm', 'v') and relname like 'rv\_cascade\_%')
        and attnum > 0 and not attisdropped
    union all
    select 'view ' || oid::regclass::text || ' [' || relkind::text || ']: ' || pg_get_viewdef(oid)
      from pg_class where relnamespace = 'test_schema'::regnamespace and relkind in ('v', 'm') and relname like 'rv\_cascade\_%'
    union all
    select 'policy ' || polrelid::regclass::text || '.' || polname || ': ' || coalesce(pg_get_expr(polqual, polrelid), '') || ' / ' || coalesce(pg_get_expr(polwithcheck, polrelid), '')
      from pg_policy where polname like 'rv\_cascade\_%'
    union all
    select 'column default ' || adrelid::regclass::text || '.' || a.attname || ': ' || pg_get_expr(d.adbin, d.adrelid) || ' [' || a.attgenerated::text || ']'
      from pg_attrdef d join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum
      where d.adrelid in (select oid from pg_class where relnamespace = 'test_schema'::regnamespace and relname like 'rv\_cascade\_%')
    union all
    select 'constraint ' || conrelid::regclass::text || '.' || conname || ': ' || pg_get_constraintdef(oid)
      from pg_constraint
      where conrelid in (select oid from pg_class where relnamespace = 'test_schema'::regnamespace and relname like 'rv\_cascade\_%')
    union all
    select 'index ' || indexrelid::regclass::text || ': ' || pg_get_indexdef(indexrelid)
      from pg_index
      where indrelid in (select oid from pg_class where relnamespace = 'test_schema'::regnamespace and relname like 'rv\_cascade\_%')
    union all
    select 'routine ' || p.oid::regprocedure::text || ': ' || pg_get_functiondef(p.oid) || ' acl ' || coalesce(p.proacl::text, '(default)')
      from pg_proc p where p.pronamespace = 'test_schema'::regnamespace and p.proname like 'rv\_cascade\_%'
  ) s"
in_a=$($PSQL -d a -c "$q_objects"); in_b=$($PSQL -d b -c "$q_objects")
if [ "$in_a" != "$in_b" ]; then
  echo "::error::reg-cascade: domains, triggers, statistics, table columns, defaults, constraints, indexes or routines around the CASCADE-dropped routines differ between a and b"
  diff <(echo "$in_a") <(echo "$in_b") || true
  fail=1
fi

# 2. The migration detaches a domain default right before the routine it calls
#    is dropped with CASCADE (which would otherwise drop the domain and every
#    column of it), and sets it again afterwards.
line_of() { { grep -n -F "$1" "$OUT" || true; } | head -1 | cut -d: -f1; }
detach=$(line_of "alter domain test_schema.rv_cascade_code_def drop default;")
drop_fn=$(line_of "drop function if exists test_schema.rv_cascade_first_code () cascade;")
restore=$(line_of "alter domain test_schema.rv_cascade_code_def set default test_schema.rv_cascade_first_code();")
if [ -z "$detach" ] || [ -z "$drop_fn" ] || [ -z "$restore" ] || [ "$detach" -ge "$drop_fn" ] || [ "$drop_fn" -ge "$restore" ]; then
  echo "::error::reg-cascade: expected detach < drop function < set default for rv_cascade_code_def (lines '$detach' '$drop_fn' '$restore')"
  fail=1
fi

# 3. Spot checks that the objects are really there and work in a.
expect() {  # expect <label> <sql> <expected>
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::reg-cascade: $1: expected '$3', got '$got'"; fail=1; fi
}
# The same for a statement that writes, run in a transaction rolled back.
expect_rolled_back() {  # expect_rolled_back <label> <sql> <expected>
  got=$($PSQL -q -d a -c "begin" -c "$2" -c "rollback" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::reg-cascade: $1: expected '$3', got '$got'"; fail=1; fi
}
expect_error() {  # expect_error <label> <sql> <error text>; <sql> is rolled back
  got=$($PSQL -q -d a -c "begin" -c "$2" -c "rollback" 2>&1 || true)
  case "$got" in
    *"$3"*) ;;
    *) echo "::error::reg-cascade: $1: expected an error containing '$3', got '$got'"; fail=1 ;;
  esac
}
expect "column retyped under the SQL-standard readers" "select format_type(atttypid, atttypmod) from pg_attribute where attrelid = 'test_schema.rv_cascade_codes'::regclass and attname = 'code'" "character varying(10)"
expect "data of the domain-default column kept" "select fallback from test_schema.rv_cascade_orders where id = 1" "kept"
expect "data of the return-type domain-default column kept" "select preset from test_schema.rv_cascade_scores where id = 1" "42"
expect_rolled_back "domain default calling the SQL-standard reader" "insert into test_schema.rv_cascade_orders (id, code, checked) values (3, 'ok', 'ok') returning fallback" "ok"
expect_rolled_back "domain default calling the re-typed routine" "insert into test_schema.rv_cascade_scores (id, qty, positive) values (2, 1, 1) returning preset" "7"
expect_error "domain CHECK over the SQL-standard reader enforced" "select 'zzz'::test_schema.rv_cascade_code_d" "violates check constraint"
expect_error "domain CHECK over the re-typed routine enforced" "select (-1)::test_schema.rv_cascade_score_d" "violates check constraint"
expect_error "WHEN trigger over the SQL-standard reader fires" "insert into test_schema.rv_cascade_orders (id, code, checked) values (4, 'nope', 'ok')" "rv_cascade: rejected"
expect_error "WHEN trigger over the re-typed routine fires" "insert into test_schema.rv_cascade_scores (id, qty) values (5, -1)" "rv_cascade: rejected"
expect "expression statistics restored" "select string_agg(stxname, ',' order by stxname) from pg_statistic_ext where stxname like 'rv\_cascade\_%'" "rv_cascade_orders_st,rv_cascade_scores_st"
expect "TO-only CHECK, indexes and generated column over the re-typed routine" "select string_agg(n, ',' order by n) from (select conname::text as n from pg_constraint where conname = 'rv_cascade_scores_qty_chk' union all select relname::text from pg_class where relname in ('rv_cascade_scores_score_idx', 'rv_cascade_scores_doubled_idx') union all select attname::text from pg_attribute where attrelid = 'test_schema.rv_cascade_scores'::regclass and attname = 'doubled' and attgenerated = 's') s" "doubled,rv_cascade_scores_doubled_idx,rv_cascade_scores_qty_chk,rv_cascade_scores_score_idx"
expect "the table phase filled the new column through the argument-default user" "select bonus from test_schema.rv_cascade_scores where id = 1" "1"
expect "SQL-standard body and view taken through the argument-default user re-created" "select r from test_schema.rv_cascade_score_view" "1"
expect "indexes on the materialized views restored" "select string_agg(relname, ',' order by relname) from pg_class where relname in ('rv_cascade_orders_mv_norm_idx', 'rv_cascade_scores_mv_idx')" "rv_cascade_orders_mv_norm_idx,rv_cascade_scores_mv_idx"
expect "foreign-table column defaults restored" "select string_agg(a.attname || '=' || pg_get_expr(d.adbin, d.adrelid), ',' order by a.attname) from pg_attrdef d join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum where d.adrelid = 'test_schema.rv_cascade_ft'::regclass" "checked=test_schema.rv_cascade_is_valid('ok'::text),code=test_schema.rv_cascade_first_code(),preset=test_schema.rv_cascade_score(9)"
expect "what the table phase leaves for after the predropped routines" "select string_agg(n, ',' order by n) from (select conname::text as n from pg_constraint where conname in ('rv_cascade_orders_code_chk', 'rv_cascade_fresh_chk') union all select relname::text from pg_class where relname = 'rv_cascade_orders_norm_idx' union all select polname::text from pg_policy where polname = 'rv_cascade_orders_pol') s" "rv_cascade_fresh_chk,rv_cascade_orders_code_chk,rv_cascade_orders_norm_idx,rv_cascade_orders_pol"
expect "rewritten CHECK no longer calling the predropped routine" "select pg_get_constraintdef(oid) from pg_constraint where conname = 'rv_cascade_orders_id_chk'" "CHECK ((id > 0))"
if [ "$ver" -ge 150000 ]; then
  # From PostgreSQL 15 a partition's trigger clone is an ordinary trigger.
  expect "partition trigger clones keep their own state and comment" "select string_agg(tgrelid::regclass::text || ':' || tgenabled::text || ':' || coalesce(obj_description(oid, 'pg_trigger'), '-'), ',' order by tgrelid::regclass::text) from pg_trigger where tgname = 'rv_cascade_parted_chk'" "test_schema.rv_cascade_parted:O:-,test_schema.rv_cascade_parted_1:D:-,test_schema.rv_cascade_parted_2:O:rv_cascade: clone comment"
fi
expect "routines whose argument defaults call them re-created" "select test_schema.rv_cascade_flag()::text || ',' || test_schema.rv_cascade_score_or()" "true,1"
expect "privileges of the re-created argument-default user" "select has_function_privilege('pgc_pg19_sqlbody_reader', 'test_schema.rv_cascade_flag(boolean)', 'execute')::text || ',' || coalesce((select bool_or(a.grantee = 0) from pg_proc p, aclexplode(p.proacl) a where p.oid = 'test_schema.rv_cascade_flag(boolean)'::regprocedure), false)::text" "true,false"
[ "$fail" -eq 0 ] && echo "reg-cascade: a matches b (server_version_num $ver)"
exit "$fail"
