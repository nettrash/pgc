#!/usr/bin/env bash
# CI step (pre-cascade), run after "Apply diff to database a" (round 1).
# What a routine's DROP ... CASCADE takes through a view: the routine phase
# drops data.rv_pcascade_norm (its return type changes) with CASCADE, which
# takes every view calling it and everything reading those. Before the fix
# the migration re-created only the views calling the routine: the view over
# one, the materialized view (with its index and statistics), the
# SQL-standard routine reading them and the rule on one were lost until the
# next compare, and a rule reading one failed to re-create. The views it did
# re-create came back without their privileges, and the routines the
# migration creates (new, or dropped and re-created) kept what the schema's
# default privileges for functions grant although TO revokes it. A second
# compare cannot see a property lost on both sides alike, so compare a and b
# directly, and check the objects work in a. A view calling an unchanged
# overload of the routine is not taken by the CASCADE and must not be dropped
# and rebuilt (it has an INSTEAD OF trigger pgc does not model), and a routine
# the migration creates with a NULL ACL under default privileges it revokes
# must not lose PUBLIC's EXECUTE on the next compare.
# Env: PGPASSWORD (PGPORT optional, default 5432); OUTPUT_SQL = round-1 SQL.
set -euo pipefail
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
ver=$($PSQL -d a -c "show server_version_num")
fail=0
if [ "$ver" -lt 140000 ]; then
  echo "pre-cascade: SQL-standard bodies need PostgreSQL 14+ (server_version_num $ver), skipped"
  exit 0
fi

# 1. Views, materialized views, their columns, owners, privileges, comments,
#    indexes, statistics and triggers, the rule, the routines with their
#    privileges (a NULL ACL read as the default it stands for) and the default
#    privileges for functions are the same in a and b.
q_objects="select coalesce(string_agg(x, E'\n' order by x), '') from (
    select 'relation ' || c.oid::regclass::text || ' [' || c.relkind::text || '] owner ' || pg_get_userbyid(c.relowner)
           || ' populated ' || c.relispopulated::text
           || ' acl ' || coalesce((select string_agg(a::text, ',' order by a::text) from unnest(c.relacl) a), '(default)')
           || ' comment ' || coalesce(obj_description(c.oid, 'pg_class'), '(none)')
           || case when c.relkind in ('v', 'm') then ': ' || pg_get_viewdef(c.oid) else '' end as x
      from pg_class c
      where c.relnamespace in ('test_schema'::regnamespace, 'public'::regnamespace) and c.relkind in ('r', 'v', 'm') and c.relname like 'rv\_pcascade\_%'
    union all
    select 'column ' || attrelid::regclass::text || '.' || attname || ': ' || format_type(atttypid, atttypmod)
           || ' comment ' || coalesce(col_description(attrelid, attnum), '(none)')
      from pg_attribute
      where attrelid in (select oid from pg_class where relnamespace in ('test_schema'::regnamespace, 'public'::regnamespace) and relname like 'rv\_pcascade\_%' and relkind in ('r', 'v', 'm'))
        and attnum > 0 and not attisdropped
    union all
    select 'index ' || indexrelid::regclass::text || ': ' || pg_get_indexdef(indexrelid)
           || ' comment ' || coalesce(obj_description(indexrelid, 'pg_class'), '(none)')
      from pg_index
      where indrelid in (select oid from pg_class where relnamespace in ('test_schema'::regnamespace, 'public'::regnamespace) and relname like 'rv\_pcascade\_%')
    union all
    select 'trigger ' || tgrelid::regclass::text || '.' || tgname || ': ' || pg_get_triggerdef(oid)
      from pg_trigger
      where not tgisinternal
        and tgrelid in (select oid from pg_class where relnamespace in ('test_schema'::regnamespace, 'public'::regnamespace) and relname like 'rv\_pcascade\_%')
    union all
    select 'statistics ' || stxname || ': ' || pg_get_statisticsobjdef(oid)
      from pg_statistic_ext where stxnamespace = 'test_schema'::regnamespace and stxname like 'rv\_pcascade\_%'
    union all
    select 'rule ' || ev_class::regclass::text || '.' || rulename || ': ' || pg_get_ruledef(oid)
      from pg_rewrite where rulename like 'rv\_pcascade\_%'
    union all
    select 'routine ' || p.oid::regprocedure::text || ': ' || pg_get_functiondef(p.oid)
           || ' acl ' || (select coalesce(string_agg(a::text, ',' order by a::text), '(none)') from unnest(coalesce(p.proacl, acldefault('f', p.proowner))) a)
      from pg_proc p where p.pronamespace = 'data'::regnamespace and p.proname like 'rv\_pcascade\_%'
    union all
    select 'default privileges ' || pg_get_userbyid(defaclrole) || ' ' || defaclobjtype::text || ': '
           || coalesce((select string_agg(a::text, ',' order by a::text) from unnest(defaclacl) a), '')
      from pg_default_acl where defaclnamespace = 'data'::regnamespace
  ) s"
in_a=$($PSQL -d a -c "$q_objects"); in_b=$($PSQL -d b -c "$q_objects")
if [ "$in_a" != "$in_b" ]; then
  echo "::error::pre-cascade: views, materialized views, indexes, statistics, rules, routines or privileges around the CASCADE-dropped routine differ between a and b"
  diff <(echo "$in_a") <(echo "$in_b") || true
  fail=1
fi
if [ -z "$in_b" ]; then
  echo "::error::pre-cascade: the rv_pcascade fixture is missing from b"
  fail=1
fi

# 2. The round-1 migration dropped the view that others read ahead of them,
#    and left the one nothing reads to the routine's CASCADE.
line_of() { { grep -n -i -E "$1" "$OUT" || true; } | head -1 | cut -d: -f1; }
drop_v=$(line_of "^drop view (if exists )?test_schema\.rv_pcascade_items_v;")
drop_v2=$(line_of "^drop view (if exists )?test_schema\.rv_pcascade_items_v2;")
drop_solo=$(line_of "^drop view (if exists )?test_schema\.rv_pcascade_solo_v;")
drop_fn=$(line_of "^drop function if exists data\.rv_pcascade_norm \(t text\) cascade;")
create_v=$(line_of "^create or replace view test_schema\.rv_pcascade_items_v as")
if [ -z "$drop_v" ] || [ -z "$drop_v2" ] || [ -z "$drop_fn" ] || [ -z "$create_v" ] \
   || [ "$drop_v2" -ge "$drop_v" ] || [ "$drop_v" -ge "$drop_fn" ] || [ "$drop_fn" -ge "$create_v" ]; then
  echo "::error::pre-cascade: expected drop view items_v2 < drop view items_v < drop function norm < create view items_v (lines '$drop_v2' '$drop_v' '$drop_fn' '$create_v')"
  fail=1
fi
if [ -n "$drop_solo" ]; then
  echo "::error::pre-cascade: rv_pcascade_solo_v, which nothing reads, was dropped ahead (line $drop_solo)"
  fail=1
fi
drop_int=$(line_of "^drop (materialized )?view (if exists )?public\.rv_pcascade_int_(v|mv);")
if [ -n "$drop_int" ]; then
  echo "::error::pre-cascade: a view calling an unchanged overload of the helper, or the materialized view reading it, was dropped (line $drop_int)"
  fail=1
fi
public_revoke=$(line_of "^REVOKE EXECUTE ON FUNCTION data\.rv_pcascade_plain_(ret|new)\(.*\) FROM PUBLIC;")
if [ -n "$public_revoke" ]; then
  echo "::error::pre-cascade: PUBLIC's EXECUTE revoked from a routine whose TO ACL is NULL (line $public_revoke)"
  fail=1
fi

# 3. Spot checks that the objects are really there and work in a.
expect() {  # expect <label> <sql> <expected>
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::pre-cascade: $1: expected '$3', got '$got'"; fail=1; fi
}
expect "helper re-created with its new return type" "select pg_get_function_result('data.rv_pcascade_norm(text)'::regprocedure)" "character varying"
expect "view over the view, materialized view and SQL-standard reader re-created" "select (select count(*) from test_schema.rv_pcascade_items_v2) || ',' || (select count(*) from test_schema.rv_pcascade_items_mv) || ',' || data.rv_pcascade_count()" "2,2,2"
expect "rule on the view over the view re-created" "select count(*) from pg_rewrite where rulename = 'rv_pcascade_items_v2_del'" "1"
got=$($PSQL -q -d a -c "begin" -c "delete from test_schema.rv_pcascade_items_v2 where id = 1" \
  -c "select count(*) from test_schema.rv_pcascade_log" -c "rollback" 2>&1 || true)
if [ "$got" != "1" ]; then echo "::error::pre-cascade: rule on the view over the view works: expected '1', got '$got'"; fail=1; fi
expect "view privileges back" "select string_agg(v || ':' || has_table_privilege('pgc_pg19_pcascade_reader', 'test_schema.' || v, 'select')::text || ':' || has_table_privilege('pgc_grant_reader', 'test_schema.' || v, 'select')::text, ',' order by v collate \"C\") from unnest(array['rv_pcascade_items_v', 'rv_pcascade_items_v2', 'rv_pcascade_items_mv', 'rv_pcascade_solo_v']) v" "rv_pcascade_items_mv:true:true,rv_pcascade_items_v:true:false,rv_pcascade_items_v2:true:true,rv_pcascade_solo_v:true:false"
expect "EXECUTE the function default privileges grant revoked" "select count(*) from pg_proc p, aclexplode(p.proacl) a where p.oid = any (array['data.rv_pcascade_norm(text)', 'data.rv_pcascade_count()', 'data.rv_pcascade_new()', 'data.rv_pcascade_plain_ret(integer)', 'data.rv_pcascade_plain_new()']::regprocedure[]) and (a.grantee = 'pgc_pg19_pcascade_exec'::regrole or (a.grantee = 'pgc_pg19_pcascade_reader'::regrole and p.proname in ('rv_pcascade_new', 'rv_pcascade_plain_ret', 'rv_pcascade_plain_new')))" "0"
expect "PUBLIC's EXECUTE: kept on the helper and the new routines, revoked from the reader" "select string_agg(f || ':' || has_function_privilege('pgc_pg19_pcascade_owner', f, 'execute')::text, ',' order by f collate \"C\") from unnest(array['data.rv_pcascade_norm(text)', 'data.rv_pcascade_count()', 'data.rv_pcascade_new()', 'data.rv_pcascade_plain_ret(integer)', 'data.rv_pcascade_plain_new()']) f" "data.rv_pcascade_count():false,data.rv_pcascade_new():true,data.rv_pcascade_norm(text):true,data.rv_pcascade_plain_new():true,data.rv_pcascade_plain_ret(integer):true"
expect "view calling the unchanged overload keeps its trigger, and the delegate's grant is gone" "select (select count(*) from pg_trigger where tgrelid = 'public.rv_pcascade_int_v'::regclass and not tgisinternal) || ',' || has_table_privilege('pgc_pg19_pcascade_reader', 'public.rv_pcascade_int_v', 'select')::text || ',' || (select count(*) from public.rv_pcascade_int_mv)" "1,false,2"
# A routine whose TO ACL is NULL has PUBLIC's EXECUTE: test_schema.update_timestamp()
# loses a grant FROM has, and pgc 1.0.x / 1.1.0 revoked PUBLIC's EXECUTE along with it.
q_acl="select (select coalesce(string_agg(a::text, ',' order by a::text), '(none)') from unnest(coalesce(proacl, acldefault('f', proowner))) a) from pg_proc where oid = 'test_schema.update_timestamp()'::regprocedure"
acl_a=$($PSQL -d a -c "$q_acl" 2>&1 || true); acl_b=$($PSQL -d b -c "$q_acl" 2>&1 || true)
if [ "$acl_a" != "$acl_b" ]; then
  echo "::error::pre-cascade: test_schema.update_timestamp() privileges differ: a '$acl_a', b '$acl_b'"
  fail=1
fi
[ "$fail" -eq 0 ] && echo "pre-cascade: a matches b (server_version_num $ver)"
exit "$fail"
