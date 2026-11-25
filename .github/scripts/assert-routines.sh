#!/usr/bin/env bash
# CI step (pg19:routines), run after "Apply diff to database a" (round 1).
# After the migration, a must match b for every routine property this WP emits,
# including properties round 2 cannot see when both sides lose them.
# Env: PGPASSWORD=postgres (PGPORT optional, default 5432).
set -euo pipefail
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
ver=$($PSQL -d a -c "show server_version_num")
q_agg="select coalesce(string_agg(format('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s', p.oid::regprocedure, p.prokind, p.proparallel, p.prosupport::regproc, a.aggkind, a.aggnumdirectargs, a.aggtransspace, a.aggmtransspace, coalesce(a.agginitval, '-'), a.aggcombinefn::regproc, a.aggsortop::regoper), E'\n' order by p.oid::regprocedure::text), '') from pg_aggregate a join pg_proc p on p.oid = a.aggfnoid join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'test_schema'"
q_kind="select coalesce(string_agg(format('%s|%s', p.oid::regprocedure, p.prokind), E'\n' order by p.oid::regprocedure::text), '') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'test_schema' and p.proname like 'pg19\_routines\_%'"
q_cfg="select coalesce(string_agg(format('%s|%s', p.oid::regprocedure, array_to_string(p.proconfig, ' ; ')), E'\n' order by p.oid::regprocedure::text), '') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname in ('test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public') and p.proconfig is not null and not exists (select 1 from pg_depend d where d.classid = 'pg_proc'::regclass and d.objid = p.oid and d.deptype = 'e')"
# Effective privileges (a NULL proacl is acldefault), order-insensitive.
q_acl="select coalesce(string_agg(format('%s|%s', p.oid::regprocedure, (select string_agg(x::text, ',' order by x::text) from unnest(coalesce(p.proacl, acldefault('f', p.proowner))) x)), E'\n' order by p.oid::regprocedure::text), '') from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'test_schema' and p.proname like 'pg19\_routines\_%'"
fail=0
for check in agg kind cfg acl; do
  q_var="q_$check"; q=${!q_var}
  in_a=$($PSQL -d a -c "$q"); in_b=$($PSQL -d b -c "$q")
  if [ "$in_a" != "$in_b" ]; then
    echo "::error::routines ($check): database a differs from b after applying the migration"
    diff <(echo "$in_a") <(echo "$in_b") || true
    fail=1
  fi
done
# Spot checks that the fixture values really reached a (not merely a == b).
expect() {  # expect <label> <sql> <expected>
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::routines: $1: expected '$3', got '$got'"; fail=1; fi
}
expect "negative SSPACE" "select aggtransspace from pg_aggregate where aggfnoid = 'test_schema.pg19_routines_agg_sspace(integer)'::regprocedure" "-1"
expect "negative MSSPACE" "select aggmtransspace from pg_aggregate where aggfnoid = 'test_schema.pg19_routines_agg_msspace(integer)'::regprocedure" "-1"
expect "aggregate PARALLEL" "select proparallel from pg_proc where oid = 'test_schema.pg19_routines_agg_parallel(bigint)'::regprocedure" "s"
expect "aggregate kind change" "select aggkind from pg_aggregate where aggfnoid = 'test_schema.pg19_routines_agg_kind(\"any\")'::regprocedure" "h"
expect "function to aggregate" "select prokind from pg_proc where oid = 'test_schema.pg19_routines_fn_to_agg(bigint)'::regprocedure" "a"
expect "view on replaced aggregate survives" "select count(*) from pg_views where schemaname = 'test_schema' and viewname = 'pg19_routines_agg_initcond_v'" "1"
expect "SET search_path = ''" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_empty_string()'::regprocedure" 'search_path=""'
expect "70-byte search_path element" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_long_element()'::regprocedure" 'search_path=pg19_routines_a_schema_name_that_is_longer_than_sixty_three_bytes_x, public'
expect "aggregate SORTOP" "select aggsortop::regoper from pg_aggregate where aggfnoid = 'test_schema.pg19_routines_agg_sortop(integer)'::regprocedure" "pg_catalog.>"
expect "numeric search_path element" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_numeric()'::regprocedure" 'search_path=public, 42'
expect "numeric element beside an empty one" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_numeric_mixed()'::regprocedure" 'search_path="", 7'
# An explicit grant (has_function_privilege would also see PUBLIC's EXECUTE).
expect "grant survives a kind change" "select p.prokind::text || exists (select 1 from aclexplode(p.proacl) e where e.grantee = 'pgc_pg19_routines_role'::regrole and e.privilege_type = 'EXECUTE')::text from pg_proc p where p.oid = 'test_schema.pg19_routines_grant_kind(bigint)'::regprocedure" "atrue"
expect "PUBLIC revoke survives a return type change" "select format_type(p.prorettype, null) || ' ' || has_function_privilege('public', p.oid, 'EXECUTE')::text from pg_proc p where p.oid = 'test_schema.pg19_routines_grant_revoked(bigint)'::regprocedure" "integer false"
if [ "$($PSQL -d a -c "select count(*) from pg_settings where name = 'output_plugin_libraries'")" = "1" ]; then
  expect "output_plugin_libraries list" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_output_plugins()'::regprocedure" 'output_plugin_libraries=pgoutput, test_decoding'
fi
if [ "$ver" -ge 190000 ]; then
  expect "PG19 aggregate SUPPORT" "select string_agg(p.proname || '=' || p.prosupport::regproc, ',' order by p.proname) from pg_proc p where p.pronamespace = 'test_schema'::regnamespace and p.proname like 'pg19\_routines\_support\_%'" "pg19_routines_support_add=int8inc_support,pg19_routines_support_drop=-,pg19_routines_support_new=int8inc_support,pg19_routines_support_same=int8inc_support"
  expect "PG19 SET search_path TO NULL" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_to_null()'::regprocedure" 'search_path='
  expect "PG19 SET temp_tablespaces TO NULL" "select array_to_string(proconfig, ' ; ') from pg_proc where oid = 'test_schema.pg19_routines_guc_null_proc()'::regprocedure" 'temp_tablespaces='
fi
[ "$fail" -eq 0 ] && echo "routines: a matches b (server_version_num $ver)"
exit "$fail"