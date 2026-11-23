#!/usr/bin/env bash
# CI step (fix:sqlbody), run after "Apply diff to database a" (round 1).
# SQL-standard routine bodies (BEGIN ATOMIC ... END / RETURN expr, PG14+).
# pgc 1.0.x dumped them empty on BOTH sides, so round 2 stayed empty while the
# migrated database kept stale bodies and empty `$$$$` procedures; and such a
# body is a hard dependent of what it uses, which pgc now drops and re-creates
# around. Compare a and b directly.
# Env: PGPASSWORD (PGPORT optional, default 5432); OUTPUT_SQL = round-1 SQL.
set -euo pipefail
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
ver=$($PSQL -d a -c "show server_version_num")
fail=0
if [ "$ver" -lt 140000 ]; then
  echo "sqlbody: SQL-standard bodies need PostgreSQL 14+ (server_version_num $ver), skipped"
  exit 0
fi

# 1. The migration never replays a routine with an empty string body.
if grep -n 'as \$\$\$\$' "$OUT"; then
  echo "::error::sqlbody: the round-1 migration emits an empty routine body (as \$\$\$\$)"
  fail=1
fi

# 2. After the migration every routine of the compared schemas is defined in a
#    exactly as in b: pg_get_functiondef covers the body (either form), the
#    language, every option and the SET clauses. Like pgc's own routine query,
#    C and internal functions (range constructors, ...) are left out, and so
#    are aggregates, which have no functiondef (another step covers them), and
#    extension members, which are not pgc's to migrate.
q_defs="select coalesce(string_agg(p.oid::regprocedure::text || E'\n' || pg_get_functiondef(p.oid), E'\n----\n' order by p.oid::regprocedure::text), '')
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace join pg_language l on l.oid = p.prolang
  where n.nspname in ('test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public')
    and p.prokind <> 'a' and l.lanname not in ('c', 'internal')
    and not exists (select 1 from pg_depend d where d.classid = 'pg_proc'::regclass and d.objid = p.oid and d.deptype = 'e')"
in_a=$($PSQL -d a -c "$q_defs"); in_b=$($PSQL -d b -c "$q_defs")
if [ "$in_a" != "$in_b" ]; then
  echo "::error::sqlbody: routine definitions of database a differ from b after applying the migration"
  diff <(echo "$in_a") <(echo "$in_b") || true
  fail=1
fi

# 3. What the SQL-standard routines drag along when pgc drops them ahead of a
#    relation change and re-creates them after it: the views calling them, a
#    CHECK constraint and a column default calling them, and their privileges;
#    plus the fixture's operators and casts, which the deferred routines use.
q_deps="select coalesce(string_agg(x, E'\n' order by x), '') from (
    select 'view ' || c.oid::regclass::text || ': ' || pg_get_viewdef(c.oid) as x
      from pg_class c where c.relkind = 'v' and c.relname like 'pg19\_sqlbody\_%'
    union all
    select 'constraint ' || conname || ': ' || pg_get_constraintdef(oid)
      from pg_constraint where conname like 'pg19\_sqlbody\_%'
    union all
    select 'default ' || c.relname || '.' || a.attname || ': ' || pg_get_expr(d.adbin, d.adrelid)
      from pg_attrdef d join pg_class c on c.oid = d.adrelid
      join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum
      where c.relname like 'pg19\_sqlbody\_%'
    union all
    select 'acl ' || p.oid::regprocedure::text || ': ' || coalesce(p.proacl::text, '(default)')
      from pg_proc p where p.proname like 'pg19\_sqlbody\_%'
    union all
    select 'operator ' || o.oid::regoperator::text || ': ' || o.oprcode::regprocedure::text
      from pg_operator o where o.oprnamespace = 'test_schema'::regnamespace and o.oprname in ('===', '<=>')
    union all
    select 'cast ' || c.castsource::regtype::text || ' -> ' || c.casttarget::regtype::text
      from pg_cast c where c.casttarget::regtype::text like '%pg19\_sqlbody\_%'
  ) s"
deps_a=$($PSQL -d a -c "$q_deps"); deps_b=$($PSQL -d b -c "$q_deps")
if [ "$deps_a" != "$deps_b" ]; then
  echo "::error::sqlbody: views, constraints, defaults, privileges, operators or casts around the SQL-standard routines differ between a and b"
  diff <(echo "$deps_a") <(echo "$deps_b") || true
  fail=1
fi

# 4. Spot checks that the fixture really reached a (not merely a == b).
expect() {  # expect <label> <sql> <expected>
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::sqlbody: $1: expected '$3', got '$got'"; fail=1; fi
}
# SQL-standard bodies stay SQL-standard (prosqlbody set, prosrc empty).
sqlbody_fns="pg19_sqlbody_a_caller,pg19_sqlbody_cols_max,pg19_sqlbody_cols_old,pg19_sqlbody_eq,pg19_sqlbody_eq_or_null,pg19_sqlbody_literals,pg19_sqlbody_ne,pg19_sqlbody_options,pg19_sqlbody_parted_count,pg19_sqlbody_proc_changed,pg19_sqlbody_proc_to_only,pg19_sqlbody_return,pg19_sqlbody_rt_callee,pg19_sqlbody_rt_caller,pg19_sqlbody_shape_count,pg19_sqlbody_to_atomic,pg19_sqlbody_unchanged,pg19_sqlbody_uses_cast,pg19_sqlbody_uses_new_op,pg19_sqlbody_uses_op,pg19_sqlbody_z_callee"
[ "$ver" -ge 190000 ] && sqlbody_fns="${sqlbody_fns/pg19_sqlbody_unchanged,/pg19_sqlbody_unchanged,pg19_sqlbody_upsert,}"
expect "SQL-standard bodies in a" "select string_agg(proname, ',' order by proname) from pg_proc where pronamespace = 'test_schema'::regnamespace and proname like 'pg19\_sqlbody\_%' and prosqlbody is not null and prosrc = ''" "$sqlbody_fns"
expect "public SQL-standard bodies" "select string_agg(proname, ',' order by proname) from pg_proc where pronamespace = 'public'::regnamespace and proname like 'pg19\_sqlbody\_%' and prosqlbody is not null and prosrc = ''" "pg19_sqlbody_a_pub_caller,pg19_sqlbody_pub_view_count,pg19_sqlbody_public_count,pg19_sqlbody_z_pub_callee"
# The body is dumped with an empty search_path, so the migration names the
# public callee qualified (under the default path it printed as a bare call).
if ! grep -q 'SELECT (public.pg19_sqlbody_z_pub_callee(p) + 1)' "$OUT"; then
  echo "::error::sqlbody: the migration does not create public.pg19_sqlbody_a_pub_caller with a qualified callee"
  fail=1
fi
expect "SQL-standard body turned string body" "select prosqlbody is null and prosrc like '%SELECT p * 3%' from pg_proc where oid = 'test_schema.pg19_sqlbody_to_string(integer)'::regprocedure" "t"
expect "FROM-only routines dropped" "select count(*) from pg_proc where proname in ('pg19_sqlbody_proc_from_only', 'pg19_sqlbody_legacy_count', 'pg19_sqlbody_legacy_next')" "0"
expect "FROM-only relations under SQL-standard readers dropped" "select count(*) from pg_class where relnamespace = 'test_schema'::regnamespace and relname in ('pg19_sqlbody_legacy', 'pg19_sqlbody_legacy_seq')" "0"
expect "column under a SQL-standard reader dropped" "select count(*) from pg_attribute where attrelid = 'test_schema.pg19_sqlbody_cols'::regclass and attname = 'old_col' and not attisdropped" "0"
expect "column under a SQL-standard reader retyped" "select atttypid::regtype from pg_attribute where attrelid = 'test_schema.pg19_sqlbody_cols'::regclass and attname = 'v'" "bigint"
expect "table under a SQL-standard reader rebuilt" "select pg_get_partkeydef('test_schema.pg19_sqlbody_parted'::regclass)" "RANGE (k)"
expect "view under a SQL-standard reader reshaped" "select string_agg(attname, ',' order by attnum) from pg_attribute where attrelid = 'test_schema.pg19_sqlbody_shape_view'::regclass and attnum > 0" "id,b"
expect "RETURN body changed" "select test_schema.pg19_sqlbody_return(1)" "3"
expect "caller created after its callee" "select test_schema.pg19_sqlbody_a_caller(5)" "40"
expect "public caller created after its callee" "select public.pg19_sqlbody_a_pub_caller(5)" "11"
expect "caller of a re-typed callee re-created" "select test_schema.pg19_sqlbody_rt_caller(1)" "4"
expect "routines using a new operator and cast created after them" "select test_schema.pg19_sqlbody_uses_new_op(1)::text || ',' || (test_schema.pg19_sqlbody_uses_cast(7)).b || ',' || (select nonzero from test_schema.pg19_sqlbody_new_op_view)" "true,7,true"
expect "user of a rebuilt operator re-created on the new one" "select test_schema.pg19_sqlbody_uses_op(null)::text" "true"
expect "CR LF and \$\$ literals kept byte for byte" "select test_schema.pg19_sqlbody_literals() = E'line1\r\nline2\$\$ not a delimiter \$\$'" "t"
expect "procedure body changed" "select pg_get_function_sqlbody('test_schema.pg19_sqlbody_proc_changed(integer)'::regprocedure) like '%DO UPDATE SET val = excluded.val%'" "t"
if [ "$ver" -ge 190000 ]; then
  expect "PG19 ON CONFLICT DO SELECT body" "select pg_get_function_sqlbody('test_schema.pg19_sqlbody_upsert(integer)'::regprocedure) like '%DO SELECT FOR UPDATE%'" "t"
fi
[ "$fail" -eq 0 ] && echo "sqlbody: a matches b (server_version_num $ver)"
exit "$fail"
