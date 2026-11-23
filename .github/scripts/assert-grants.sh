#!/usr/bin/env bash
# CI step (fix:grants), run after "Apply diff to database a" (round 1).
# After the migration, a must hold the same privileges as b on every fix:grants
# fixture object (grantor-insensitive, as pgc compares them), no entry a
# delegate granted may survive where b dropped it, and the round-1 SQL must
# have revoked those entries by naming their grantor, in the form the migrated
# server accepts. Works on default and OUTPUT_FOR_PRODUCTION output (whose
# grantor revokes sit, indented, inside guarded DO blocks).
# Env: PGPASSWORD=postgres (PGPORT optional, default 5432).
set -euo pipefail
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
ver=$($PSQL -d a -c "show server_version_num")
# One line per object: label|grantee:privilege:grantable,... merged over grantors.
acl_rows() {  # acl_rows <label> <aclitem[] expression> <catalog> <condition> [<grantee filter>]
  echo "select '$1|' || coalesce((select string_agg(r, ',' order by r) from (select format('%s:%s:%s', g, x.privilege_type, bool_or(x.is_grantable)) as r from $3, aclexplode($2) x, coalesce(nullif(pg_get_userbyid(x.grantee), 'unknown (OID=0)'), 'PUBLIC') g where ($4) and ${5:-true} group by g, x.privilege_type) s), '')"
}
rel() {  # rel <label> <table>
  acl_rows "$1" "coalesce(c.relacl, acldefault('r', c.relowner))" "pg_class c" "c.oid = 'public.$2'::regclass"
}
col() {  # col <label> <table> <column>
  acl_rows "$1" "a.attacl" "pg_attribute a" "a.attrelid = 'public.$2'::regclass and a.attname = '$3'"
}
q="$(rel chain pg19_grants_chain)
union all $(col chain.note pg19_grants_chain note)
union all $(rel opt pg19_grants_opt)
union all $(acl_rows seq "coalesce(c.relacl, acldefault('s', c.relowner))" "pg_class c" "c.oid = 'public.pg19_grants_seq'::regclass")
union all $(acl_rows fn "coalesce(p.proacl, acldefault('f', p.proowner))" "pg_proc p" "p.oid = 'public.pg19_grants_fn()'::regprocedure")
union all $(rel granted_by pg19_grants_granted_by)
union all $(col granted_by.note pg19_grants_granted_by note)
union all $(rel same pg19_grants_same)
union all $(rel colkeep pg19_grants_colkeep)
union all $(col colkeep.note pg19_grants_colkeep note)
union all $(rel coldep pg19_grants_coldep)
union all $(col coldep.note pg19_grants_coldep note)
union all $(acl_rows schema.public "coalesce(n.nspacl, acldefault('n', n.nspowner))" "pg_namespace n" "n.nspname = 'public'" "g ilike 'pgc\_pg19\_grants\_%'")"
fail=0
in_a=$($PSQL -d a -c "$q"); in_b=$($PSQL -d b -c "$q")
if [ "$in_a" != "$in_b" ]; then
  echo "::error::grants: privileges in database a differ from b after applying the migration"
  diff <(echo "$in_a") <(echo "$in_b") || true
  fail=1
fi
expect() {  # expect <label> <sql> <expected>
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::grants: $1: expected '$3', got '$got'"; fail=1; fi
}
# Grantor-aware: nothing the delegates granted survives where b dropped it.
delegated="pg_get_userbyid(x.grantor) in ('pgc_pg19_grants_delegate', 'pgc_pg19_grants_sub', 'pgc_pg19_Grants_Mixed')"
tables="('public.pg19_grants_chain'::regclass, 'public.pg19_grants_opt'::regclass, 'public.pg19_grants_seq'::regclass, 'public.pg19_grants_granted_by'::regclass, 'public.pg19_grants_colkeep'::regclass, 'public.pg19_grants_coldep'::regclass)"
expect "delegated table/sequence entries left" "select count(*) from pg_class c, aclexplode(c.relacl) x where c.oid in $tables and $delegated" "0"
expect "delegated column entries left" "select count(*) from pg_attribute a, aclexplode(a.attacl) x where a.attrelid in $tables and $delegated" "0"
expect "delegated function entries left" "select count(*) from pg_proc p, aclexplode(p.proacl) x where p.oid = 'public.pg19_grants_fn()'::regprocedure and $delegated" "0"
expect "delegated schema entries left" "select count(*) from pg_namespace n, aclexplode(n.nspacl) x where n.nspname = 'public' and $delegated" "0"
# The grant-option downgrade: chain gone, reader's SELECT granted back by the owner.
acl_text="select array_to_string(array(select i::text from unnest((select relacl from pg_class where oid = 'public.pg19_grants_opt'::regclass)) i order by 1), ',')"
expect "pg19_grants_opt ACL (grantors included)" "$acl_text" "$($PSQL -d b -c "$acl_text")"
# The round-1 SQL named the grantor, in the form the migrated server accepts.
# Leading whitespace is dropped so the guarded (production) form matches too.
stmts=$(sed 's/^[[:space:]]*//' "$OUT")
has() { grep -qxF "$1" <<<"$stmts" || { echo "::error::grants: round-1 SQL lacks: $1"; fail=1; }; }
if [ "$ver" -ge 190000 ]; then
  has "REVOKE SELECT, UPDATE ON TABLE public.pg19_grants_chain FROM pgc_pg19_grants_reader GRANTED BY pgc_pg19_grants_delegate;"
  has "REVOKE SELECT ON TABLE public.pg19_grants_opt FROM pgc_pg19_grants_reader GRANTED BY pgc_pg19_grants_sub;"
  has "REVOKE SELECT (note) ON TABLE public.pg19_grants_granted_by FROM pgc_pg19_grants_sub GRANTED BY pgc_pg19_grants_delegate;"
  has "REVOKE USAGE ON SEQUENCE public.pg19_grants_seq FROM pgc_pg19_grants_reader GRANTED BY \"pgc_pg19_Grants_Mixed\";"
  has "REVOKE SELECT (note) ON TABLE public.pg19_grants_coldep FROM pgc_pg19_grants_reader GRANTED BY pgc_pg19_grants_delegate;"
  if grep -q '^SET ROLE' <<<"$stmts"; then echo "::error::grants: SET ROLE emitted for a PG19+ migration"; fail=1; fi
else
  has "SET ROLE pgc_pg19_grants_delegate;"
  has "SET ROLE pgc_pg19_grants_sub;"
  has "SET ROLE \"pgc_pg19_Grants_Mixed\";"
  has "REVOKE SELECT, UPDATE ON TABLE public.pg19_grants_chain FROM pgc_pg19_grants_reader;"
  has "REVOKE SELECT (note) ON TABLE public.pg19_grants_coldep FROM pgc_pg19_grants_reader;"
  if grep -q 'GRANTED BY' <<<"$stmts"; then echo "::error::grants: GRANTED BY emitted for a pre-19 migration"; fail=1; fi
fi
# A table-level REVOKE also strips the columns: what b keeps comes back in round 1.
has "GRANT SELECT (note) ON TABLE public.pg19_grants_colkeep TO pgc_pg19_grants_reader;"
has "GRANT SELECT (note) ON TABLE public.pg19_grants_colkeep TO pgc_pg19_grants_sub;"
has "GRANT SELECT (note) ON TABLE public.pg19_grants_coldep TO pgc_pg19_grants_delegate WITH GRANT OPTION;"
# reg-misc: privileges the table DDL takes away. A re-added column
# (rv_regmisc_grants_readd.g) and a recreated table (rv_regmisc_grants_rebuilt)
# start with none: b's come back in round 1 and nothing is revoked from them
# (a grantor-named REVOKE there fails with "permission denied for column"). A
# partition retyped through its parent (rv_regmisc_grants_pt1) is not dropped
# and loses what b does not have, the delegate's grant included.
rq="$(rel rv_readd rv_regmisc_grants_readd)
union all $(col rv_readd.g rv_regmisc_grants_readd g)
union all $(col rv_readd.note rv_regmisc_grants_readd note)
union all $(rel rv_rebuilt rv_regmisc_grants_rebuilt)
union all $(col rv_rebuilt.code rv_regmisc_grants_rebuilt code)
union all $(rel rv_pt1 rv_regmisc_grants_pt1)"
rv_a=$($PSQL -d a -c "$rq"); rv_b=$($PSQL -d b -c "$rq")
if [ "$rv_a" != "$rv_b" ]; then
  echo "::error::grants: rv_regmisc privileges in database a differ from b after applying the migration"
  diff <(echo "$rv_a") <(echo "$rv_b") || true
  fail=1
fi
rv_tables="('public.rv_regmisc_grants_readd'::regclass, 'public.rv_regmisc_grants_rebuilt'::regclass, 'public.rv_regmisc_grants_pt1'::regclass)"
expect "rv_regmisc delegated table entries left" "select count(*) from pg_class c, aclexplode(c.relacl) x where c.oid in $rv_tables and $delegated" "0"
# (A dropped column keeps its attacl on some majors, hence attisdropped.)
expect "rv_regmisc delegated column entries left" "select count(*) from pg_attribute a, aclexplode(a.attacl) x where a.attrelid in $rv_tables and not a.attisdropped and $delegated" "0"
if grep -E '^REVOKE .* ON TABLE public\.rv_regmisc_grants_(readd|rebuilt) ' <<<"$stmts"; then
  echo "::error::grants: round-1 SQL revokes privileges the table DDL already took away"; fail=1
fi
has "GRANT SELECT (g) ON TABLE public.rv_regmisc_grants_readd TO pgc_pg19_grants_reader;"
has "GRANT SELECT (g) ON TABLE public.rv_regmisc_grants_readd TO pgc_pg19_grants_delegate WITH GRANT OPTION;"
has "GRANT SELECT (code) ON TABLE public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_reader;"
sets=$(grep -c '^SET ROLE ' <<<"$stmts" || true); resets=$(grep -c '^RESET ROLE;$' <<<"$stmts" || true)
[ "$sets" = "$resets" ] || { echo "::error::grants: $sets SET ROLE but $resets RESET ROLE"; fail=1; }
exit $fail
