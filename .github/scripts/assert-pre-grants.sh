#!/usr/bin/env bash
# CI step (fix:pre-grants), run after "Apply diff to database a" (round 1).
# After the migration, a must match b on what this package migrates, compared
# in the catalogs directly (round 2 cannot see a property both dumps lose):
#   - PUBLIC's EXECUTE / USAGE on every routine and type both databases have,
#     reading a NULL ACL as the default it stands for (acldefault);
#   - the privileges of the rv_pgrants_* objects, granted to roles whose names
#     need quotes, and the default privileges of pgc_pg19_grants_owner;
#   - the rv_pgrants_* subscriptions' publication lists;
#   - the rv_pgrants_* publications' members, column lists, row filters and
#     EXCEPT entries, and the columns, persistence and partition status of
#     their tables.
# It also checks that the round-1 SQL spelled those roles and publication
# names quoted, skipped the refresh of a disabled subscription, and released
# the publication rows ahead of the table changes they block.
# Step B (below) runs its own USE_DROP=false migration.
# Env: PGPASSWORD=postgres (PGPORT optional, default 5432);
# OUTPUT_SQL (default /tmp/output.sql); PGC and PGRANTS_WORK for step B.
set -euo pipefail
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
ver=$($PSQL -d a -c "show server_version_num")
schemas="('test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public')"
fail=0

compare() {  # compare <label> <sql>: the same rows in a and b
  local in_a in_b
  in_a=$($PSQL -d a -c "$2"); in_b=$($PSQL -d b -c "$2")
  if [ "$in_a" != "$in_b" ]; then
    echo "::error::pre-grants: $1 differ between a and b after the migration (server_version_num $ver)"
    diff <(echo "$in_a") <(echo "$in_b") || true
    fail=1
  fi
}

compare_common() {  # compare_common <label> <sql giving key|value>: values of the keys both have
  local mismatches
  mismatches=$(LC_ALL=C join -t '|' <($PSQL -d a -c "$2" | LC_ALL=C sort -t '|' -k1,1) <($PSQL -d b -c "$2" | LC_ALL=C sort -t '|' -k1,1) \
    | awk -F'|' '$2 != $3')
  if [ -n "$mismatches" ]; then
    echo "::error::pre-grants: $1 differ between a and b after the migration (server_version_num $ver)"
    echo "key|a|b"
    echo "$mismatches"
    fail=1
  fi
}

# PUBLIC's default privilege on every routine and type, NULL read as acldefault.
compare_common "PUBLIC EXECUTE on routines" "
  select format('%I.%I(%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid))
         || '|' || exists (select 1 from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
                           where x.grantee = 0 and x.privilege_type = 'EXECUTE')
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where n.nspname in $schemas
    and not exists (select 1 from pg_depend d where d.classid = 'pg_proc'::regclass
                    and d.objid = p.oid and d.deptype = 'e')"
compare_common "PUBLIC USAGE on types" "
  select format('%I.%I', n.nspname, t.typname)
         || '|' || exists (select 1 from aclexplode(coalesce(t.typacl, acldefault('T', t.typowner))) x
                           where x.grantee = 0 and x.privilege_type = 'USAGE')
  from pg_type t join pg_namespace n on n.oid = t.typnamespace
  left join pg_class c on c.oid = t.typrelid
  where n.nspname in $schemas
    and (t.typtype in ('d', 'e', 'r') or (t.typtype = 'c' and c.relkind = 'c'))
    and not exists (select 1 from pg_depend d where d.classid = 'pg_type'::regclass
                    and d.objid = t.oid and d.deptype = 'e')"

# Privileges of the rv_pgrants_* objects, grantee by name, merged over grantors.
# The owner's own entry is left out, as pgc leaves it out of the comparison.
acl_rows() {  # acl_rows <label> <aclitem[] expression> <catalog> <condition> <owner oid>
  echo "select '$1|' || coalesce((select string_agg(r, ',' order by r) from (select format('%s:%s:%s', g, x.privilege_type, bool_or(x.is_grantable)) as r from $3, aclexplode($2) x, coalesce(nullif(pg_get_userbyid(x.grantee), 'unknown (OID=0)'), 'PUBLIC') g where ($4) and x.grantee <> $5 group by g, x.privilege_type) s), '')"
}
rel() { acl_rows "$1" "coalesce(c.relacl, acldefault('r', c.relowner))" "pg_class c" "c.oid = 'public.$1'::regclass" c.relowner; }
fn() { acl_rows "$1" "coalesce(p.proacl, acldefault('f', p.proowner))" "pg_proc p" "p.oid = 'public.$1()'::regprocedure" p.proowner; }
typ() { acl_rows "$1" "coalesce(t.typacl, acldefault('T', t.typowner))" "pg_type t" "t.oid = 'public.$1'::regtype" t.typowner; }
compare "rv_pgrants privileges" "$(rel rv_pgrants_tbl)
union all $(acl_rows rv_pgrants_tbl.note "a.attacl" "pg_attribute a" "a.attrelid = 'public.rv_pgrants_tbl'::regclass and a.attname = 'note'" 0)
union all $(rel rv_pgrants_tbl_revoke)
union all $(rel rv_pgrants_chain)
union all $(fn rv_pgrants_secret)
union all $(fn rv_pgrants_granted)
union all $(fn rv_pgrants_reopen)
union all $(fn rv_pgrants_kept)
union all $(fn rv_pgrants_emptied)
union all $(fn rv_pgrants_new_fn)
union all $(typ rv_pgrants_dom)
union all $(typ rv_pgrants_enum)
union all $(typ rv_pgrants_new_dom)
union all $(acl_rows rv_pgrants_app_fr "coalesce(p.proacl, acldefault('f', p.proowner))" "pg_proc p" "p.oid = 'test_order.rv_pgrants_app_fr()'::regprocedure" p.proowner)
union all $(acl_rows rv_pgrants_app_new "coalesce(p.proacl, acldefault('f', p.proowner))" "pg_proc p" "p.oid = 'test_order.rv_pgrants_app_new()'::regprocedure" p.proowner)
union all $(typ rv_pgrants_app_dom)
union all $(typ rv_pgrants_range)"
compare "default privileges of pgc_pg19_grants_owner and pgc_pg19_pgrants_app" "
  select coalesce(n.nspname, '') || '|' || d.defaclobjtype::text || '|' ||
         (select string_agg(format('%s:%s', coalesce(nullif(pg_get_userbyid(x.grantee), 'unknown (OID=0)'), 'PUBLIC'), x.privilege_type), ',' order by 1)
          from aclexplode(d.defaclacl) x)
  from pg_default_acl d left join pg_namespace n on n.oid = d.defaclnamespace
  where pg_get_userbyid(d.defaclrole) in ('pgc_pg19_grants_owner', 'pgc_pg19_pgrants_app') order by 1"

# Subscriptions of this database (pg_subscription is shared).
compare "rv_pgrants subscriptions" "
  select subname || '|' || subenabled || '|' || subpublications::text from pg_subscription
  where subdbid = (select oid from pg_database where datname = current_database())
    and subname like 'rv\_pgrants\_%' order by 1"

# Publication rows and the tables they are on.
if [ "$ver" -ge 150000 ]; then
  spec="coalesce((select string_agg(quote_ident(a.attname), ',' order by a.attname) from pg_attribute a
                  where a.attrelid = pr.prrelid and a.attnum = any (pr.prattrs)), '*')
        || ':' || coalesce(pg_get_expr(pr.prqual, pr.prrelid), '-')"
else
  spec="'*:-'"
fi
if [ "$ver" -ge 190000 ]; then
  except="case when pr.prexcept then ':except' else '' end"
else
  except="''"
fi
compare "rv_pgrants publication rows" "
  select p.pubname || '|' || p.puballtables || '|' || coalesce(string_agg(
           pr.prrelid::regclass::text || ':' || $spec || $except, ';' order by pr.prrelid::regclass::text), '')
  from pg_publication p left join pg_publication_rel pr on pr.prpubid = p.oid
  where p.pubname like 'rv\_pgrants\_pub\_%' group by p.pubname, p.puballtables order by 1"
compare "rv_pgrants publication tables" "
  select c.relname || '|' || c.relpersistence::text || '|' || c.relispartition || '|' ||
         string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ',' order by a.attnum)
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
  where n.nspname = 'test_schema' and c.relname like 'rv\_pgrants\_pub%'
  group by c.relname, c.relpersistence, c.relispartition order by 1"

# Spot checks: the fixture values really arrived.
expect() {  # expect <label> <sql> <expected>
  local got
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::pre-grants: $1: expected '$3', got '$got'"; fail=1; fi
}
public_exec="select exists (select 1 from pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x where p.oid = 'public.%s()'::regprocedure and x.grantee = 0)"
# shellcheck disable=SC2059
expect "PUBLIC EXECUTE on rv_pgrants_secret()" "$(printf "$public_exec" rv_pgrants_secret)" "f"
# shellcheck disable=SC2059
expect "PUBLIC EXECUTE on rv_pgrants_reopen()" "$(printf "$public_exec" rv_pgrants_reopen)" "t"
# shellcheck disable=SC2059
expect "PUBLIC EXECUTE on rv_pgrants_kept()" "$(printf "$public_exec" rv_pgrants_kept)" "t"
expect "rv_pgrants_sub_quoted publications" \
  "select subpublications::text from pg_subscription where subname = 'rv_pgrants_sub_quoted' and subdbid = (select oid from pg_database where datname = current_database())" \
  '{RvPgrants_Remote,"rv pgrants remote",select}'
expect "rv_pgrants_publog persistence" \
  "select relpersistence from pg_class where oid = 'test_schema.rv_pgrants_publog'::regclass" "u"
if [ "$ver" -ge 150000 ]; then
  expect "rv_pgrants_pubfilter.qty type" \
    "select format_type(atttypid, atttypmod) from pg_attribute where attrelid = 'test_schema.rv_pgrants_pubfilter'::regclass and attname = 'qty'" "bigint"
  expect "rv_pgrants_pubtbl.qty dropped" \
    "select count(*) from pg_attribute where attrelid = 'test_schema.rv_pgrants_pubtbl'::regclass and attname = 'qty' and not attisdropped" "0"
fi
if [ "$ver" -ge 190000 ]; then
  expect "rv_pgrants_pubexc_part attached" \
    "select relispartition from pg_class where oid = 'test_schema.rv_pgrants_pubexc_part'::regclass" "t"
fi

# The round-1 SQL. Leading whitespace is dropped so production output matches too.
stmts=$(sed 's/^[[:space:]]*//' "$OUT")
has() { grep -qxF "$1" <<<"$stmts" || { echo "::error::pre-grants: round-1 SQL lacks: $1"; fail=1; }; }
lacks() { if grep -qxF "$1" <<<"$stmts"; then echo "::error::pre-grants: round-1 SQL has: $1"; fail=1; fi; }
before() {  # before <first> <second>: both present, first earlier
  local a b
  a=$(grep -nxF "$1" <<<"$stmts" | head -1 | cut -d: -f1 || true)
  b=$(grep -nxF "$2" <<<"$stmts" | head -1 | cut -d: -f1 || true)
  if [ -z "$a" ] || [ -z "$b" ] || [ "$a" -ge "$b" ]; then
    echo "::error::pre-grants: round-1 SQL must have '$1' (line ${a:-none}) before '$2' (line ${b:-none})"
    fail=1
  fi
}
has 'GRANT SELECT ON TABLE public.rv_pgrants_tbl TO "pgc_pg19_Pgrants_Reader";'
has 'GRANT INSERT ON TABLE public.rv_pgrants_tbl TO "pgc_pg19_pgrants reader";'
has 'GRANT UPDATE (note) ON TABLE public.rv_pgrants_tbl TO "pgc_pg19_Pgrants_Reader";'
has 'REVOKE SELECT ON TABLE public.rv_pgrants_tbl_revoke FROM "pgc_pg19_Pgrants_Reader";'
has 'REVOKE GRANT OPTION FOR SELECT ON TABLE public.rv_pgrants_chain FROM "pgc_pg19_Pgrants_Deleg";'
has 'REVOKE SELECT ON TABLE public.rv_pgrants_chain FROM "pgc_pg19_Pgrants_Deleg";'
has 'ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_grants_owner IN SCHEMA public GRANT SELECT ON TABLES TO "pgc_pg19_Pgrants_Reader";'
has 'REVOKE EXECUTE ON FUNCTION public.rv_pgrants_secret() FROM PUBLIC;'
has 'REVOKE EXECUTE ON FUNCTION public.rv_pgrants_new_fn() FROM PUBLIC;'
has 'GRANT EXECUTE ON FUNCTION public.rv_pgrants_reopen() TO PUBLIC;'
has 'GRANT EXECUTE ON FUNCTION public.rv_pgrants_emptied() TO PUBLIC;'
has 'REVOKE USAGE ON TYPE public.rv_pgrants_dom FROM PUBLIC;'
has 'REVOKE USAGE ON TYPE public.rv_pgrants_new_dom FROM PUBLIC;'
lacks 'REVOKE EXECUTE ON FUNCTION public.rv_pgrants_kept() FROM PUBLIC;'
# Created objects: the FROM default privileges of pgc_pg19_pgrants_app do not
# apply to what the migration's (superuser) session creates.
has 'REVOKE EXECUTE ON FUNCTION test_order.rv_pgrants_app_fr() FROM PUBLIC;'
has 'GRANT EXECUTE ON FUNCTION test_order.rv_pgrants_app_fr() TO "pgc_pg19_Pgrants_Reader";'
has 'REVOKE EXECUTE ON FUNCTION test_order.rv_pgrants_app_new() FROM PUBLIC;'
has 'GRANT EXECUTE ON FUNCTION test_order.rv_pgrants_app_new() TO "pgc_pg19_Pgrants_Reader";'
has 'REVOKE USAGE ON TYPE public.rv_pgrants_app_dom FROM PUBLIC;'
has 'REVOKE USAGE ON TYPE public.rv_pgrants_range FROM PUBLIC;'
has 'GRANT USAGE ON TYPE public.rv_pgrants_range TO "pgc_pg19_Pgrants_Reader";'
has 'ALTER SUBSCRIPTION rv_pgrants_sub_quoted SET PUBLICATION "RvPgrants_Remote", "rv pgrants remote", "select" WITH (refresh = false);'
has 'ALTER SUBSCRIPTION rv_pgrants_sub_setpub SET PUBLICATION rv_pgrants_remote_p1, rv_pgrants_remote_p2 WITH (refresh = false);'
before 'ALTER PUBLICATION rv_pgrants_pub_unlogged DROP TABLE ONLY test_schema.rv_pgrants_publog;' \
       'alter table test_schema.rv_pgrants_publog set unlogged;'
if [ "$ver" -ge 150000 ]; then
  before 'ALTER PUBLICATION rv_pgrants_pub_filter DROP TABLE ONLY test_schema.rv_pgrants_pubfilter;' \
         'alter table test_schema.rv_pgrants_pubfilter alter column qty type bigint;'
  before 'ALTER PUBLICATION rv_pgrants_pub_collist DROP TABLE ONLY test_schema.rv_pgrants_pubtbl;' \
         'alter table test_schema.rv_pgrants_pubtbl alter column code type character varying(20);'
  before 'alter table test_schema.rv_pgrants_pubfilter alter column qty type bigint;' \
         'ALTER PUBLICATION rv_pgrants_pub_filter SET TABLE test_schema.rv_pgrants_pubfilter WHERE ((qty > 0));'
fi
if [ "$ver" -ge 190000 ]; then
  before 'ALTER PUBLICATION rv_pgrants_pub_except SET ALL TABLES;' \
         'alter table test_schema.rv_pgrants_pubexc_log set unlogged;'
fi
[ "$fail" -eq 0 ] && echo "OK: pre-grants catalog state of a matches b on server_version_num ${ver}"

# ============================================================================
# Step B - publication rows released for a table change, in publications the
# migration keeps as they are (USE_DROP=false): a FROM-only publication, and
# on PG15-18 one whose FOR TABLE -> FOR ALL TABLES recreate is commented out.
# Nothing else puts the rows back, so the migration adds them again after the
# tables, with the FROM column list / row filter; a member made UNLOGGED
# cannot come back and gets a warning. Own databases pgc_pgrants_a / _b.
# Env: PGC (default ./app/target/release/pgc), PGRANTS_WORK (default
# /tmp/pgc_pgrants_<port>).
# ============================================================================
port=${PGPORT:-5432}
pgc=${PGC:-./app/target/release/pgc}
work=${PGRANTS_WORK:-/tmp/pgc_pgrants_${port}}
mkdir -p "$work"
P="psql -h 127.0.0.1 -p $port -U postgres -X -q -v ON_ERROR_STOP=1"
for db in pgc_pgrants_a pgc_pgrants_b; do
  $P -d postgres -c "drop database if exists ${db} with (force)" -c "create database ${db}"
done
$P -d pgc_pgrants_a -c "create table public.t (id integer primary key, qty integer)" \
  -c "create table public.l (id integer primary key)" \
  -c "create publication rv_pgrants_keep_log for table public.l"
$P -d pgc_pgrants_b -c "create table public.t (id integer primary key, qty bigint)" \
  -c "create unlogged table public.l (id integer primary key)"
if [ "$ver" -ge 150000 ]; then
  $P -d pgc_pgrants_a -c "create publication rv_pgrants_keep for table public.t where (qty > 0)" \
    -c "create publication rv_pgrants_rec for table public.t (id, qty)"
  $P -d pgc_pgrants_b -c "create publication rv_pgrants_rec for all tables"
fi
cat > "$work/pgc.conf" <<CONF
FROM_HOST=127.0.0.1
FROM_PORT=${port}
FROM_USER=postgres
FROM_PASSWORD=postgres
FROM_DATABASE=pgc_pgrants_a
FROM_SCHEME=public
FROM_SSL=false
FROM_DUMP=${work}/a.dump
TO_HOST=127.0.0.1
TO_PORT=${port}
TO_USER=postgres
TO_PASSWORD=postgres
TO_DATABASE=pgc_pgrants_b
TO_SCHEME=public
TO_SSL=false
TO_DUMP=${work}/b.dump
OUTPUT=${work}/output.sql
USE_DROP=false
USE_COMMENTS=false
GRANTS_MODE=ignore
USE_SINGLE_TRANSACTION=false
CONF
"$pgc" --config "$work/pgc.conf" > "$work/round1.log" 2>&1 || { cat "$work/round1.log"; exit 1; }
cp "$work/output.sql" "$work/round1.sql"
$P -d pgc_pgrants_a -f "$work/round1.sql" || { echo "::error::pre-grants: the USE_DROP=false migration does not apply"; cat "$work/round1.sql"; fail=1; }
rows() {  # rows <publication>: its rows with column list and row filter
  if [ "$ver" -ge 150000 ]; then
    $P -d pgc_pgrants_a -tAc "select coalesce(string_agg(pr.prrelid::regclass::text || ':' ||
        coalesce((select string_agg(a.attname, ',' order by a.attname) from pg_attribute a
                  where a.attrelid = pr.prrelid and a.attnum = any (pr.prattrs)), '*') || ':' ||
        coalesce(pg_get_expr(pr.prqual, pr.prrelid), '-'), ';'), '')
      from pg_publication p left join pg_publication_rel pr on pr.prpubid = p.oid where p.pubname = '$1'"
  else
    $P -d pgc_pgrants_a -tAc "select coalesce(string_agg(pr.prrelid::regclass::text, ';'), '')
      from pg_publication p left join pg_publication_rel pr on pr.prpubid = p.oid where p.pubname = '$1'"
  fi
}
check() { [ "$2" = "$3" ] || { echo "::error::pre-grants (USE_DROP=false): $1: expected '$3', got '$2'"; fail=1; }; }
check "t.qty type" "$($P -d pgc_pgrants_a -tAc "select format_type(atttypid, atttypmod) from pg_attribute where attrelid = 'public.t'::regclass and attname = 'qty'")" "bigint"
check "l persistence" "$($P -d pgc_pgrants_a -tAc "select relpersistence from pg_class where oid = 'public.l'::regclass")" "u"
check "rv_pgrants_keep_log rows (UNLOGGED member cannot come back)" "$(rows rv_pgrants_keep_log)" ""
grep -qF 'Warning: publication rv_pgrants_keep_log: member public.l was dropped because the migration makes it UNLOGGED' "$work/round1.log" \
  || { echo "::error::pre-grants (USE_DROP=false): no warning about rv_pgrants_keep_log's UNLOGGED member"; cat "$work/round1.log"; fail=1; }
if [ "$ver" -ge 150000 ]; then
  check "rv_pgrants_keep rows" "$(rows rv_pgrants_keep)" "t:*:(qty > 0)"
  if [ "$ver" -ge 190000 ]; then
    # PostgreSQL 19 alters FOR TABLE into FOR ALL TABLES in place.
    check "rv_pgrants_rec kind" "$($P -d pgc_pgrants_a -tAc "select puballtables from pg_publication where pubname = 'rv_pgrants_rec'")" "t"
  else
    check "rv_pgrants_rec rows" "$(rows rv_pgrants_rec)" "t:id,qty:-"
  fi
fi
"$pgc" --config "$work/pgc.conf" > "$work/round2.log" 2>&1 || { cat "$work/round2.log"; exit 1; }
if [ -n "$(tr -d '[:space:]' < "$work/output.sql")" ]; then
  echo "::error::pre-grants (USE_DROP=false): second compare not empty:"; cat "$work/output.sql"; fail=1
fi
for db in pgc_pgrants_a pgc_pgrants_b; do
  $P -d postgres -c "drop database if exists ${db} with (force)"
done
[ "$fail" -eq 0 ] || exit 1
echo "OK: released publication rows come back in publications a USE_DROP=false migration keeps (PG ${ver})"
