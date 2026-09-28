#!/usr/bin/env bash
# reg-misc: after "Apply diff to database `a`" (round 1), before round 2.
# Round 2 cannot see a property both dumps lose, so compare a with b directly
# for the three things the table DDL of a migration takes away:
#   - the pg_publication_rel row of a partition retyped through its parent
#     (never dropped, so it must be dropped from a publication explicitly);
#   - CHECK enforcement around a rewrite of the column the CHECK reads
#     (PG19 in place: relax before the rewrite, tighten after it), including
#     a partition's own CHECK under its parent's recursing ALTER COLUMN TYPE
#     (relaxed or dropped ahead of every table alter);
#   - column/table privileges of a re-added column, a recreated table, and a
#     partition retyped through its parent.
# Set TEXT_CHECKS=0 to skip the checks on the round-1 SQL text (e.g. for an
# OUTPUT_FOR_PRODUCTION=true run, whose drops say IF EXISTS).
# Failures are collected in $fail, so neither -e nor pipefail (the defaults of
# a GitHub Actions bash step) may cut the report short.
set -u +e +o pipefail
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
TEXT_CHECKS="${TEXT_CHECKS:-1}"
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
ver=$($PSQL -d a -c "show server_version_num") || { echo "::error::reg-misc: cannot connect"; exit 1; }
fail=0
same() { # <label> <sql>: the query returns the same rows in a and b
  local ra rb
  ra=$($PSQL -d a -c "$2") || { echo "::error::reg-misc: $1 query failed on a"; fail=1; return; }
  rb=$($PSQL -d b -c "$2") || { echo "::error::reg-misc: $1 query failed on b"; fail=1; return; }
  if [ "$ra" != "$rb" ]; then
    echo "::error::reg-misc: $1 differ between a and b (server_version_num $ver)"
    diff <(printf '%s\n' "$ra") <(printf '%s\n' "$rb") || true
    fail=1
  fi
}
expect() { # <label> <sql> <expected> (on a)
  local got
  got=$($PSQL -d a -c "$2" 2>&1) || true
  [ "$got" = "$3" ] || { echo "::error::reg-misc: $1: expected '$3', got '$got'"; fail=1; }
}
line_of() { grep -nxF "$1" "$OUT" | head -1 | cut -d: -f1 || true; }
before() { # <label> <first statement> <second statement>
  local a b
  a=$(line_of "$2"); b=$(line_of "$3")
  if [ -z "$a" ] || [ -z "$b" ] || [ "$a" -ge "$b" ]; then
    echo "::error::reg-misc: $1: expected '$2' (line ${a:-none}) before '$3' (line ${b:-none})"; fail=1
  fi
}

# 1. Publications: one row per pg_publication_rel entry (prexcept on 19+).
except_col="''"; [ "$ver" -ge 190000 ] && except_col="pr.prexcept::text"
same "rv_regmisc publications" "
  select coalesce(string_agg(format('%s|%s', p.pubname, p.puballtables), E'\n' order by p.pubname), '')
  from pg_publication p where p.pubname like 'rv\_regmisc\_%'"
same "rv_regmisc publication members" "
  select coalesce(string_agg(format('%s|%s|%s', p.pubname, pr.prrelid::regclass, $except_col),
                             E'\n' order by p.pubname, pr.prrelid::regclass::text), '')
  from pg_publication_rel pr join pg_publication p on p.oid = pr.prpubid
  where p.pubname like 'rv\_regmisc\_%'"
expect "rv_regmisc_pubtbl_pt1 left in an emptied publication" "
  select count(*) from pg_publication_rel pr join pg_publication p on p.oid = pr.prpubid
  where p.pubname = 'rv_regmisc_pub_list_to_empty'" "0"

# 2. CHECK enforcement around a rewrite of the column it reads.
enforced="true"; [ "$ver" -ge 180000 ] && enforced="c.conenforced"
same "rv_regmisc CHECK constraints" "
  select coalesce(string_agg(format('%s|%s|enforced=%s|validated=%s|%s', c.conrelid::regclass, c.conname,
                                    $enforced, c.convalidated, pg_get_constraintdef(c.oid)), E'\n' order by 1), '')
  from pg_constraint c join pg_class t on t.oid = c.conrelid join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'test_schema' and t.relname like 'rv\_regmisc\_ck%' and c.contype = 'c'"
same "rv_regmisc column types and expressions" "
  select coalesce(string_agg(format('%s|%s|%s|%s|%s', a.attrelid::regclass, a.attname,
                                    format_type(a.atttypid, a.atttypmod), a.attgenerated,
                                    pg_get_expr(d.adbin, d.adrelid)), E'\n' order by 1), '')
  from pg_attribute a join pg_class t on t.oid = a.attrelid join pg_namespace n on n.oid = t.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where n.nspname = 'test_schema' and t.relname like 'rv\_regmisc\_ck%' and t.relkind in ('r', 'p')
    and a.attnum > 0 and not a.attisdropped"
# The rows were rewritten (b holds none, so compare with the expected values).
expect "rv_regmisc_ck_expr.g" "select string_agg(g::text, ',') from test_schema.rv_regmisc_ck_expr" "-6"
expect "rv_regmisc_ck_type.c" "select string_agg(c::text, ',') from test_schema.rv_regmisc_ck_type" "1"
expect "rv_regmisc_ck_tight_expr.g" "select string_agg(g::text, ',') from test_schema.rv_regmisc_ck_tight_expr" "-2"
expect "rv_regmisc_ck_tight_type.c" "select string_agg(c::text, ',') from test_schema.rv_regmisc_ck_tight_type" "1"
[ "$ver" -ge 180000 ] && expect "rv_regmisc_ck_virt.g" "select string_agg(g::text, ',') from test_schema.rv_regmisc_ck_virt" "-6"
expect "rv_regmisc_ckfam_p.c" "select string_agg(c::text, ',' order by id) from test_schema.rv_regmisc_ckfam_p" "1,1"
if [ "$TEXT_CHECKS" = 1 ]; then
  # The partition's CHECK lets go before the parent's recursing rewrite; the
  # tightened one is validated after it.
  if [ "$ver" -ge 190000 ]; then
    fam_first="alter table test_schema.rv_regmisc_ckfam_p1 alter constraint rv_regmisc_ckfam_p1_c not enforced;"
    fam_last="alter table test_schema.rv_regmisc_ckfam_p2 alter constraint rv_regmisc_ckfam_p2_c enforced;"
  else
    fam_first="alter table test_schema.rv_regmisc_ckfam_p1 drop constraint rv_regmisc_ckfam_p1_c;"
    fam_last="alter table test_schema.rv_regmisc_ckfam_p2 add constraint rv_regmisc_ckfam_p2_c check (c <> 0.6) ;"
  fi
  fam_type="alter table test_schema.rv_regmisc_ckfam_p alter column c type numeric(6, 0);"
  before "rv_regmisc_ckfam_p1 let go before the parent's ALTER TYPE" "$fam_first" "$fam_type"
  before "rv_regmisc_ckfam_p2 validated after the parent's ALTER TYPE" "$fam_type" "$fam_last"
fi
if [ "$TEXT_CHECKS" = 1 ] && [ "$ver" -ge 190000 ]; then
  for t in expr virt; do
    before "rv_regmisc_ck_$t relaxed before SET EXPRESSION" \
      "alter table test_schema.rv_regmisc_ck_$t alter constraint rv_regmisc_ck_${t}_g not enforced;" \
      "alter table only test_schema.rv_regmisc_ck_$t alter column g set expression as (c * 3);"
  done
  before "rv_regmisc_ck_type relaxed before ALTER TYPE" \
    "alter table test_schema.rv_regmisc_ck_type alter constraint rv_regmisc_ck_type_c not enforced;" \
    "alter table test_schema.rv_regmisc_ck_type alter column c type numeric(6, 0);"
  before "rv_regmisc_ck_tight_expr tightened after SET EXPRESSION" \
    "alter table only test_schema.rv_regmisc_ck_tight_expr alter column g set expression as (c + 1);" \
    "alter table test_schema.rv_regmisc_ck_tight_expr alter constraint rv_regmisc_ck_tight_expr_g enforced;"
  before "rv_regmisc_ck_tight_type tightened after ALTER TYPE" \
    "alter table test_schema.rv_regmisc_ck_tight_type alter column c type numeric(6, 0);" \
    "alter table test_schema.rv_regmisc_ck_tight_type alter constraint rv_regmisc_ck_tight_type_c enforced;"
fi

# 3. Privileges, grantor-insensitive (as pgc compares them), plus no entry a
# delegate granted where b dropped it.
acl_rows() { # <label> <aclitem[] expression> <from> <condition>
  echo "select '$1|' || coalesce((select string_agg(r, ',' order by r) from (select format('%s:%s:%s', g, x.privilege_type, bool_or(x.is_grantable)) as r from $3, aclexplode($2) x, coalesce(nullif(pg_get_userbyid(x.grantee), 'unknown (OID=0)'), 'PUBLIC') g where $4 group by g, x.privilege_type) s), '')"
}
rel() { acl_rows "$1" "coalesce(c.relacl, acldefault('r', c.relowner))" "pg_class c" "c.oid = 'public.$1'::regclass"; }
col() { acl_rows "$1.$2" "a.attacl" "pg_attribute a" "a.attrelid = 'public.$1'::regclass and a.attname = '$2' and not a.attisdropped"; }
same "rv_regmisc privileges" "$(rel rv_regmisc_grants_readd)
  union all $(col rv_regmisc_grants_readd g)
  union all $(col rv_regmisc_grants_readd note)
  union all $(rel rv_regmisc_grants_rebuilt)
  union all $(col rv_regmisc_grants_rebuilt code)
  union all $(rel rv_regmisc_grants_pt1)"
rv_tables="('public.rv_regmisc_grants_readd'::regclass, 'public.rv_regmisc_grants_rebuilt'::regclass, 'public.rv_regmisc_grants_pt1'::regclass)"
delegated="pg_get_userbyid(x.grantor) = 'pgc_pg19_grants_delegate'"
expect "rv_regmisc delegated table entries left" "select count(*) from pg_class c, aclexplode(c.relacl) x where c.oid in $rv_tables and $delegated" "0"
expect "rv_regmisc delegated column entries left" "select count(*) from pg_attribute a, aclexplode(a.attacl) x where a.attrelid in $rv_tables and not a.attisdropped and $delegated" "0"
if [ "$TEXT_CHECKS" = 1 ]; then
  # Nothing is revoked from a column the DDL re-created: its entries are gone.
  if grep -E 'REVOKE .* ON TABLE public\.rv_regmisc_grants_(readd|rebuilt) ' "$OUT"; then
    echo "::error::reg-misc: round-1 SQL revokes privileges the table DDL already took away"; fail=1
  fi
fi

[ "$fail" -eq 0 ] && echo "OK: reg-misc catalogs of a match b (server_version_num $ver)"
exit "$fail"
