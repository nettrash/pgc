#!/usr/bin/env bash
# ============================================================================
# r3:types - "Assert composite types, sequence types and multirange schemas" -
# run after "Apply diff to database `a`" (env PGPASSWORD=postgres, PGPORT set).
# Works on PG14-19 (multiranges exist from 14 on).
# ============================================================================
# After round 1 is applied, `a` must match `b` in:
#   - the attributes (name, type, order) of every composite type, including
#     shared_schema.address_type / test_schema.user_profile, whose added
#     attributes were only ever a stripped "not implemented" comment;
#   - the columns of the typed table test_schema.rv_r3t_typed, which its
#     type's ALTER TYPE ... CASCADE changes;
#   - the type, bounds and owner of every sequence;
#   - the owners of the rv_r3t_own_* tables and their serial / identity
#     sequences, with no direct ALTER SEQUENCE ... OWNER TO for them (which
#     PostgreSQL refuses; CI's ON_ERROR_STOP=0 apply would hide it);
#   - the schema of every range type's multirange;
#   - the rows of the typed table rv_r3t_typed (a NOT NULL DEFAULT column its
#     type adds is filled with the default first), the column types of the
#     typed table rv_r3t_tv under its view rv_r3t_tv_v (retyped after the
#     view is dropped), and which column every sequence is OWNED BY
#     (rv_r3t_mv_seq moves to another table while its owner changes).
# Round 2 cannot see these: an empty "not implemented" comment made the
# composite case look converged, and a multirange is never compared.
set -u
fail=0
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
SCHEMAS="'test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public'"
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

same "composite type attributes (name, type, order)" \
  "select n.nspname || '.' || t.typname,
          string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ', '
                     order by a.attnum)
     from pg_type t
     join pg_namespace n on n.oid = t.typnamespace
     join pg_class c on c.oid = t.typrelid and c.relkind = 'c'
     left join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
    where n.nspname in (${SCHEMAS})
    group by 1 order by 1"
# Not vacuous: the fixture's added attributes exist in b, in b's order.
expect_in_b "address_type ends in region" \
  "select string_agg(attname, ',' order by attnum) from pg_attribute
    where attrelid = 'shared_schema.address_type'::regclass and attnum > 0 and not attisdropped" \
  "street,city,postal_code,country,region"
expect_in_b "user_profile ends in phone" \
  "select string_agg(attname, ',' order by attnum) from pg_attribute
    where attrelid = 'test_schema.user_profile'::regclass and attnum > 0 and not attisdropped" \
  "first_name,last_name,email,birth_date,phone"
expect_in_b "rv_r3t_comp order" \
  "select string_agg(attname || ':' || format_type(atttypid, atttypmod), ',' order by attnum)
     from pg_attribute
    where attrelid = 'test_schema.rv_r3t_comp'::regclass and attnum > 0 and not attisdropped" \
  "a:bigint,bb:integer,c:text,added:text"

# ADD ATTRIBUTE keeps the stored composite values.
same "rv_r3t_used_tbl values" \
  "select id, v::text from test_schema.rv_r3t_used_tbl order by id"

same "typed table rv_r3t_typed columns (type, NOT NULL, default)" \
  "select a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
          coalesce(pg_get_expr(ad.adbin, ad.adrelid), '-')
     from pg_attribute a
     left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
    where a.attrelid = 'test_schema.rv_r3t_typed'::regclass and a.attnum > 0
      and not a.attisdropped
    order by a.attnum"
same "typed table rv_r3t_typed type" \
  "select reloftype::regtype::text from pg_class where oid = 'test_schema.rv_r3t_typed'::regclass"

same "typed table rv_r3t_typed rows (extra filled with its default)" \
  "select id, keep, extra from test_schema.rv_r3t_typed order by id"
expect_in_b "rv_r3t_typed row" \
  "select string_agg(id || ':' || keep || ':' || extra, ',') from test_schema.rv_r3t_typed" \
  "1:kept:7"
same "typed table rv_r3t_tv columns under view rv_r3t_tv_v" \
  "select c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
     from pg_attribute a
     join pg_class c on c.oid = a.attrelid
    where c.relnamespace = 'test_schema'::regnamespace
      and c.relname in ('rv_r3t_tv', 'rv_r3t_tv_v') and a.attnum > 0 and not a.attisdropped
    order by 1, a.attnum"
expect_in_b "rv_r3t_tv.id is bigint" \
  "select format_type(atttypid, atttypmod) from pg_attribute
    where attrelid = 'test_schema.rv_r3t_tv'::regclass and attname = 'id'" \
  "bigint"

same "sequences (type, bounds, increment, cache, cycle, owner)" \
  "select n.nspname || '.' || c.relname, format_type(s.seqtypid, null), s.seqmin, s.seqmax,
          s.seqincrement, s.seqcache, s.seqcycle, pg_get_userbyid(c.relowner)
     from pg_sequence s
     join pg_class c on c.oid = s.seqrelid
     join pg_namespace n on n.oid = c.relnamespace
    where n.nspname in (${SCHEMAS})
    order by 1"
same "rv_r3t_ser sequence position" \
  "select last_value, is_called from test_schema.rv_r3t_ser_id_seq"
expect_in_b "rv_r3t sequence types" \
  "select string_agg(c.relname || ':' || format_type(s.seqtypid, null), ',' order by c.relname)
     from pg_sequence s join pg_class c on c.oid = s.seqrelid
    where c.relnamespace = 'test_schema'::regnamespace
      and c.relname in ('rv_r3t_ser_id_seq', 'rv_r3t_bigser_id_seq', 'rv_r3t_seq', 'rv_r3t_ident_id_seq')" \
  "rv_r3t_bigser_id_seq:integer,rv_r3t_ident_id_seq:bigint,rv_r3t_seq:bigint,rv_r3t_ser_id_seq:bigint"

same "sequences OWNED BY (table.column)" \
  "select n.nspname || '.' || c.relname,
          coalesce(d.refobjid::regclass::text || '.' || a.attname, '-')
     from pg_class c
     join pg_namespace n on n.oid = c.relnamespace
     left join pg_depend d on d.classid = 'pg_class'::regclass and d.objid = c.oid
          and d.refclassid = 'pg_class'::regclass and d.deptype in ('a', 'i')
     left join pg_attribute a on a.attrelid = d.refobjid and a.attnum = d.refobjsubid
    where c.relkind = 'S' and n.nspname in (${SCHEMAS})
    order by 1"
expect_in_b "rv_r3t_mv_seq owned by rv_r3t_mv_b.id, owner pgc_pg19_r3t_owner" \
  "select d.refobjid::regclass::text || '.' || a.attname || ':' || pg_get_userbyid(c.relowner)
     from pg_class c
     join pg_depend d on d.classid = 'pg_class'::regclass and d.objid = c.oid
          and d.refclassid = 'pg_class'::regclass and d.deptype = 'a'
     join pg_attribute a on a.attrelid = d.refobjid and a.attnum = d.refobjsubid
    where c.oid = 'test_schema.rv_r3t_mv_seq'::regclass" \
  "test_schema.rv_r3t_mv_b.id:pgc_pg19_r3t_owner"

same "rv_r3t_own_* table and sequence owners" \
  "select c.relname, pg_get_userbyid(c.relowner)
     from pg_class c
    where c.relnamespace = 'test_schema'::regnamespace and c.relname like 'rv\_r3t\_own\_%'
    order by 1"
expect_in_b "rv_r3t_own_* owned by pgc_pg19_r3t_owner" \
  "select count(*) from pg_class
    where relnamespace = 'test_schema'::regnamespace and relname like 'rv\_r3t\_own\_%'
      and relkind in ('r', 'S') and pg_get_userbyid(relowner) = 'pgc_pg19_r3t_owner'" \
  "4"
if grep -Eiq '^[[:space:]]*alter sequence (if exists )?test_schema\.rv_r3t_own_(ser|ident)_id_seq owner to' "$OUT"; then
  echo "ERROR: round 1 changes the owner of a sequence linked to its table directly (PostgreSQL refuses it):"
  grep -Ei 'alter sequence (if exists )?test_schema\.rv_r3t_own_(ser|ident)_id_seq owner to' "$OUT"
  fail=1
else
  echo "OK: no direct owner change of a linked sequence"
fi

same "range types and the schema of their multirange" \
  "select rn.nspname || '.' || rt.typname, mn.nspname || '.' || mt.typname
     from pg_range r
     join pg_type rt on rt.oid = r.rngtypid
     join pg_namespace rn on rn.oid = rt.typnamespace
     join pg_type mt on mt.oid = r.rngmultitypid
     join pg_namespace mn on mn.oid = mt.typnamespace
    where rn.nspname in (${SCHEMAS})
    order by 1"
expect_in_b "rv_r3t multiranges in test_schema" \
  "select count(*) from pg_type
    where typnamespace = 'test_schema'::regnamespace and typtype = 'm'
      and typname in ('rv_r3t_mis_multirange', 'rv_r3t_new_rng_multirange')" \
  "2"
if grep -Eiq '^[[:space:]]*alter type public\.rv_r3t_mis_multirange set schema test_schema;' "$OUT"; then
  echo "OK: misplaced multirange moved with SET SCHEMA"
else
  echo "ERROR: round 1 does not move public.rv_r3t_mis_multirange into test_schema"
  fail=1
fi

exit $fail
