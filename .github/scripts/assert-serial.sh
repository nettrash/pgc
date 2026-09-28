#!/usr/bin/env bash
# ============================================================================
# fix:serial - "Assert owned sequences and serial columns" - run after
# "Apply diff to database `a`" (env PGPASSWORD=postgres, PGPORT set). Works on
# PG14-19; PG15+ / PG19-only checks are guarded by server_version_num.
# ============================================================================
# After round 1 is applied, every test_schema.pg19_serial_* sequence in `a`
# must match `b`: type, options, persistence, comment and the column it is
# OWNED BY (pg_depend deptype 'a', or 'i' for an identity sequence), and every
# pg19_serial_* column its type, NOT NULL and default. Round 2 cannot see a
# lost OWNED BY: the sequence hash does not include it.
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
ver=$(q b "select current_setting('server_version_num')::int")

same "pg19_serial sequences (type, options, persistence, comment, OWNED BY)" \
  "select c.relname, format_type(s.seqtypid, null), s.seqstart, s.seqincrement, s.seqmin, s.seqmax,
          s.seqcache, s.seqcycle, c.relpersistence, coalesce(obj_description(c.oid, 'pg_class'), '-'),
          coalesce((select d.deptype::text || ':' || d.refobjid::regclass::text || '.' || quote_ident(a.attname)
                      from pg_depend d
                      join pg_attribute a on a.attrelid = d.refobjid and a.attnum = d.refobjsubid
                     where d.classid = 'pg_class'::regclass and d.objid = c.oid
                       and d.refclassid = 'pg_class'::regclass and d.deptype in ('a', 'i')), '-')
     from pg_sequence s join pg_class c on c.oid = s.seqrelid
     join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'test_schema' and c.relname like 'pg19\_serial\_%' order by 1"
# A partition's own column default is not compared: pgc does not diff
# partition-level column defaults at all (a separate table-comparison issue),
# so pg19_serial_part_p1.id comes out of round 1 without its default.
same "pg19_serial columns (type, NOT NULL, default)" \
  "select c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
          case when c.relispartition then '(partition default not compared)'
               else coalesce(pg_get_expr(ad.adbin, ad.adrelid), '-') end
     from pg_attribute a join pg_class c on c.oid = a.attrelid
     join pg_namespace n on n.oid = c.relnamespace
     left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
    where n.nspname = 'test_schema' and c.relname like 'pg19\_serial\_%'
      and c.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped order by 1, 2"

# Not vacuous: all 19 fixture sequences owned through OWNED BY exist in b.
owned=$(q b "select count(*) from pg_sequence s join pg_class c on c.oid = s.seqrelid
              join pg_namespace n on n.oid = c.relnamespace
              join pg_depend d on d.classid = 'pg_class'::regclass and d.objid = c.oid
               and d.refclassid = 'pg_class'::regclass and d.deptype = 'a'
             where n.nspname = 'test_schema' and c.relname like 'pg19\_serial\_%'") || owned=error
if [ "$owned" != 19 ]; then
  echo "ERROR: expected 19 owned pg19_serial sequences in b, found ${owned}"
  fail=1
fi
if [ "$ver" -ge 150000 ]; then
  # The ALTER SEQUENCE ... SET [UN]LOGGED fixtures took effect in b.
  pers=$(q b "select string_agg(relname || '=' || relpersistence::text, ',' order by relname) from pg_class
               where relnamespace = 'test_schema'::regnamespace
                 and relname in ('pg19_serial_seq_logged_id_seq', 'pg19_serial_seq_unlogged_id_seq')") || pers=error
  if [ "$pers" != "pg19_serial_seq_logged_id_seq=p,pg19_serial_seq_unlogged_id_seq=u" ]; then
    echo "ERROR: unexpected sequence persistence in b: ${pers}"
    fail=1
  fi
fi
if [ "$ver" -ge 190000 ]; then
  typ=$(q b "select format_type(atttypid, atttypmod) from pg_attribute
              where attrelid = 'test_schema.pg19_serial_oid8'::regclass and attname = 'id'") || typ=error
  if [ "$typ" != oid8 ]; then
    echo "ERROR: test_schema.pg19_serial_oid8.id is ${typ} in b, expected oid8"
    fail=1
  fi
fi

# Emitted SQL: only a genuine serial becomes a pseudo-type; the others keep
# their own type and get an explicit sequence plus OWNED BY. The optional
# "if not exists" is what OUTPUT_FOR_PRODUCTION=true adds.
if ! awk '/^create table (if not exists )?test_schema\.pg19_serial_genuine /,/^\);/' "$OUT" | grep -q '^    big bigserial'; then
  echo "ERROR: pg19_serial_genuine.big is not emitted as bigserial"
  fail=1
fi
if ! awk '/^create table (if not exists )?test_schema\.pg19_serial_gpart /,/^\)/' "$OUT" | grep -q '^    id bigserial'; then
  echo "ERROR: pg19_serial_gpart.id (partitioned table) is not emitted as bigserial"
  fail=1
fi
for seq in pg19_serial_num_id_seq pg19_serial_custom_counter pg19_serial_mismatch_id_seq \
           pg19_serial_part_p1_id_seq pg19_serial_ident_extra pg19_serial_shared_id_seq; do
  if ! grep -Eq "^create sequence (if not exists )?test_schema\.${seq} " "$OUT"; then
    echo "ERROR: no create sequence for test_schema.${seq}"
    fail=1
  fi
  if ! grep -q "^alter sequence test_schema\.${seq} owned by " "$OUT"; then
    echo "ERROR: no owned by for test_schema.${seq}"
    fail=1
  fi
done
# FROM-only ordinary sequence owned by an identity column that TO keeps.
if ! grep -q '^drop sequence if exists test_schema\.pg19_serial_ident_drop_extra;' "$OUT"; then
  echo "ERROR: no drop sequence for test_schema.pg19_serial_ident_drop_extra"
  fail=1
fi
explicit='num|custom|mismatch|tuned|nullable|renamed|plain|shared'
# On PG14 the persistence fixtures are ordinary serials.
[ "$ver" -ge 150000 ] && explicit="${explicit}|seq_logged|seq_unlogged"
if grep -Eq "^ +id (small|big)?serial" <(awk "/^create (unlogged )?table (if not exists )?test_schema\\.pg19_serial_(${explicit}) /,/^\\);/" "$OUT"); then
  echo "ERROR: a pg19_serial column that serial cannot reproduce was emitted as a serial pseudo-type"
  fail=1
fi
[ "$fail" -eq 0 ] || exit 1
echo "OK: owned sequences and serial columns match"
