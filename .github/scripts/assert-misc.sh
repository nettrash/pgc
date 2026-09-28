#!/usr/bin/env bash
# ============================================================================
# Step A - "Assert pg19 misc catalog state" - run after "Apply diff to
# database `a`" (env PGPASSWORD=postgres). Works on PG14-19; PG19-only checks
# are guarded by server_version_num >= 190000.
# ============================================================================
# pg19 misc: after round 1 is applied, database `a` must match `b` for every
# property the misc work package handles. Round 2 cannot see a property that
# both dumps lose the same way, so read the catalogs directly.
set -u
fail=0
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
ver=$(q a "select current_setting('server_version_num')::int")

same "text search dictionaries (template, options, owner)" \
  "select d.dictname, tn.nspname || '.' || t.tmplname, coalesce(d.dictinitoption, ''), pg_get_userbyid(d.dictowner)
     from pg_ts_dict d join pg_namespace n on n.oid = d.dictnamespace
     join pg_ts_template t on t.oid = d.dicttemplate join pg_namespace tn on tn.oid = t.tmplnamespace
    where n.nspname = 'test_schema' order by 1"
same "text search configuration mappings" \
  "select c.cfgname, m.maptokentype, m.mapseqno, dn.nspname || '.' || d.dictname
     from pg_ts_config c join pg_namespace n on n.oid = c.cfgnamespace
     left join pg_ts_config_map m on m.mapcfg = c.oid
     left join pg_ts_dict d on d.oid = m.mapdict left join pg_namespace dn on dn.oid = d.dictnamespace
    where n.nspname = 'test_schema' order by 1, 2, 3"
same "tsvector dependents of pg19_misc_tsconfig_nl (generated column, index)" \
  "select a.attname, pg_get_expr(ad.adbin, ad.adrelid)
     from pg_attribute a left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
    where a.attrelid = 'test_schema.misc_pg19_ts_docs'::regclass and a.attnum > 0 and not a.attisdropped
   union all
   select indexrelid::regclass::text, pg_get_indexdef(indexrelid) from pg_index
    where indrelid = 'test_schema.misc_pg19_ts_docs'::regclass order by 1"
# Stored generated values are data, which no compare looks at: recompute them.
# A row still holding the pre-ALTER stems means the column was added before
# the dictionary's in-place ALTER (on PG19; trivially 0 elsewhere).
stale=$(q a "select count(*) from test_schema.misc_pg19_ts_notes
              where note_tsv is distinct from to_tsvector('test_schema.pg19_misc_tsconfig_nl'::regconfig, note)") \
  || { echo "ERROR: misc_pg19_ts_notes query failed on a"; fail=1; stale=0; }
if [ "$stale" != 0 ]; then
  echo "ERROR: ${stale} misc_pg19_ts_notes rows in a hold tsvector values computed before the dictionary ALTER"
  fail=1
else
  echo "OK: misc_pg19_ts_notes generated tsvector values are current"
fi
same "extensions (name, version, schema)" \
  "select e.extname, e.extversion, n.nspname from pg_extension e
     join pg_namespace n on n.oid = e.extnamespace order by 1"
same "misc_pg19 column types and defaults (oid8 and friends)" \
  "select c.relname, a.attname, format_type(a.atttypid, a.atttypmod), coalesce(pg_get_expr(ad.adbin, ad.adrelid), '')
     from pg_class c join pg_namespace n on n.oid = c.relnamespace
     join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
     left join pg_attrdef ad on ad.adrelid = c.oid and ad.adnum = a.attnum
    where n.nspname = 'test_schema' and c.relname like 'misc\\_pg19\\_%' and c.relkind in ('r', 'p') order by 1, 2"
same "misc_pg19 constraints and indexes" \
  "select conrelid::regclass::text, conname, pg_get_constraintdef(oid) from pg_constraint
    where connamespace = 'test_schema'::regnamespace
      and (conrelid::regclass::text like 'test_schema.misc\\_pg19\\_%' or contypid <> 0 and contypid::regtype::text like 'test_schema.pg19\\_misc\\_%')
   union all
   select indrelid::regclass::text, indexrelid::regclass::text, pg_get_indexdef(indexrelid) from pg_index
    where indrelid::regclass::text like 'test_schema.misc\\_pg19\\_%' order by 1, 2"
# Multirange types and the internal range constructors are left out: pgc
# emits an unqualified multirange_type_name (a separate, pre-existing issue).
same "pg19_misc domains, range types and routines" \
  "select t.typname::text, t.typtype::text, format_type(t.typbasetype, t.typtypmod) from pg_type t
    where t.typnamespace = 'test_schema'::regnamespace and t.typname like 'pg19\\_misc\\_%'
      and t.typtype in ('d', 'r')
   union all
   select p.proname::text, p.prokind::text, pg_get_function_arguments(p.oid) || ' -> ' || pg_get_function_result(p.oid)
     from pg_proc p join pg_language l on l.oid = p.prolang
    where p.pronamespace = 'test_schema'::regnamespace and p.proname like 'pg19\\_misc\\_%'
      and l.lanname not in ('internal', 'c') order by 1"
same "pg19_misc view and materialized view definitions" \
  "select c.relname, c.relkind, pg_get_viewdef(c.oid, true), coalesce(c.reloptions::text, '')
     from pg_class c where c.relnamespace = 'test_schema'::regnamespace
      and c.relkind in ('v', 'm') and c.relname like 'pg19\\_misc\\_%' order by 1"
same "misc_pg19 table reloptions" \
  "select c.relname, coalesce(c.reloptions::text, '') from pg_class c
    where c.relnamespace = 'test_schema'::regnamespace and c.relkind in ('r', 'p')
      and c.relname like 'misc\\_pg19\\_%' order by 1"

if [ "$ver" -ge 190000 ]; then
  # PG19-only fixture content really is present, so the checks above did
  # not pass on two empty sides.
  opt=$(q a "select dictinitoption from pg_ts_dict where dictname = 'pg19_misc_dict_nl'")
  if [ "$opt" != "language = 'dutch_porter', stopwords = 'dutch'" ]; then
    echo "ERROR: pg19_misc_dict_nl options in a: '${opt}'"; fail=1
  fi
  n=$(q a "select count(*) from pg_attribute where attrelid = 'test_schema.misc_pg19_oid8_refs'::regclass
             and atttypid = 'oid8'::regtype and not attisdropped")
  [ "$n" -ge 4 ] || { echo "ERROR: expected >= 4 oid8 columns in a, found ${n}"; fail=1; }
  for obj in pg19_misc_v_ignore_nulls_new pg19_misc_mv_null_treatment pg19_misc_ja_view; do
    def=$(q a "select pg_get_viewdef('test_schema.${obj}'::regclass, true)")
    case "$obj" in
      pg19_misc_ja_view) pat='JSON_ARRAY( SELECT' ;;
      *) pat='IGNORE NULLS' ;;
    esac
    printf '%s' "$def" | grep -qF "$pat" || { echo "ERROR: ${obj} in a lacks '${pat}'"; fail=1; }
  done
  r=$(q a "select array_to_string(reloptions, ',') from pg_class where oid = 'test_schema.misc_pg19_reloptions_test'::regclass")
  [ "$r" = "fillfactor=80,autovacuum_parallel_workers=4,log_autoanalyze_min_duration=1000" ] \
    || { echo "ERROR: misc_pg19_reloptions_test reloptions in a: '${r}'"; fail=1; }
  t=$(q a "select note_tsv::text from test_schema.misc_pg19_ts_notes where id = 1")
  [ "$t" = "'mogelijk':1" ] \
    || { echo "ERROR: misc_pg19_ts_notes row 1 in a stores '${t}', not the dutch_porter stem"; fail=1; }
  e=$(q a "select extversion from pg_extension where extname = 'pg_stash_advice'")
  [ -n "$e" ] || { echo "ERROR: pg_stash_advice not created in a"; fail=1; }
fi
[ "$fail" -eq 0 ] || exit 1
echo "OK: misc catalog state of a matches b on server_version_num ${ver}"

# ============================================================================
# Step B - "Assert standard literals survive standard_conforming_strings = off"
# - standalone, any time after "Build pgc (release)" (env PGPASSWORD=postgres).
# Uses its own databases pgc_scs_a / pgc_scs_b; skipped on PG19+.
# ============================================================================
# A table named pg<non-underscore>... must be dumped and therefore created:
# the tables query once used `not like 'pg_%'`, where `_` matches any
# character, and silently skipped it.
if [ "$(psql -h 127.0.0.1 -U postgres -d a -X -tAc "select to_regclass('test_schema.pgx_like_escape') is not null")" != "t" ]; then
  echo "ERROR: test_schema.pgx_like_escape was not created in a (pg_ prefix filter treats _ as a wildcard)"
  exit 1
fi
echo "OK: tables named pgX... are dumped"
if [ "$(psql -h 127.0.0.1 -U postgres -d a -X -tAc "select string_agg(attname, '|' order by attnum) from pg_attribute where attrelid = to_regclass('test_schema.\"spaced Columns\"') and attnum > 0 and not attisdropped")" != 'Col X|a b c|plain_col' ]; then
  echo "ERROR: test_schema.\"spaced Columns\" was not created with its three columns in a"
  exit 1
fi
echo "OK: quoted column names with spaces survive CREATE TABLE"

# pg19 misc: a migration applied to a PostgreSQL 14-18 database whose clients
# default to standard_conforming_strings = off. pgc dumps literals in standard
# form, so the migration must switch the setting on for its own session, or
# psql reads 'C:\temp' as an escape string and stores a tab. PG19 cannot turn
# the setting off, so the step is skipped there.
set -u
port=${PGPORT:-5432}
pgc=${PGC:-./app/target/release/pgc}
work=${SCS_WORK:-/tmp/pgc_scs}
mkdir -p "$work"
P="psql -h 127.0.0.1 -p $port -U postgres -v ON_ERROR_STOP=1 -q"
ver=$($P -d postgres -tAc "select current_setting('server_version_num')::int")
if [ "$ver" -ge 190000 ]; then
  echo "SKIP: PostgreSQL ${ver} cannot set standard_conforming_strings = off"
  exit 0
fi
for db in pgc_scs_a pgc_scs_b; do
  $P -d postgres -c "drop database if exists ${db} with (force)" -c "create database ${db}" \
     -c "alter database ${db} set standard_conforming_strings = off"
done
$P -d pgc_scs_a -c "create table public.scs_probe (id integer primary key, path text)"
$P -d pgc_scs_b -c "create table public.scs_probe (id integer primary key, path text default E'C:\\\\temp')"
cat > "$work/pgc.conf" <<CONF
FROM_HOST=127.0.0.1
FROM_PORT=${port}
FROM_USER=postgres
FROM_PASSWORD=postgres
FROM_DATABASE=pgc_scs_a
FROM_SCHEME=public
FROM_SSL=false
FROM_DUMP=${work}/a.dump
TO_HOST=127.0.0.1
TO_PORT=${port}
TO_USER=postgres
TO_PASSWORD=postgres
TO_DATABASE=pgc_scs_b
TO_SCHEME=public
TO_SSL=false
TO_DUMP=${work}/b.dump
OUTPUT=${work}/output.sql
USE_DROP=true
USE_COMMENTS=false
GRANTS_MODE=ignore
USE_SINGLE_TRANSACTION=false
CONF
fail=0
"$pgc" --config "$work/pgc.conf" > "$work/round1.log" 2>&1 || { cat "$work/round1.log"; exit 1; }
head -1 "$work/output.sql" | grep -qx 'set standard_conforming_strings = on;' \
  || { echo "ERROR: the migration does not start with set standard_conforming_strings = on;"; cat "$work/output.sql"; fail=1; }
# Applied the way the CI applies every migration: psql, one statement at a time.
$P -d pgc_scs_a -f "$work/output.sql" || fail=1
v=$($P -d pgc_scs_a -tAc "insert into public.scs_probe (id) values (1) returning length(path) || ':' || (path = E'C:\\\\temp')")
[ "$v" = "7:true" ] || { echo "ERROR: default stored as length:equal = '${v}', expected 7:true"; fail=1; }
"$pgc" --config "$work/pgc.conf" > "$work/round2.log" 2>&1 || { cat "$work/round2.log"; exit 1; }
if [ -n "$(tr -d '[:space:]' < "$work/output.sql")" ]; then
  echo "ERROR: second compare not empty:"; cat "$work/output.sql"; fail=1
fi
for db in pgc_scs_a pgc_scs_b; do
  $P -d postgres -c "drop database if exists ${db} with (force)"
done
[ "$fail" -eq 0 ] || exit 1
echo "OK: standard literals survive a migration applied with standard_conforming_strings = off (PG ${ver})"