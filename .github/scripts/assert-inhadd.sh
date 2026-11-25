#!/usr/bin/env bash
# fix:inhadd and fix:retypesettings — after applying round 1, the columns of
# the `inhadd_*` and `rtset_*` fixtures in database `a` must match `b`:
# local or inherited, NOT NULL, default, storage, compression, statistics
# target, comment, and the inheritance links.
#
# The round-2 diff cannot see the inhadd failure: an INHERITS parent's
# recursing ADD COLUMN already gives every child the column, so a child's
# own ADD COLUMN fails ("column ... already exists") while the catalogs
# end up right. CI applies with ON_ERROR_STOP=0, so the statement checks
# read the migration itself: no child adds a column its parent's ADD
# COLUMN creates.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). Set TEXT_CHECKS=0 to run only the catalog checks.
set -u
out="${OUTPUT_SQL:-/tmp/output.sql}"
text_checks="${TEXT_CHECKS:-1}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
ver=$(q a "show server_version_num")
fail=0
same() { # <label> <sql>: the query must return the same rows in a and b
  local ra rb
  ra=$(q a "$2") || { echo "ERROR: $1 query failed on a"; fail=1; return; }
  rb=$(q b "$2") || { echo "ERROR: $1 query failed on b"; fail=1; return; }
  if [ "$ra" != "$rb" ]; then
    echo "ERROR: $1 differs between a and b (server_version_num $ver)"
    diff <(printf '%s\n' "$ra") <(printf '%s\n' "$rb") || true
    fail=1
  fi
}
same "inhadd and rtset columns" "
  select format('%s|%s|%s|local=%s|inh=%s|notnull=%s|default=%s|storage=%s|compression=%s|stats=%s|comment=%s',
                a.attrelid::regclass, a.attname,
                format_type(a.atttypid, a.atttypmod), a.attislocal, a.attinhcount,
                a.attnotnull, pg_get_expr(d.adbin, d.adrelid), a.attstorage,
                a.attcompression, coalesce(a.attstattarget, -1),
                col_description(a.attrelid, a.attnum))
  from pg_attribute a
  join pg_class t on t.oid = a.attrelid
  join pg_namespace n on n.oid = t.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where n.nspname = 'test_schema'
    and (t.relname like 'inhadd\_%' or t.relname like 'rtset\_%')
    and t.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped
  order by 1"
same "inhadd and rtset inheritance" "
  select format('%s <- %s', i.inhparent::regclass, i.inhrelid::regclass)
  from pg_inherits i join pg_class c on c.oid = i.inhrelid
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'test_schema'
    and (c.relname like 'inhadd\_%' or c.relname like 'rtset\_%')
  order by 1"
# The fixture values really arrived (both sides could have lost them).
expect() { # <label> <sql> <expected>
  local got
  got=$(q a "$2") || { echo "ERROR: $1 query failed on a"; fail=1; return; }
  if [ "$got" != "$3" ]; then
    echo "ERROR: $1: got '$got', expected '$3' (server_version_num $ver)"; fail=1
  fi
}
expect "inhadd_chi.note keeps the parent's storage and its own default" \
  "select a.attstorage::text || ' ' || pg_get_expr(d.adbin, d.adrelid)
   from pg_attribute a join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
   where a.attrelid = 'test_schema.inhadd_chi'::regclass and a.attname = 'note'" \
  "m 'c'::text"
expect "rtset_own.v is MAIN after its retype" \
  "select attstorage from pg_attribute
   where attrelid = 'test_schema.rtset_own'::regclass and attname = 'v'" "m"
expect "rtset_typed.b is pglz after its type's retype" \
  "select attcompression from pg_attribute
   where attrelid = 'test_schema.rtset_typed'::regclass and attname = 'b'" "p"

if [ "$text_checks" = 1 ]; then
  for child in inhadd_chi inhadd_grand inhadd_left inhadd_own; do
    if grep -qF "alter table test_schema.${child} add column" "$out"; then
      echo "ERROR: ${child} adds a column its parent's recursing ADD COLUMN creates:"
      grep -F "alter table test_schema.${child} add column" "$out"
      fail=1
    fi
  done
  line_of() { grep -nF "$1" "$out" | head -1 | cut -d: -f1; }
  in_order() { # <label> <statement>...: each statement present, in this order
    local label=$1 prev=0 n; shift
    for s in "$@"; do
      n=$(line_of "$s")
      if [ -z "$n" ]; then
        echo "ERROR: $label: missing: $s"; fail=1; return
      fi
      if [ "$n" -le "$prev" ]; then
        echo "ERROR: $label: out of order: $s"; fail=1; return
      fi
      prev=$n
    done
  }
  in_order "inhadd_left: the copy the parent's add gave it is dropped once it has left" \
    "alter table test_schema.inhadd_par add column note text default 'p'::text not null;" \
    "alter table test_schema.inhadd_left no inherit test_schema.inhadd_par;" \
    "alter table test_schema.inhadd_left drop column note;"
  in_order "inhadd_chi: its own settings, after the parent's add" \
    "alter table test_schema.inhadd_par add column tag text;" \
    "alter table test_schema.inhadd_chi alter column tag set not null;"
fi
[ "$fail" -eq 0 ] || exit 1
echo "OK: fix:inhadd and fix:retypesettings columns of a match b (server_version_num $ver)"
