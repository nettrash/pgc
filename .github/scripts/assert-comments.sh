#!/usr/bin/env bash
# ============================================================================
# fix:comments - names carrying comment delimiters. Run after "Apply diff to
# database `a`" (env PGPASSWORD=postgres, PGPORT). Works on PG14-19.
# ============================================================================
# The pg19_comments_* fixtures are named with `*/` and `/*`, which close or
# nest the block-comment headers pgc writes (`/* Table: s.t*/`). Round 2 would
# stay empty if a swallowed statement were lost on both sides the same way,
# so check the catalogs and the round-1 script directly.
set -u
fail=0
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -v ON_ERROR_STOP=1 -XtAc "$2"; }
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

# 1. The table the first fixture name tries to drop is still there, row and all.
victim=$(q a "select count(*) from public.pg19_comments_victim") \
  || { echo "ERROR: public.pg19_comments_victim is gone from a"; fail=1; victim=0; }
if [ "$victim" != 1 ]; then
  echo "ERROR: public.pg19_comments_victim holds ${victim} rows in a, expected 1"
  fail=1
else
  echo "OK: public.pg19_comments_victim survived the migration"
fi

# 2. Every pg19_comments_* object reached its TO shape: nothing was swallowed
#    by a comment left open or cut short.
same "pg19_comments relations and columns" \
  "select c.relname, c.relkind, coalesce(a.attname, ''), coalesce(format_type(a.atttypid, a.atttypmod), '')
     from pg_class c join pg_namespace n on n.oid = c.relnamespace
     left join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
    where n.nspname = 'test_schema' and c.relname like 'pg19\\_comments\\_%'
    order by 1, 3"
same "pg19_comments view and materialized view definitions" \
  "select c.relname, c.relkind, pg_get_viewdef(c.oid) from pg_class c
    where c.relnamespace = 'test_schema'::regnamespace and c.relkind in ('v', 'm')
      and c.relname like 'pg19\\_comments\\_%' order by 1"
same "pg19_comments routines" \
  "select p.proname, pg_get_function_arguments(p.oid), pg_get_function_result(p.oid), p.prosrc
     from pg_proc p where p.pronamespace = 'test_schema'::regnamespace
      and p.proname like 'pg19\\_comments\\_%' order by 1"
same "pg19_comments types and enum labels" \
  "select t.typname, t.typtype, coalesce(string_agg(e.enumlabel, ',' order by e.enumsortorder), '')
     from pg_type t left join pg_enum e on e.enumtypid = t.oid
    where t.typnamespace = 'test_schema'::regnamespace and t.typname like 'pg19\\_comments\\_%'
      and t.typtype <> 'b' group by 1, 2 order by 1"

# 3. Names in statements, not only in comments. A materialized view is named
#    through quote_ident (pgc read it raw: "pg19_comments_MvNew" came out as
#    pg19_comments_mvnew, and "pg19_comments_Mv*/" did not parse), the guarded
#    REPLICA IDENTITY of a partition is a DO block whose dollar tag must not
#    occur in the partition's name ("pg19_comments_rp$pgc$1"), and a changed
#    foreign-table option is SET, not added a second time.
same "pg19_comments materialized view indexes and column comments" \
  "select c.relname,
          coalesce((select string_agg(pg_get_indexdef(i.indexrelid), '; ' order by pg_get_indexdef(i.indexrelid))
                      from pg_index i where i.indrelid = c.oid), ''),
          coalesce((select string_agg(a.attname || '=' || col_description(c.oid, a.attnum), ',' order by a.attnum)
                      from pg_attribute a
                     where a.attrelid = c.oid and a.attnum > 0 and col_description(c.oid, a.attnum) is not null), '')
     from pg_class c
    where c.relnamespace = 'test_schema'::regnamespace and c.relkind = 'm'
      and c.relname like 'pg19\\_comments\\_%' order by 1"
same "pg19_comments replica identity" \
  "select c.relname, c.relreplident,
          coalesce((select ic.relname from pg_index i join pg_class ic on ic.oid = i.indexrelid
                     where i.indrelid = c.oid and i.indisreplident), '')
     from pg_class c
    where c.relnamespace = 'test_schema'::regnamespace and c.relkind in ('r', 'p')
      and c.relname like 'pg19\\_comments\\_%' order by 1"
same "pg19_comments foreign table options" \
  "select c.relname, array(select o from unnest(ft.ftoptions) o order by o)::text
     from pg_foreign_table ft join pg_class c on c.oid = ft.ftrelid
    where c.relnamespace = 'test_schema'::regnamespace
      and c.relname like 'pg19\\_comments\\_%' order by 1"
# The checks above compare a with b; these make sure b really holds what the
# fixture built, so a fixture that failed to apply cannot pass them vacuously.
built_ok=1
for check in \
  "select count(*) from pg_class where oid = to_regclass('test_schema.\"pg19_comments_MvNew\"') and relkind = 'm'|1" \
  "select count(*) from pg_class where oid = to_regclass('test_schema.\"pg19_comments_Mv*/\"') and relkind = 'm'|1" \
  "select relreplident from pg_class where oid = to_regclass('test_schema.\"pg19_comments_rp\$pgc\$1\"')|i" \
  "select array(select o from unnest(ftoptions) o order by o)::text from pg_foreign_table where ftrelid = to_regclass('test_schema.pg19_comments_ft')|{schema_name=public,table_name=b,updatable=false}"; do
  sql=${check%|*}; want=${check##*|}
  for db in a b; do
    got=$(q "$db" "$sql") || got="(query failed)"
    if [ "$got" != "$want" ]; then
      echo "ERROR: on ${db}, ${sql} gave '${got}', expected '${want}'"
      fail=1; built_ok=0
    fi
  done
done
if [ $built_ok -eq 1 ]; then
  echo "OK: materialized view names, the replica-identity guard and foreign-table options"
fi

# 4. In the script itself the payload appears only inside the quoted table
#    name, never as a statement of its own.
if [ ! -f "$OUT" ]; then
  echo "ERROR: round-1 script ${OUT} not found"
  fail=1
else
  # (the second form is the name as a USE_COMMENTS=true header prints it)
  stray=$(sed -e 's#"pg19_comments_t\*/drop table public\.pg19_comments_victim;/\*"##g' \
              -e 's#"pg19_comments_t\* /drop table public\.pg19_comments_victim;/ \*"##g' "$OUT" \
    | grep -c 'pg19_comments_victim' || true)
  if [ "$stray" != 0 ]; then
    echo "ERROR: round-1 script names pg19_comments_victim outside the quoted fixture name (${stray} lines)"
    fail=1
  else
    echo "OK: round-1 script names pg19_comments_victim only inside the quoted fixture name"
  fi
fi
exit $fail
