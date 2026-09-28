#!/usr/bin/env bash
# r3-gencol (rv_r3g_*) — after applying round 1, database `a` must match `b`
# for what goes with a stored generated column that a routine's DROP ...
# CASCADE takes (views and a view over one, a SQL-standard routine reading
# it, column privileges, UPDATE OF / WHEN triggers, publication rows, a
# partition's own index, policy, trigger and privileges on its copy), for the
# publication rows and partition objects on a generated column re-created
# around a retype, for a PostgreSQL 14 generated column over a function or
# operator whose type changes, and for the policies and triggers in the way of
# an in-place column retype — and, from the review fixes, for what calls a
# routine that waits for the re-add, the TO-only objects on a re-added column,
# statistics on a materialized view held back with the routines, policies of
# other tables reading a retyped column, and PostgreSQL 14 generated columns
# made plain (rewritten, keeping their values). The round-2 diff alone cannot
# prove it: a property lost on both sides compares empty, and so does a
# migration that failed under ON_ERROR_STOP=0 but left the objects equal by
# accident.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). Connection: psql -h 127.0.0.1 -U postgres, with PGPORT and
# PGPASSWORD taken from the environment. The statement checks read the
# default-mode output; set TEXT_CHECKS=0 to run only the catalog checks (e.g.
# for an OUTPUT_FOR_PRODUCTION=true run, whose statements carry IF [NOT]
# EXISTS).
set -u
out="${OUTPUT_SQL:-/tmp/output.sql}"
text_checks="${TEXT_CHECKS:-1}"
q() { psql -h 127.0.0.1 -U postgres -X -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
[ -r "$out" ] || { echo "ERROR: round-1 migration $out not found"; exit 1; }
ver=$(q a "show server_version_num") || { echo "ERROR: cannot reach database a"; exit 1; }
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
expect() { # <db> <label> <sql> <expected>
  local got
  got=$(q "$1" "$3") || { echo "ERROR: $2 query failed on $1"; fail=1; return; }
  [ "$got" = "$4" ] || { echo "ERROR: $2 on $1: expected '$4', got '$got'"; fail=1; }
}
rels="n.nspname = 'test_schema' and c.relname like 'rv\\_r3g\\_%'"

# Columns (type, generation), views, and the rows whose generated values were
# computed again.
same "rv_r3g columns" "
  select format('%s|%s|%s|%s|%s', a.attrelid::regclass, a.attname,
                format_type(a.atttypid, a.atttypmod), a.attgenerated,
                pg_get_expr(d.adbin, d.adrelid))
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where $rels and c.relkind in ('r', 'p', 'v') and a.attnum > 0 and not a.attisdropped
  order by 1"
same "rv_r3g views" "
  select format('%s|%s|%s', c.oid::regclass, pg_get_viewdef(c.oid), c.relacl)
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where $rels and c.relkind = 'v' order by 1"
for db in a b; do
  expect "$db" "rv_r3g views present" "
    select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where $rels and c.relkind = 'v'" "8"
done
same "rv_r3g routines" "
  select format('%s|%s', p.oid::regprocedure, pg_get_functiondef(p.oid))
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
  where n.nspname = 'test_schema' and p.proname like 'rv\\_r3g\\_%' order by 1"
expect a "rv_r3g_items rows" "select string_agg(format('%s:%s:%s', id, a, g), ',') from test_schema.rv_r3g_items" "1:3:6"
expect a "rv_r3g_part rows" "select string_agg(format('%s:%s:%s', id, a, g), ',') from test_schema.rv_r3g_part" "1:4:8"
expect a "rv_r3g_c16 rows" "select string_agg(format('%s:%s:%s', id, a, g), ',') from test_schema.rv_r3g_c16" "1:5:10"
expect a "rv_r3g_pdep rows" "select string_agg(format('%s:%s:%s', a, b, g), ',') from test_schema.rv_r3g_pdep" "2:3:5"
expect a "rv_r3g_pg14 rows" "select string_agg(format('%s:%s:%s:%s', v, g, o, b), ',') from test_schema.rv_r3g_pg14" "5:15:8:5"
# Generated columns made plain keep their values: on PostgreSQL 14 through a
# rewrite, which also leaves no dependency on the functions behind (a DROP
# ... CASCADE of rv_r3g_gone_fn used to take rv_r3g_plain.h with its data).
expect a "rv_r3g_plain rows" "select string_agg(format('%s:%s:%s:%s', id, v, g, h), ',') from test_schema.rv_r3g_plain" "1:5:15:25"
expect a "rv_r3g_plainp rows" "select string_agg(format('%s:%s:%s', id, v, g), ',') from test_schema.rv_r3g_plainp" "1:2:6"
expect a "rv_r3g_plain dependencies on routines" "
  select count(*) from pg_depend d
  where d.classid = 'pg_class'::regclass and d.objsubid > 0 and d.refclassid = 'pg_proc'::regclass
    and d.objid in ('test_schema.rv_r3g_plain'::regclass, 'test_schema.rv_r3g_plainp'::regclass,
                    'test_schema.rv_r3g_plainp_1'::regclass)" "0"
expect a "rv_r3g_plain.h comment" "
  select col_description(a.attrelid, a.attnum) from pg_attribute a
  where a.attrelid = 'test_schema.rv_r3g_plain'::regclass and a.attname = 'h'" "rv_r3g kept"
# What calls a routine waiting for the re-add, and what only TO has on a
# re-added column, is there — on both sides, so a loss on both cannot hide.
for db in a b; do
  expect "$db" "rv_r3g_use default" "
    select pg_get_expr(d.adbin, d.adrelid) from pg_attrdef d
    where d.adrelid = 'test_schema.rv_r3g_use'::regclass" "test_schema.rv_r3g_items_max()"
  expect "$db" "rv_r3g review-fix constraints present" "
    select count(*) from pg_constraint
    where conname in ('rv_r3g_use_chk', 'rv_r3g_use2_chk', 'rv_r3g_newdep_chk', 'rv_r3g_newdep_uk')
       or (conrelid = 'test_schema.rv_r3g_newdep_ref'::regclass and contype = 'f')" "5"
  expect "$db" "rv_r3g review-fix policies present" "
    select count(*) from pg_policy
    where polname in ('rv_r3g_use_pol', 'rv_r3g_newdep_pol', 'rv_r3g_rt_a_reader_pol', 'rv_r3g_rt_a_new_pol')" "4"
  expect "$db" "rv_r3g review-fix indexes present" "
    select count(*) from pg_class
    where relnamespace = 'test_schema'::regnamespace
      and relname in ('rv_r3g_newdep_g', 'rv_r3g_plain_g', 'rv_r3g_plainp_1_g')" "3"
  expect "$db" "rv_r3g_items_mv and its statistics present" "
    select count(*) from pg_statistic_ext s join pg_class c on c.oid = s.stxrelid
    where s.stxname = 'rv_r3g_items_mv_st' and c.relname = 'rv_r3g_items_mv' and c.relkind = 'm'" "1"
done

# Column privileges, row security, policies, triggers (partition clones
# included, in their enabled state), indexes, constraints and statistics.
same "rv_r3g column privileges" "
  select format('%s|%s|%s', a.attrelid::regclass, a.attname, a.attacl)
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and a.attacl is not null and not a.attisdropped order by 1"
for db in a b; do
  expect "$db" "rv_r3g column privileges present" "
    select count(*) from pg_attribute a
    join pg_class c on c.oid = a.attrelid
    join pg_namespace n on n.oid = c.relnamespace
    where $rels and a.attacl is not null and not a.attisdropped" "7"
done
same "rv_r3g row security" "
  select format('%s|%s', c.oid::regclass, c.relrowsecurity)
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where $rels and c.relkind in ('r', 'p') order by 1"
same "rv_r3g policies" "
  select format('%s|%s|%s|%s|%s', p.polrelid::regclass, p.polname, p.polpermissive,
                pg_get_expr(p.polqual, p.polrelid), pg_get_expr(p.polwithcheck, p.polrelid))
  from pg_policy p
  join pg_class c on c.oid = p.polrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels order by 1"
same "rv_r3g triggers" "
  select format('%s|%s|clone=%s|%s|%s', t.tgrelid::regclass, t.tgname, t.tgparentid <> 0,
                t.tgenabled, pg_get_triggerdef(t.oid))
  from pg_trigger t
  join pg_class c on c.oid = t.tgrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and (not t.tgisinternal or t.tgparentid <> 0) and t.tgconstraint = 0
  order by 1"
for db in a b; do
  expect "$db" "rv_r3g own triggers present" "
    select count(*) from pg_trigger t
    join pg_class c on c.oid = t.tgrelid
    join pg_namespace n on n.oid = c.relnamespace
    where $rels and not t.tgisinternal and t.tgparentid = 0" "9"
done
same "rv_r3g indexes" "
  select format('%s|%s', c.oid::regclass,
                regexp_replace(pg_get_indexdef(i.indexrelid), ' INDEX \S+ ON ', ' INDEX ON '))
  from pg_index i join pg_class c on c.oid = i.indrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and not exists (select 1 from pg_inherits h where h.inhrelid = i.indexrelid)
  order by 1"
expect a "rv_r3g_pdep_1 own index" "
  select count(*) from pg_class where oid = 'test_schema.rv_r3g_pdep_1_g'::regclass" "1"
same "rv_r3g constraints" "
  select format('%s|%s|%s|%s', k.conrelid::regclass,
                case when k.contype = 'n' then '(not null)' else k.conname end,
                k.contype, pg_get_constraintdef(k.oid))
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels
  order by 1"
same "rv_r3g statistics" "
  select format('%s|%s|%s', s.stxname, s.stxkind::text,
                (select string_agg(a.attname, ',' order by a.attname)
                 from unnest(s.stxkeys) k
                 join pg_attribute a on a.attrelid = s.stxrelid and a.attnum = k))
  from pg_statistic_ext s
  where s.stxnamespace = 'test_schema'::regnamespace and s.stxname like 'rv\\_r3g\\_%'
  order by 1"

# Publication rows: row filters from PostgreSQL 15, generated columns in a
# column list from 18. Columns are compared by name: a re-added column gets a
# new attnum.
if [ "$ver" -ge 150000 ]; then
  same "rv_r3g publication rows" "
    select format('%s|%s|%s|%s', p.pubname, r.prrelid::regclass,
                  (select string_agg(a.attname, ',' order by a.attname)
                   from unnest(r.prattrs) k
                   join pg_attribute a on a.attrelid = r.prrelid and a.attnum = k),
                  pg_get_expr(r.prqual, r.prrelid))
    from pg_publication_rel r join pg_publication p on p.oid = r.prpubid
    where p.pubname like 'rv\\_r3g\\_%' order by 1"
  if [ "$ver" -ge 180000 ]; then rows=8; else rows=6; fi
  for db in a b; do
    expect "$db" "rv_r3g publication rows present" "
      select count(*) from pg_publication_rel r join pg_publication p on p.oid = r.prpubid
      where p.pubname like 'rv\\_r3g\\_%'" "$rows"
  done
fi

if [ "$text_checks" = 1 ]; then
  line_of() { grep -nF "$1" "$out" | head -1 | cut -d: -f1; }
  in_order() { # <label> <statement>...: each statement present, in this order
    local label=$1 prev=0 n; shift
    for s in "$@"; do
      n=$(line_of "$s")
      if [ -z "$n" ] || [ "$n" -le "$prev" ]; then
        echo "ERROR: $label: expected '$s' after line $prev (found at '${n}')"; fail=1; return
      fi
      prev=$n
    done
  }
  not_in_output() { # <label> <text>
    if grep -qF "$2" "$out"; then echo "ERROR: $1: unexpected '$2'"; fail=1; fi
  }
  # The column the routine's CASCADE re-adds keeps its type until then: an
  # ALTER COLUMN ... TYPE ahead of it is redundant, and PostgreSQL 14
  # refuses it.
  for t in rv_r3g_items rv_r3g_part; do
    not_in_output "$t: retype ahead of the re-add" "alter table test_schema.$t alter column g type"
  done
  in_order "rv_r3g_items: readers re-created after the re-add" \
    "drop view if exists test_schema.rv_r3g_items_v;" \
    "drop function if exists test_schema.rv_r3g_fn (x integer) cascade;" \
    "alter table test_schema.rv_r3g_items add column if not exists g bigint generated always as (test_schema.rv_r3g_fn(a)) stored;" \
    "CREATE OR REPLACE VIEW test_schema.rv_r3g_items_v as"
  in_order "rv_r3g_c16: view after both re-adds" \
    "alter table test_schema.rv_r3g_c16 drop column g;" \
    "alter table test_schema.rv_r3g_c16 alter column a type bigint;" \
    "drop function if exists test_schema.rv_r3g_f16 (x bigint) cascade;" \
    "alter table test_schema.rv_r3g_c16 add column if not exists g bigint" \
    "CREATE OR REPLACE VIEW test_schema.rv_r3g_c16_v as"
  in_order "rv_r3g_rt_docs: policy dropped ahead of the retype" \
    "drop policy if exists rv_r3g_rt_docs_pol on test_schema.rv_r3g_rt_docs;" \
    "alter table test_schema.rv_r3g_rt_docs alter column tenant_id type bigint;" \
    "create policy rv_r3g_rt_docs_pol on test_schema.rv_r3g_rt_docs"
  in_order "rv_r3g_rt_orders: trigger dropped ahead of the retype" \
    "drop trigger if exists rv_r3g_rt_orders_upd on test_schema.rv_r3g_rt_orders;" \
    "alter table test_schema.rv_r3g_rt_orders alter column status type character varying(20);" \
    "CREATE TRIGGER rv_r3g_rt_orders_upd"
  if [ "$ver" -lt 150000 ]; then
    # PostgreSQL 14 cannot retype a generated column over a function or
    # operator in place: dropped and re-added. A built-in-only one can.
    for col in g o; do
      not_in_output "rv_r3g_pg14.$col retyped in place on 14" "alter table test_schema.rv_r3g_pg14 alter column $col type"
      in_order "rv_r3g_pg14.$col dropped and re-added on 14" \
        "alter table test_schema.rv_r3g_pg14 drop column $col;" \
        "alter table test_schema.rv_r3g_pg14 add column $col bigint generated always as"
    done
    in_order "rv_r3g_pg14.b retyped in place" "alter table test_schema.rv_r3g_pg14 alter column b type bigint;"
  else
    in_order "rv_r3g_pg14.g retyped in place" "alter table test_schema.rv_r3g_pg14 alter column g type bigint;"
  fi
  # Review fixes: what calls a routine re-created after the re-add follows
  # it; policies of other tables reading a retyped column follow the retype.
  in_order "rv_r3g_use: callers after rv_r3g_items_max" \
    "alter table test_schema.rv_r3g_items add column if not exists g bigint" \
    "create or replace function test_schema.rv_r3g_items_max()" \
    "alter table test_schema.rv_r3g_use add constraint rv_r3g_use_chk"
  in_order "rv_r3g_use2: CHECK after rv_r3g_items_top is re-created" \
    "drop function if exists test_schema.rv_r3g_items_top () cascade;" \
    "alter table test_schema.rv_r3g_use2 add constraint rv_r3g_use2_chk"
  for pol in rv_r3g_rt_a_reader_pol rv_r3g_rt_a_new_pol; do
    in_order "$pol after the retype it reads" \
      "alter table test_schema.rv_r3g_rt_docs alter column tenant_id type bigint;" \
      "create policy $pol on"
  done
  if [ "$ver" -lt 150000 ]; then
    not_in_output "rv_r3g_plain.g retyped in place on 14" "alter table test_schema.rv_r3g_plain alter column g type"
    in_order "rv_r3g_plain.h rewritten before its function is dropped" \
      "alter table test_schema.rv_r3g_plain alter column \"pgc_rewrite_h\" type integer using (h);" \
      "alter table test_schema.rv_r3g_plain rename column \"pgc_rewrite_h\" to h;" \
      "drop function if exists test_schema.rv_r3g_gone_fn (x integer) cascade;"
  fi
  if [ "$ver" -ge 150000 ]; then
    in_order "rv_r3g_pubrt: member released before the retype" \
      "ALTER PUBLICATION rv_r3g_pub_rt_filter DROP TABLE ONLY test_schema.rv_r3g_pubrt;" \
      "alter table test_schema.rv_r3g_pubrt drop column v;" \
      "ALTER PUBLICATION rv_r3g_pub_rt_filter SET TABLE test_schema.rv_r3g_pubrt WHERE ((v > 0));"
  fi
fi

[ "$fail" -eq 0 ] && echo "OK: rv_r3g generated columns, readers, privileges, triggers, policies and publication rows in a match b (server_version_num $ver)"
exit "$fail"
