#!/usr/bin/env bash
# pre-columns (rv_pcols_*) — after applying round 1, database `a` must match
# `b` for the columns, extended statistics, CHECK constraints, partition
# trigger clones (a parent outside the dumped schemas included), and what a
# column drop + add drops first and puts back (policies, triggers, foreign
# keys, named NOT NULL constraints) of the fixture. The round-2 diff alone cannot prove it: a
# property lost on both sides compares empty, and so does a migration that
# failed under ON_ERROR_STOP=0 but left the objects equal by accident.
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
expect() { # <label> <sql on a> <expected>
  local got
  got=$(q a "$2") || { echo "ERROR: $1 query failed on a"; fail=1; return; }
  [ "$got" = "$3" ] || { echo "ERROR: $1: expected '$3', got '$got'"; fail=1; }
}
rels="n.nspname = 'test_schema' and c.relname like 'rv\\_pcols\\_%'"

# (1) Columns: types and generation; the indexes and views a generated
# column's drop takes along (its CHECKs and statistics are compared below);
# and the rows, whose generated values were computed again.
same "rv_pcols columns" "
  select format('%s|%s|%s|%s|%s', a.attrelid::regclass, a.attname,
                format_type(a.atttypid, a.atttypmod), a.attgenerated,
                pg_get_expr(d.adbin, d.adrelid))
  from pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where $rels and c.relkind in ('r', 'p', 'v') and a.attnum > 0 and not a.attisdropped
  order by 1"
# A partition's copy of its parent's index is compared without its name,
# which PostgreSQL picks (and --output-for-production spells differently).
same "rv_pcols indexes" "
  select case when exists (select 1 from pg_inherits h where h.inhrelid = i.indexrelid)
              then format('%s|(partition index)|%s', c.oid::regclass,
                          regexp_replace(pg_get_indexdef(i.indexrelid), ' INDEX \S+ ON ', ' INDEX ON '))
              else format('%s|%s|%s', c.oid::regclass, i.indexrelid::regclass,
                          pg_get_indexdef(i.indexrelid)) end
  from pg_index i join pg_class c on c.oid = i.indrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels order by 1"
same "rv_pcols views" "
  select format('%s|%s', c.oid::regclass, pg_get_viewdef(c.oid))
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where $rels and c.relkind = 'v' order by 1"
expect "rv_pcols_gen rows" "select string_agg(format('%s:%s:%s', id, a, g), ',' order by id) from test_schema.rv_pcols_gen" "1:5:10,2:7:14"
expect "rv_pcols_gen_expr rows" "select string_agg(format('%s:%s', a, g), ',') from test_schema.rv_pcols_gen_expr" "5:15"
expect "rv_pcols_gen_plain rows" "select string_agg(format('%s:%s', a, g), ',') from test_schema.rv_pcols_gen_plain" "5:10"
expect "rv_pcols_gen_part rows" "select string_agg(format('%s:%s', a, g), ',') from test_schema.rv_pcols_gen_part" "2:20"
if [ "$ver" -ge 180000 ]; then
  expect "rv_pcols_gen_virt rows" "select string_agg(format('%s:%s', a, g), ',') from test_schema.rv_pcols_gen_virt" "5:10"
fi

# (2) Extended statistics: kinds, the plain columns as a set (PostgreSQL
# prints them in attnum order, which differs between a and b by design), and
# the expressions in order.
same "rv_pcols statistics" "
  select format('%s|%s|%s|%s', s.stxname, s.stxkind::text,
                (select string_agg(a.attname, ',' order by a.attname)
                 from unnest(s.stxkeys) k
                 join pg_attribute a on a.attrelid = s.stxrelid and a.attnum = k),
                coalesce(array_to_string(pg_get_statisticsobjdef_expressions(s.oid), ' ; '), ''))
  from pg_statistic_ext s
  where s.stxnamespace = 'test_schema'::regnamespace and s.stxname like 'rv\\_pcols\\_%'
  order by 1"

# (3) CHECK constraints of the inheritance families: definition,
# enforceability and how each copy is held (local, inherited).
if [ "$ver" -ge 180000 ]; then enforced="k.conenforced"; else enforced="true"; fi
same "rv_pcols CHECK constraints" "
  select format('%s|%s|enforced=%s|validated=%s|local=%s|inh=%s|%s',
                k.conrelid::regclass, k.conname, $enforced, k.convalidated,
                k.conislocal, k.coninhcount, pg_get_constraintdef(k.oid))
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and k.contype = 'c'
  order by 1"

# (4) Triggers, partition clones included (internal on PostgreSQL 14):
# every clone must exist once, in its own enabled state, with its comment,
# and every trigger with its definition (UPDATE OF / WHEN included). The
# partitions of rv_pcols_outside.rv_pcols_out, a parent outside the dumped
# schemas, are in test_schema and so among them.
same "rv_pcols triggers" "
  select format('%s|%s|clone=%s|%s|%s|%s', t.tgrelid::regclass, t.tgname, t.tgparentid <> 0,
                t.tgenabled, coalesce(obj_description(t.oid, 'pg_trigger'), '-'),
                pg_get_triggerdef(t.oid))
  from pg_trigger t
  join pg_class c on c.oid = t.tgrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and (not t.tgisinternal or t.tgparentid <> 0) and t.tgconstraint = 0
  order by 1"
expect "rv_pcols_trg_zgone dropped" "select count(*) from pg_class where relname in ('rv_pcols_trg_zgone', 'rv_pcols_trg_agone')" "0"
expect "rv_pcols_out_gone dropped, rv_pcols_out_3 attached" "
  select string_agg(c.relname, ',' order by c.relname) from pg_inherits h
  join pg_class c on c.oid = h.inhrelid
  where h.inhparent = 'rv_pcols_outside.rv_pcols_out'::regclass" "rv_pcols_out_1,rv_pcols_out_2,rv_pcols_out_3"
expect "rv_pcols_att_2 attached" "
  select count(*) from pg_inherits
  where inhrelid = 'test_schema.rv_pcols_att_2'::regclass
    and inhparent = 'test_schema.rv_pcols_att'::regclass" "1"

# (5) What a column drop + add has to drop first and put back: the
# policies, and the constraints other than CHECK (compared above) — the
# foreign key on another table's mixed-case unique key, and on PostgreSQL 18+
# the named NOT NULL constraint. An auto-named NOT NULL constraint (PG18+)
# is compared without its name, which PostgreSQL picks: a table attached as
# a partition keeps the names it had, one created as a partition takes its
# parent's.
same "rv_pcols policies" "
  select format('%s|%s|%s|%s', p.polrelid::regclass, p.polname,
                pg_get_expr(p.polqual, p.polrelid), pg_get_expr(p.polwithcheck, p.polrelid))
  from pg_policy p
  join pg_class c on c.oid = p.polrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels order by 1"
same "rv_pcols constraints (not CHECK)" "
  select format('%s|%s|%s|%s', k.conrelid::regclass,
                case when k.contype = 'n' and k.conname like '%\\_not\\_null%'
                     then '(auto)' else k.conname end,
                k.contype, pg_get_constraintdef(k.oid))
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and k.contype <> 'c'
  order by 1"
expect "rv_pcols_blk rows" "select string_agg(format('%s:%s:%s', id, a, \"G x\"), ',') from test_schema.rv_pcols_blk" "1:3:6"
expect "rv_pcols_blk_ref rows" "select string_agg(format('%s:%s', id, gx), ',') from test_schema.rv_pcols_blk_ref" "1:6"
expect "rv_pcols_ihc rows" "select string_agg(format('%s:%s:%s', id, a, g), ',' order by id) from only test_schema.rv_pcols_ihc" "1:4:8"
expect "rv_pcols_ihgc rows" "select string_agg(format('%s:%s:%s:%s', id, a, g, h), ',') from test_schema.rv_pcols_ihgc" "2:5:10:6"
if [ "$ver" -ge 180000 ]; then
  expect "rv_pcols_blk named NOT NULL" "
    select string_agg(pg_get_constraintdef(oid), ',') from pg_constraint
    where conrelid = 'test_schema.rv_pcols_blk'::regclass and contype = 'n'
      and conname = 'rv_pcols_blk_gx_nn'" 'NOT NULL "G x"'
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
  in_order "rv_pcols_gen: generated column out of the way of the retype" \
    "alter table test_schema.rv_pcols_gen drop column g;" \
    "alter table test_schema.rv_pcols_gen alter column a type bigint;" \
    "alter table test_schema.rv_pcols_gen add column g integer generated always as (a * 2) stored;"
  in_order "rv_pcols_gen_plain: DROP EXPRESSION before the retype" \
    "alter table test_schema.rv_pcols_gen_plain alter column g drop expression;" \
    "alter table test_schema.rv_pcols_gen_plain alter column a type bigint;"
  in_order "rv_pcols_gen_gone: dropped before the retype" \
    "alter table test_schema.rv_pcols_gen_gone drop column g;" \
    "alter table test_schema.rv_pcols_gen_gone alter column a type bigint;"
  in_order "rv_pcols_gen_both: generated column dropped first" \
    "alter table test_schema.rv_pcols_gen_both drop column g;" \
    "alter table test_schema.rv_pcols_gen_both drop column a;"
  # Only the changed object is re-created.
  not_in_output "rv_pcols_st_same (plain column order only)" "drop statistics if exists test_schema.rv_pcols_st_same;"
  not_in_output "rv_pcols_st_expr (plain column order only)" "drop statistics if exists test_schema.rv_pcols_st_expr;"
  grep -qF "drop statistics if exists test_schema.rv_pcols_st_changed;" "$out" \
    || { echo "ERROR: rv_pcols_st_changed (new column set) was not re-created"; fail=1; }
  # A clone is never created or dropped on its own.
  for p in rv_pcols_trg_ex_1 rv_pcols_trg_ex_1a rv_pcols_trg_ex_2 rv_pcols_trg_new_1 \
           rv_pcols_trg_new_2 rv_pcols_trg_agone; do
    not_in_output "clone on $p" "ON test_schema.$p FOR EACH"
    not_in_output "clone on $p" "trigger if exists rv_pcols_trg_changed on test_schema.$p;"
    not_in_output "clone on $p" "trigger if exists rv_pcols_trg_gone on test_schema.$p;"
  done
  # The family's CHECKs: all drops, parents first, then the re-adds.
  in_order "rv_pcols_ckx family" \
    "alter table test_schema.rv_pcols_ckx_par drop constraint rv_pcols_ckx_amount;" \
    "alter table test_schema.rv_pcols_ckx_ch drop constraint rv_pcols_ckx_amount;" \
    "alter table test_schema.rv_pcols_ckx_gch drop constraint rv_pcols_ckx_amount;" \
    "alter table test_schema.rv_pcols_ckx_par add constraint rv_pcols_ckx_amount check (amount > 1) ;"
  in_order "rv_pcols_cku family" \
    "alter table test_schema.rv_pcols_cku_par drop constraint rv_pcols_cku_amount;" \
    "alter table test_schema.rv_pcols_cku_ch drop constraint rv_pcols_cku_amount;" \
    "alter table test_schema.rv_pcols_cku_par add constraint rv_pcols_cku_amount check (amount > 0) ;"
  not_in_output "rv_pcols_cku_ch (inherited only in TO)" "alter table test_schema.rv_pcols_cku_ch add constraint"
  if [ "$ver" -ge 180000 ] && [ "$ver" -lt 190000 ]; then
    for f in ckr ckt ckc; do
      in_order "rv_pcols_$f family (PG18: drop + re-add)" \
        "alter table test_schema.rv_pcols_${f}_par drop constraint rv_pcols_${f}_amount;" \
        "alter table test_schema.rv_pcols_${f}_ch drop constraint rv_pcols_${f}_amount;" \
        "alter table test_schema.rv_pcols_${f}_par add constraint rv_pcols_${f}_amount check (amount > 0)"
    done
  fi
  # INHERITS: only the parent retypes; a child's own generated column
  # reading the column is out of the way before it, and back after.
  in_order "rv_pcols_ihc: generated column out of the way of the parent's retype" \
    "alter table test_schema.rv_pcols_ihc drop column g;" \
    "alter table test_schema.rv_pcols_ihgc drop column h;" \
    "alter table test_schema.rv_pcols_ihp alter column a type bigint;" \
    "alter table test_schema.rv_pcols_ihc add column g bigint generated always as (a * 2) stored;" \
    "alter table test_schema.rv_pcols_ihgc add column h bigint generated always as (a + 1) stored;"
  for t in rv_pcols_ihc rv_pcols_ihgc rv_pcols_ihc2; do
    not_in_output "$t (inherits the retype)" "alter table test_schema.$t alter column a type"
  done
  # The column's blockers go first; the trigger comes back with the others.
  for stmt in \
    "alter table test_schema.rv_pcols_blk_ref drop constraint if exists rv_pcols_blk_ref_gx_fkey;" \
    "drop policy if exists rv_pcols_blk_pol on test_schema.rv_pcols_blk;" \
    "drop trigger if exists rv_pcols_blk_upd on test_schema.rv_pcols_blk;" \
    "drop trigger if exists rv_pcols_blk_when on test_schema.rv_pcols_blk;"; do
    in_order "rv_pcols_blk: blocker dropped before the column" "$stmt" \
      "alter table test_schema.rv_pcols_blk drop column \"G x\";"
  done
  in_order "rv_pcols_blkp: blockers dropped before the column" \
    "drop trigger if exists rv_pcols_blkp_upd on test_schema.rv_pcols_blkp;" \
    "drop trigger if exists rv_pcols_blkp_own on test_schema.rv_pcols_blkp_1;" \
    "alter table test_schema.rv_pcols_blkp drop column g;" \
    "CREATE TRIGGER rv_pcols_blkp_upd AFTER UPDATE OF g ON test_schema.rv_pcols_blkp" \
    "alter table test_schema.rv_pcols_blkp_2 disable trigger rv_pcols_blkp_upd;"
  # The own trigger goes before the ATTACH that clones the parent's.
  in_order "rv_pcols_att_2: own trigger dropped before the attach" \
    "drop trigger if exists rv_pcols_att_trg on test_schema.rv_pcols_att_2;" \
    "alter table test_schema.rv_pcols_att attach partition test_schema.rv_pcols_att_2"
  # A clone of a parent outside the dumped schemas is never created or
  # dropped, only set.
  not_in_output "orphan clones" "ON test_schema.rv_pcols_out_"
  not_in_output "orphan clones" "drop trigger if exists rv_pcols_out_"
  in_order "orphan clone states" \
    "alter table test_schema.rv_pcols_out_1a enable always trigger rv_pcols_out_ins;" \
    "alter table test_schema.rv_pcols_out_2 disable trigger rv_pcols_out_upd;" \
    "alter table test_schema.rv_pcols_out_3 disable trigger rv_pcols_out_ins;"
  if [ "$ver" -ge 190000 ]; then
    not_in_output "rv_pcols CHECK flips (in place on PG19)" "drop constraint rv_pcols_ckr_"
    not_in_output "rv_pcols CHECK flips (in place on PG19)" "drop constraint rv_pcols_ckt_"
    not_in_output "rv_pcols CHECK flips (in place on PG19)" "drop constraint rv_pcols_ckc_"
  fi
fi
[ "$fail" -eq 0 ] || exit 1
echo "OK: pre-columns fixtures in a match b (server_version_num $ver)"
