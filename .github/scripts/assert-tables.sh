#!/usr/bin/env bash
# pg19:tables — after applying round 1, database `a` must match `b` for CHECK
# enforceability and generated columns, and the in-place forms must have
# been used where the server supports them. The round-2 diff alone cannot
# prove either: a property lost on both sides, or a drop + re-add that
# converges anyway, still compares empty.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). The statement checks read the default-mode output; set
# TEXT_CHECKS=0 to run only the catalog checks (e.g. for an
# OUTPUT_FOR_PRODUCTION=true run, whose statements carry IF [NOT] EXISTS).
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
if [ "$ver" -ge 180000 ]; then enforced="c.conenforced"; else enforced="true"; fi
same "CHECK constraints" "
  select format('%s|%s|enforced=%s|validated=%s|noinherit=%s|local=%s|inh=%s|%s',
                c.conrelid::regclass, c.conname, $enforced, c.convalidated,
                c.connoinherit, c.conislocal, c.coninhcount, pg_get_constraintdef(c.oid))
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'test_schema' and t.relname like 'wp19\_tables\_%' and c.contype = 'c'
  order by 1"
same "generated columns" "
  select format('%s|%s|generated=%s|%s', a.attrelid::regclass, a.attname, a.attgenerated,
                pg_get_expr(d.adbin, d.adrelid))
  from pg_attribute a
  join pg_class t on t.oid = a.attrelid
  join pg_namespace n on n.oid = t.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where n.nspname = 'test_schema' and t.relname like 'wp19\_tables\_%'
    and t.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped
  order by 1"
same "relations, indexes and views" "
  select format('%s|%s', c.relkind, c.relname)
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'test_schema' and c.relname like 'wp19\_tables\_%'
    and c.relkind in ('r', 'p', 'v', 'i')
  order by 1"
same "inheritance" "
  select format('%s <- %s', i.inhparent::regclass, i.inhrelid::regclass)
  from pg_inherits i join pg_class c on c.oid = i.inhrelid
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'test_schema' and c.relname like 'wp19\_tables\_%'
  order by 1"
# SET EXPRESSION keeps the column's attnum; a drop + add moves it to the end.
attnum_sql() { echo "select attnum from pg_attribute where attrelid = 'test_schema.$1'::regclass and attname = '$2'"; }
kept_in_place() { # <table> <column> <min server_version_num>
  [ "$ver" -ge "$3" ] || return 0
  same "attnum of $1.$2 (SET EXPRESSION expected on $3+)" "$(attnum_sql "$1" "$2")"
}
kept_in_place wp19_tables_gen_stored total 170000
kept_in_place wp19_tables_gen_virtual_nochk doubled 180000
kept_in_place wp19_tables_gen_virtual doubled 190000
kept_in_place wp19_tables_gen_inh total 170000
kept_in_place wp19_tables_gen_inh_follow total 170000
kept_in_place wp19_tables_gen_inh_merged total 170000
kept_in_place wp19_tables_gen_part total 170000
kept_in_place wp19_tables_gen_part_1 total 170000
kept_in_place wp19_tables_gen_vinh doubled 180000
kept_in_place wp19_tables_gen_vinh_c doubled 180000
kept_in_place wp19_tables_gen_vpart v 190000

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
if [ "$text_checks" = 1 ]; then
  # The generated-column change runs after ADD COLUMN bonus, which the new
  # expression reads, and before DROP COLUMN legacy, which the old one read.
  if [ "$ver" -ge 170000 ]; then
    newcol="alter table only test_schema.wp19_tables_gen_newcol alter column total set expression as ((qty * 2) + (bonus * 3));"
  else
    newcol="alter table test_schema.wp19_tables_gen_newcol add column total integer generated always as ((qty * 2) + (bonus * 3)) stored;"
  fi
  in_order "wp19_tables_gen_newcol" \
    "alter table test_schema.wp19_tables_gen_newcol add column bonus integer;" \
    "$newcol" \
    "alter table test_schema.wp19_tables_gen_newcol drop column legacy;"
  # A type change with the expression change is a drop + add, never TYPE.
  in_order "wp19_tables_gen_retype" \
    "alter table test_schema.wp19_tables_gen_retype drop column stamp;" \
    "alter table test_schema.wp19_tables_gen_retype add column stamp date generated always as ('2020-01-01'::date + id) stored;"
  not_in_output "wp19_tables_gen_retype" "wp19_tables_gen_retype alter column stamp"
  # INHERITS followers never drop or add the column themselves, and a child
  # with an expression of its own is left alone.
  not_in_output "wp19_tables_gen_inh_follow" "alter table test_schema.wp19_tables_gen_inh_follow drop column total;"
  not_in_output "wp19_tables_gen_inh_follow" "alter table test_schema.wp19_tables_gen_inh_follow add column total"
  not_in_output "wp19_tables_gen_inh_own" "test_schema.wp19_tables_gen_inh_own alter column"
  if [ "$ver" -ge 180000 ] && [ "$ver" -lt 190000 ]; then
    grep -qF "alter table test_schema.wp19_tables_gen_vpart drop column v;" "$out" \
      || { echo "ERROR: PG18 must drop and re-add wp19_tables_gen_vpart.v (its new partition has a CHECK)"; fail=1; }
  fi
  if [ "$ver" -ge 190000 ]; then
    for c in "wp19_tables_ck alter constraint wp19_tables_ck_relax not enforced" \
             "wp19_tables_ck alter constraint wp19_tables_ck_tighten enforced" \
             "wp19_tables_ck alter constraint wp19_tables_ck_notvalid not enforced" \
             "wp19_tables_ck_part alter constraint wp19_tables_ck_part_amount enforced"; do
      grep -qF "alter table test_schema.$c;" "$out" || { echo "ERROR: expected in-place 'alter table test_schema.$c;'"; fail=1; }
    done
    if grep -q "drop constraint wp19_tables_ck" "$out"; then
      echo "ERROR: a CHECK enforceability flip was dropped and re-added on PG19"; fail=1
    fi
    in_order "wp19_tables_ck_parent before wp19_tables_ck_child" \
      "alter table test_schema.wp19_tables_ck_parent alter constraint wp19_tables_ck_amount not enforced;" \
      "alter table test_schema.wp19_tables_ck_child alter constraint wp19_tables_ck_amount not enforced;"
    # A column the table drops and re-adds takes its CHECK with it: the flip
    # stays a drop + re-add that runs first.
    in_order "wp19_tables_rc" \
      "alter table test_schema.wp19_tables_rc drop constraint wp19_tables_rc_g_check;" \
      "alter table test_schema.wp19_tables_rc drop column g;"
    not_in_output "wp19_tables_rc" "alter table test_schema.wp19_tables_rc alter constraint"
    # The child's merged copy stays enforced: drop + re-add on the parent.
    grep -qF "alter table test_schema.wp19_tables_keep_parent drop constraint wp19_tables_keep_amount;" "$out" \
      || { echo "ERROR: expected a drop + re-add of wp19_tables_keep_amount on the parent"; fail=1; }
    not_in_output "wp19_tables_keep" "alter constraint wp19_tables_keep_amount"
    grep -qF "alter table test_schema.wp19_tables_gen_vpart alter column v set expression as (id * 3);" "$out" \
      || { echo "ERROR: expected a recursing SET EXPRESSION on wp19_tables_gen_vpart"; fail=1; }
    if grep -q "drop view if exists test_schema.wp19_tables_gen_virtual_v" "$out"; then
      echo "ERROR: SET EXPRESSION keeps dependent views; wp19_tables_gen_virtual_v must not be dropped on PG19"; fail=1
    fi
  fi
fi
# reg-misc: a CHECK enforceability flip next to a rewrite of the column it
# reads. The rows must have been rewritten (values of a match b) with the
# CHECK in b's state, and on PG19 a relaxing flip runs before the rewrite and
# a tightening one after it.
same "rv_regmisc CHECK constraints" "
  select format('%s|%s|enforced=%s|validated=%s|%s', c.conrelid::regclass, c.conname,
                $enforced, c.convalidated, pg_get_constraintdef(c.oid))
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'test_schema' and t.relname like 'rv\_regmisc\_ck\_%' and c.contype = 'c'
  order by 1"
same "rv_regmisc generated columns" "
  select format('%s|%s|%s|generated=%s|%s', a.attrelid::regclass, a.attname,
                format_type(a.atttypid, a.atttypmod), a.attgenerated, pg_get_expr(d.adbin, d.adrelid))
  from pg_attribute a
  join pg_class t on t.oid = a.attrelid
  join pg_namespace n on n.oid = t.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where n.nspname = 'test_schema' and t.relname like 'rv\_regmisc\_ck\_%'
    and t.relkind = 'r' and a.attnum > 0 and not a.attisdropped
  order by 1"
rv_rows() { # the rows a holds after the migration; b has none, so compare to the expected values
  local got
  got=$(q a "select string_agg(format('%s', r), ',' order by r) from (select $2 as r from test_schema.$1) s") \
    || { echo "ERROR: $1 rows query failed on a"; fail=1; return; }
  [ "$got" = "$3" ] || { echo "ERROR: $1: expected rows '$3', got '$got'"; fail=1; }
}
rv_rows rv_regmisc_ck_expr "g" "-6"
rv_rows rv_regmisc_ck_type "c" "1"
rv_rows rv_regmisc_ck_tight_expr "g" "-2"
rv_rows rv_regmisc_ck_tight_type "c" "1"
[ "$ver" -ge 180000 ] && rv_rows rv_regmisc_ck_virt "g" "-6"
if [ "$text_checks" = 1 ] && [ "$ver" -ge 190000 ]; then
  for t in expr virt; do
    in_order "rv_regmisc_ck_$t relaxed before SET EXPRESSION" \
      "alter table test_schema.rv_regmisc_ck_$t alter constraint rv_regmisc_ck_${t}_g not enforced;" \
      "alter table only test_schema.rv_regmisc_ck_$t alter column g set expression as (c * 3);"
  done
  in_order "rv_regmisc_ck_type relaxed before ALTER TYPE" \
    "alter table test_schema.rv_regmisc_ck_type alter constraint rv_regmisc_ck_type_c not enforced;" \
    "alter table test_schema.rv_regmisc_ck_type alter column c type numeric(6, 0);"
  in_order "rv_regmisc_ck_tight_expr tightened after SET EXPRESSION" \
    "alter table only test_schema.rv_regmisc_ck_tight_expr alter column g set expression as (c + 1);" \
    "alter table test_schema.rv_regmisc_ck_tight_expr alter constraint rv_regmisc_ck_tight_expr_g enforced;"
  in_order "rv_regmisc_ck_tight_type tightened after ALTER TYPE" \
    "alter table test_schema.rv_regmisc_ck_tight_type alter column c type numeric(6, 0);" \
    "alter table test_schema.rv_regmisc_ck_tight_type alter constraint rv_regmisc_ck_tight_type_c enforced;"
  not_in_output "rv_regmisc_ck (in place on PG19)" "drop constraint rv_regmisc_ck_"
fi
# reg-misc: the same under a parent's rewrite. The partitions' own CHECKs are
# checked by the parent's recursing ALTER COLUMN ... TYPE, so the relax (PG19)
# or the drop (earlier majors) of rv_regmisc_ckfam_p1_c runs ahead of it, and
# the tightened rv_regmisc_ckfam_p2_c is validated after it.
same "rv_regmisc_ckfam CHECK constraints" "
  select format('%s|%s|enforced=%s|validated=%s|%s', c.conrelid::regclass, c.conname,
                $enforced, c.convalidated, pg_get_constraintdef(c.oid))
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'test_schema' and t.relname like 'rv\_regmisc\_ckfam\_%' and c.contype = 'c'
  order by 1"
same "rv_regmisc_ckfam column types" "
  select format('%s|%s|%s', a.attrelid::regclass, a.attname, format_type(a.atttypid, a.atttypmod))
  from pg_attribute a
  join pg_class t on t.oid = a.attrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'test_schema' and t.relname like 'rv\_regmisc\_ckfam\_%'
    and t.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped
  order by 1"
rv_rows rv_regmisc_ckfam_p "c" "1,1"
if [ "$text_checks" = 1 ]; then
  if [ "$ver" -ge 190000 ]; then
    rv_first="alter table test_schema.rv_regmisc_ckfam_p1 alter constraint rv_regmisc_ckfam_p1_c not enforced;"
    rv_last="alter table test_schema.rv_regmisc_ckfam_p2 alter constraint rv_regmisc_ckfam_p2_c enforced;"
  else
    rv_first="alter table test_schema.rv_regmisc_ckfam_p1 drop constraint rv_regmisc_ckfam_p1_c;"
    rv_last="alter table test_schema.rv_regmisc_ckfam_p2 add constraint rv_regmisc_ckfam_p2_c check (c <> 0.6) ;"
  fi
  rv_type="alter table test_schema.rv_regmisc_ckfam_p alter column c type numeric(6, 0);"
  in_order "rv_regmisc_ckfam_p1 let go before the parent's ALTER TYPE" "$rv_first" "$rv_type"
  in_order "rv_regmisc_ckfam_p2 validated after the parent's ALTER TYPE" "$rv_type" "$rv_last"
fi
[ "$fail" -eq 0 ] || exit 1
echo "OK: pg19:tables catalog state of a matches b (server_version_num $ver)"