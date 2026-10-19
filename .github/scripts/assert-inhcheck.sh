#!/usr/bin/env bash
# fix:inhcheck — after applying round 1, the inherited CHECK constraints of the
# `inhck_*` fixtures in database `a` must match `b` copy for copy: local,
# coninhcount, enforced, validated. The round-2 diff cannot see any of it: a
# dump holds only local constraints, so a copy a child only inherits that the
# migration dropped, or counted twice, compares equal. The probes then insert
# a row each child's CHECK must refuse.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). The statement checks read the default-mode output; set
# TEXT_CHECKS=0 to run only the catalog checks.
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
same "inhck CHECK constraints, inherited copies included" "
  select format('%s|%s|enforced=%s|validated=%s|local=%s|inh=%s|%s',
                c.conrelid::regclass, c.conname, $enforced, c.convalidated,
                c.conislocal, c.coninhcount, pg_get_constraintdef(c.oid))
  from pg_constraint c
  join pg_class t on t.oid = c.conrelid
  join pg_namespace n on n.oid = t.relnamespace
  where n.nspname = 'test_schema' and t.relname like 'inhck\_%' and c.contype = 'c'
  order by 1"
same "inhck columns" "
  select format('%s|%s|%s|generated=%s|%s', a.attrelid::regclass, a.attname,
                format_type(a.atttypid, a.atttypmod), a.attgenerated,
                pg_get_expr(d.adbin, d.adrelid))
  from pg_attribute a
  join pg_class t on t.oid = a.attrelid
  join pg_namespace n on n.oid = t.relnamespace
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
  where n.nspname = 'test_schema' and t.relname like 'inhck\_%'
    and t.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped
  order by 1"
same "inhck inheritance" "
  select format('%s <- %s', i.inhparent::regclass, i.inhrelid::regclass)
  from pg_inherits i join pg_class c on c.oid = i.inhrelid
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'test_schema' and c.relname like 'inhck\_%'
  order by 1"

# The probes: each insert must fail with a check violation in `a`, as it does
# in `b`. An accepted row is rolled back with the block.
refuses() { # <table> <columns> <values> <min server_version_num>
  [ "$ver" -ge "$4" ] || return 0
  if [ "$(q a "select to_regclass('test_schema.$1') is not null")" != t ]; then
    echo "ERROR: test_schema.$1 is missing from a (server_version_num $ver)"; fail=1; return
  fi
  local sql="do \$\$ begin
      insert into test_schema.$1 ($2) values ($3);
      raise exception 'accepted';
    exception when check_violation then null;
    end \$\$"
  q a "$sql" > /dev/null 2>&1 \
    || { echo "ERROR: test_schema.$1 accepts a row its CHECK must refuse (server_version_num $ver)"; fail=1; }
}
refuses inhck_a_child a -1000 0
refuses inhck_e_grandchild a -1000 0
refuses inhck_h_child a -1000 0
refuses inhck_b_child a -1000 170000
refuses inhck_c_child a -1000 170000
refuses inhck_d_child a -1000 170000
refuses inhck_f_m a -1000 170000
refuses inhck_g_child a -1000 170000
refuses inhck_va_child a -1000 190000
refuses inhck_vc_child a -1000 190000
refuses inhck_en_child a -2000 0
refuses inhck_enc_child t "'x'" 0
refuses inhck_enp "k, a" "1, -2000" 0

if [ "$text_checks" = 1 ]; then
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
  if [ "$ver" -ge 170000 ]; then
    # The CHECK goes before the family's first SET EXPRESSION and comes back
    # after its last, so neither statement sees it.
    in_order "inhck_a: CHECK out of the way of SET EXPRESSION" \
      "alter table test_schema.inhck_a drop constraint inhck_a_v;" \
      "alter table only test_schema.inhck_a alter column v set expression as (a * 3);" \
      "alter table only test_schema.inhck_a_child alter column v set expression as (a * 3);" \
      "alter table test_schema.inhck_a add constraint inhck_a_v check"
    in_order "inhck_c: the parent's CHECK out of the way of the child's SET EXPRESSION" \
      "alter table test_schema.inhck_c drop constraint inhck_c_v;" \
      "alter table only test_schema.inhck_c_child alter column v set expression as (a * 7);" \
      "alter table test_schema.inhck_c add constraint inhck_c_v check"
    in_order "inhck_g: TO's new CHECK added after the child's SET EXPRESSION" \
      "alter table only test_schema.inhck_g_child alter column v set expression as (a * 7);" \
      "alter table test_schema.inhck_g add constraint inhck_g_v check"
    if grep -qF "drop constraint inhck_i_b" "$out"; then
      echo "ERROR: inhck_i_b reads no changed column and must not be dropped"; fail=1
    fi
  fi
  if [ "$ver" -ge 190000 ]; then
    # A recursing rebuild of the CHECK resets the child's ENFORCED copy.
    in_order "inhck_en: child's copy ENFORCED again after the parent's retype" \
      "alter table test_schema.inhck_en alter column a type bigint;" \
      "alter table test_schema.inhck_en_child alter constraint inhck_en_a enforced;"
    in_order "inhck_enc: child's copy ENFORCED again after the parent's new collation" \
      "alter table test_schema.inhck_enc alter column t type text collate \"C\";" \
      "alter table test_schema.inhck_enc_child alter constraint inhck_enc_t enforced;"
    in_order "inhck_enp: partition's copy ENFORCED again after the parent's retype" \
      "alter table test_schema.inhck_enp alter column a type bigint;" \
      "alter table test_schema.inhck_enp_1 alter constraint inhck_enp_a enforced;"
  fi
fi
[ "$fail" -eq 0 ] || exit 1
echo "OK: fix:inhcheck inherited CHECK constraints of a match b (server_version_num $ver)"
