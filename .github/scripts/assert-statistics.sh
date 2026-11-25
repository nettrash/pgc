#!/usr/bin/env bash
# pg19:statistics — after round 1 is applied, FROM (`a`) must hold exactly
# TO's (`b`) extended statistics, property for property. The round-2 diff
# cannot see a property pgc loses on both sides; this compares the catalogs
# directly, spot-checks the fixture values, and checks in the round-1
# migration that statistics are restored only where PostgreSQL really
# dropped them.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). Connection: psql -h 127.0.0.1 -U postgres, with PGPORT and
# PGPASSWORD taken from the environment. Works on the default and the
# OUTPUT_FOR_PRODUCTION=true output alike.
set -u
out="${OUTPUT_SQL:-/tmp/output.sql}"
PSQL="psql -h 127.0.0.1 -U postgres -X -q -tA -v ON_ERROR_STOP=1"
fail=0
err() { echo "ERROR: $*"; fail=1; }

[ -r "$out" ] || { echo "ERROR: round-1 migration $out not found"; exit 1; }
ver=$($PSQL -d a -c "show server_version_num") || { echo "ERROR: cannot reach database a"; exit 1; }

# The plain columns are compared as a set: PostgreSQL prints them in attnum
# order, and a and b need not have their columns in the same physical order
# (a column the migration drops and re-adds moves to the end), which pgc does
# not treat as a difference. The expressions keep their order.
stats_sql="select n.nspname, s.stxname, tn.nspname, c.relname,
                  s.stxkind::text,
                  (select string_agg(quote_ident(a.attname), ', ' order by a.attname)
                   from unnest(s.stxkeys) k
                   join pg_attribute a on a.attrelid = s.stxrelid and a.attnum = k),
                  coalesce(array_to_string(pg_get_statisticsobjdef_expressions(s.oid), ', '), ''),
                  coalesce(s.stxstattarget::text, 'default'),
                  pg_get_userbyid(s.stxowner),
                  coalesce(obj_description(s.oid, 'pg_statistic_ext'), '')
           from pg_statistic_ext s
           join pg_namespace n on n.oid = s.stxnamespace
           join pg_class c on c.oid = s.stxrelid
           join pg_namespace tn on tn.oid = c.relnamespace
           where n.nspname in ('test_schema', 'shared_schema', 'new_reporting_schema',
                               'data', 'test_order', 'public')
           order by 1, 2"
ra=$($PSQL -d a -c "$stats_sql") || err "statistics query failed on a"
rb=$($PSQL -d b -c "$stats_sql") || err "statistics query failed on b"
if [ "$ra" != "$rb" ]; then
  err "extended statistics in a differ from b after applying the migration (server_version_num $ver)"
  diff <(printf '%s\n' "$ra") <(printf '%s\n' "$rb") || true
fi

has_stat() { # <schema> <name>: exists in a
  [ "$($PSQL -d a -c "select count(*) from pg_statistic_ext s join pg_namespace n on n.oid = s.stxnamespace where n.nspname = '$1' and s.stxname = '$2'")" = 1 ]
}
target_is() { # <name> <expected target>
  local got
  got=$($PSQL -d a -c "select s.stxstattarget from pg_statistic_ext s where s.stxname = '$1'")
  [ "$got" = "$2" ] || err "$1 has statistics target '$got' in a, expected $2"
}
in_output() { # <extended regex>: the round-1 migration has a matching line
  grep -Eq "^$1" "$out"
}

# Created from expression ON lists the old split mangled; restored after a
# column drop + add, a table recreate, a routine CASCADE (partitions and
# TO-only objects included) and a materialized view rebuild; left in place
# on a partition whose parent changes a column type (PG14+).
for st in pg19_stat_expr_kinds pg19_stat_expr_onlist pg19_stat_expr_single \
          pg19_stat_expr_tricky pg19_stat_conv_st pg19_stat_conv_setexpr \
          pg19_stat_part_st pg19_stat_fn_items_st pg19_stat_fn_part1_st \
          pg19_stat_fn_items_new pg19_stat_pc_keep pg19_stat_pc_kind \
          pg19_stat_pc_parent pg19_stat_mv_rebuilt pg19_stat_mv_new; do
  has_stat test_schema "$st" || err "test_schema.$st missing from a"
done
for st in pg19_stat_expr_from_only; do
  has_stat test_schema "$st" && err "test_schema.$st should have been dropped"
done
onlist=$($PSQL -d a -c "select pg_get_statisticsobjdef_columns(s.oid) from pg_statistic_ext s where s.stxname = 'pg19_stat_expr_onlist'")
[ "$onlist" = "qty, EXTRACT(year FROM shipped_on)" ] || err "pg19_stat_expr_onlist has ON list '$onlist'"

# Explicit statistics targets (a nullable smallint from PG17 on), including
# the ones carried through a restore and the ones PostgreSQL resets when it
# rebuilds an object for ALTER COLUMN ... TYPE or SET EXPRESSION.
for pair in pg19_stat_expr_target=250 pg19_stat_conv_st=200 pg19_stat_conv_setexpr=250 \
            pg19_stat_part_st=400 pg19_stat_fn_items_st=600 pg19_stat_fn_part1_st=700 \
            pg19_stat_pc_keep=450 pg19_stat_pc_parent=350 pg19_stat_mv_rebuilt=800; do
  target_is "${pair%%=*}" "${pair#*=}"
done

# The partition t_pg19_stat_pc1 is not dropped when its parent changes the
# type of `v`, so its unchanged statistics must not be created again.
in_output "create statistics( if not exists)? test_schema\.pg19_stat_pc_keep " &&
  err "pg19_stat_pc_keep was created again, though its table was never dropped"
# `tripled` changes its STORED expression: SET EXPRESSION from PG17 on (the
# statistics stay; only their target is set again), drop + add before.
if [ "$ver" -ge 170000 ]; then
  in_output "create statistics( if not exists)? test_schema\.pg19_stat_conv_setexpr " &&
    err "pg19_stat_conv_setexpr was recreated, though SET EXPRESSION keeps it"
  in_output "alter statistics test_schema\.pg19_stat_conv_setexpr set statistics 250;" ||
    err "the statistics target of pg19_stat_conv_setexpr was not set again after SET EXPRESSION"
else
  in_output "create statistics( if not exists)? test_schema\.pg19_stat_conv_setexpr " ||
    err "pg19_stat_conv_setexpr was not restored after the drop + add of tripled"
fi

if [ "$ver" -ge 190000 ]; then
  # Statistics on virtual generated columns (PG19+): `doubled` changes with
  # SET EXPRESSION, `readded` is dropped and re-added.
  for st in test_schema.pg19_stat_vcol_rewrite test_schema.pg19_stat_vcol_expr \
            test_schema.pg19_stat_vcol_readd public.pg19_stat_vcol_xschema \
            test_schema.pg19_stat_vcol_kinds test_schema.pg19_stat_vcol_to_only; do
    has_stat "${st%%.*}" "${st#*.}" || err "$st missing from a"
  done
  has_stat test_schema pg19_stat_vcol_from_only &&
    err "test_schema.pg19_stat_vcol_from_only should have been dropped"
  target_is pg19_stat_vcol_rewrite 500
  target_is pg19_stat_vcol_readd 550
  in_output "create statistics( if not exists)? test_schema\.pg19_stat_vcol_rewrite " &&
    err "pg19_stat_vcol_rewrite was recreated, though SET EXPRESSION keeps it"
  for st in test_schema.pg19_stat_vcol_readd public.pg19_stat_vcol_xschema; do
    in_output "create statistics( if not exists)? ${st//./\\.} " ||
      err "$st was not restored after the drop + add of readded"
  done
fi

[ "$fail" -eq 0 ] || exit 1
echo "OK: extended statistics in a match b (server_version_num $ver)"
