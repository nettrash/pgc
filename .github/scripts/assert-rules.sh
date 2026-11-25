#!/usr/bin/env bash
# fix:rules - after round 1 is applied, database `a` must carry exactly the
# rules `b` has (definition and comment). Round 2 cannot see a rule that both
# dumps lose the same way, so read the catalogs directly. Works on PG14-19;
# the ON CONFLICT DO SELECT check only runs on PG19+.
set -u
fail=0
out="${OUTPUT_SQL:-/tmp/output.sql}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -X -v ON_ERROR_STOP=1 -tAc "$2"; }

rules_sql="select n.nspname || '.' || c.relname || '.' || r.rulename || E'\n'
       || pg_get_ruledef(r.oid, true) || E'\n-- comment: '
       || coalesce(obj_description(r.oid, 'pg_rewrite'), '<none>')
  from pg_rewrite r
  join pg_class c on c.oid = r.ev_class
  join pg_namespace n on n.oid = c.relnamespace
 where r.rulename <> '_RETURN'
   and n.nspname in ('test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public')
 order by n.nspname, c.relname, r.rulename"
va=$(q a "$rules_sql") || { echo "ERROR: rule query failed on a"; exit 1; }
vb=$(q b "$rules_sql") || { echo "ERROR: rule query failed on b"; exit 1; }
if [ "$va" != "$vb" ]; then
  echo "ERROR: rules (definition, comment) differ between a and b after round 1"
  diff <(printf '%s\n' "$va") <(printf '%s\n' "$vb") || true
  fail=1
else
  echo "OK: rules match between a and b ($(printf '%s\n' "$vb" | grep -c '^-- comment:') rules)"
fi

# The rules the migration drops on the way (relation recreated, dependency
# retyped or recreated) must be back, and the removed ones gone.
present=$(q a "select string_agg(c.relname || '.' || r.rulename, ' ' order by 1)
                 from pg_rewrite r join pg_class c on c.oid = r.ev_class
                where c.relnamespace = 'test_schema'::regnamespace
                  and (r.rulename like 'pg19\\_rules\\_%' or r.rulename like 'Pg19 Rules %')") \
  || { echo "ERROR: fixture rule query failed on a"; exit 1; }
for want in pg19_rules_view.pg19_rules_view_ins pg19_rules_part.pg19_rules_part_del \
            pg19_rules_log.pg19_rules_tagged pg19_rules_src.pg19_rules_qty \
            pg19_rules_src.pg19_rules_chg pg19_rules_new_view.pg19_rules_new_view_ins \
            pg19_rules_tag_view.pg19_rules_tag_view_del \
            pg19_rules_tag_txt_view.pg19_rules_tag_txt_del \
            pg19_rules_reader.pg19_rules_reads_tag_view pg19_rules_tv.pg19_rules_tv_notify \
            pg19_rules_reader.pg19_rules_ft_read pg19_rules_reader.pg19_rules_ft2_read; do
  case " $present " in
    *" $want "*) echo "OK: $want present in a" ;;
    *) echo "ERROR: rule $want missing from a"; fail=1 ;;
  esac
done
gone=$(q a "select count(*) from pg_rewrite
             where rulename in ('Pg19 Rules Gone', 'pg19_rules_to_old_audit')")
if [ "$gone" != 0 ]; then
  echo "ERROR: FROM-only fixture rules were not dropped from a"; fail=1
fi
if [ "$(q a "select to_regclass('test_schema.pg19_rules_old_audit') is null")" != "t" ]; then
  echo "ERROR: test_schema.pg19_rules_old_audit was not dropped (a rule still depended on it)"; fail=1
fi
if [ "$(q a "select format_type(atttypid, atttypmod) from pg_attribute
              where attrelid = 'test_schema.pg19_rules_src'::regclass and attname = 'qty'")" != "bigint" ]; then
  echo "ERROR: pg19_rules_src.qty was not retyped to bigint (blocked by pg19_rules_qty?)"; fail=1
fi
if [ "$(q a "select relkind from pg_class where oid = to_regclass('test_schema.pg19_rules_tv')")" != "v" ]; then
  echo "ERROR: test_schema.pg19_rules_tv was not turned into a view"; fail=1
fi
if [ "$(q a "select to_regclass('test_schema.pg19_rules_ft') is null")" != "t" ]; then
  echo "ERROR: foreign table test_schema.pg19_rules_ft was not dropped (a rule still read it)"; fail=1
fi
if [ "$(q a "select format_type(atttypid, atttypmod) from pg_attribute
              where attrelid = 'test_schema.pg19_rules_ft2'::regclass and attname = 'v'")" != "bigint" ]; then
  echo "ERROR: pg19_rules_ft2.v was not retyped to bigint (blocked by pg19_rules_ft2_read?)"; fail=1
fi
ver=$(q a "select current_setting('server_version_num')::int")
if [ "$ver" -ge 190000 ]; then
  if q a "select pg_get_ruledef(oid, true) from pg_rewrite where rulename = 'pg19_rules_upsert'" \
      | grep -q 'DO SELECT FOR UPDATE'; then
    echo "OK: pg19_rules_upsert uses ON CONFLICT DO SELECT FOR UPDATE in a"
  else
    echo "ERROR: pg19_rules_upsert was not replaced with its ON CONFLICT DO SELECT form"; fail=1
  fi
fi

# pgc must never emit anything for a view's _RETURN rule, and every rule
# statement ends in exactly one semicolon (pg_get_ruledef already ends in one;
# a rule statement runs up to the blank line after it).
if grep -nEi '(create( or replace)? rule|drop rule if exists|comment on rule) +"?_RETURN' "$out"; then
  echo "ERROR: the migration touches a view's _RETURN rule"; fail=1
fi
if ! awk 'function chk() { if (last ~ /;[[:space:]]*;[[:space:]]*$/) { print "line " lastnr ": " last; bad = 1 } }
          /^CREATE (OR REPLACE )?RULE / { inrule = 1 }
          inrule && /^[[:space:]]*$/ { chk(); inrule = 0 }
          inrule { last = $0; lastnr = NR }
          END { if (inrule) chk(); exit bad }' "$out"; then
  echo "ERROR: a rule statement ends in a doubled semicolon"; fail=1
fi
if grep -nE '^CREATE RULE (pg19_rules_chg|"Pg19 Rules Where"|pg19_rules_upsert) ' "$out"; then
  echo "ERROR: a changed rule is emitted as CREATE RULE instead of CREATE OR REPLACE RULE"; fail=1
fi
exit $fail
