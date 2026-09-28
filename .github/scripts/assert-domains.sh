#!/usr/bin/env bash
# CI step (fix:domains), run after "Apply diff to database a" (round 1).
# After the migration, every domain CHECK constraint (name, body, validated
# state) and every enum's label list must match between a and b. Round 2
# alone cannot prove this: pgc <= 1.0.27 lost domain-CHECK-only and
# enum-label-only changes on BOTH sides, so its round 2 was empty too.
# Env: PGPASSWORD=postgres (PGPORT optional); OUTPUT_SQL = round-1 script.
set -euo pipefail
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
PSQL="psql -h 127.0.0.1 -U postgres -X -tA -v ON_ERROR_STOP=1"
SCHEMAS="'test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public'"
ver=$($PSQL -d a -c "show server_version_num")
q_domains="select coalesce(string_agg(format('%s|%s|%s|%s', format('%I.%I', n.nspname, t.typname), c.conname, c.convalidated, pg_get_constraintdef(c.oid)), E'\n' order by n.nspname, t.typname, c.conname), '') from pg_constraint c join pg_type t on t.oid = c.contypid join pg_namespace n on n.oid = t.typnamespace where c.contypid <> 0 and n.nspname in ($SCHEMAS)"
q_enums="select coalesce(string_agg(format('%s|%s', l.name, l.labels), E'\n' order by l.name), '') from (select format('%I.%I', n.nspname, t.typname) as name, string_agg(e.enumlabel, ',' order by e.enumsortorder) as labels from pg_enum e join pg_type t on t.oid = e.enumtypid join pg_namespace n on n.oid = t.typnamespace where n.nspname in ($SCHEMAS) group by 1) l"
fail=0
for check in domains enums; do
  q_var="q_$check"; q=${!q_var}
  in_a=$($PSQL -d a -c "$q"); in_b=$($PSQL -d b -c "$q")
  if [ "$in_a" != "$in_b" ]; then
    echo "::error::domains ($check): database a differs from b after applying the migration"
    diff <(echo "$in_a") <(echo "$in_b") || true
    fail=1
  fi
done
# The fixture must really have been migrated (a == b also holds when a change
# is lost on both sides), and through the intended statements. Production
# mode guards the drops with IF EXISTS; match either form.
sql=$(sed -e 's/ drop constraint if exists / drop constraint /' -e 's/ drop column if exists / drop column /' "$OUT")
expect() {  # expect <exact round-1 text>
  if ! grep -qF -- "$1" <<<"$sql"; then echo "::error::domains: round-1 SQL lacks: $1"; fail=1; fi
}
# No match must not abort the script under `set -e -o pipefail`: `before`
# reports a missing anchor itself.
line_of() { { grep -nF -- "$1" <<<"$sql" || true; } | head -n 1 | cut -d: -f1; }
before() {  # before <earlier text> <later text>: both present, in this order
  local first second
  first=$(line_of "$1"); second=$(line_of "$2")
  if [ -z "$first" ] || [ -z "$second" ] || [ "$first" -ge "$second" ]; then
    echo "::error::domains: round-1 SQL must run '$1' (line ${first:-missing}) before '$2' (line ${second:-missing})"; fail=1
  fi
}
body_add='alter domain test_schema.pg19_domains_body add constraint "pg19_domains_body_range" CHECK ((VALUE > 5));'
validate='alter domain test_schema.pg19_domains_validate validate constraint "pg19_domains_validate_nonneg";'
sku_add='alter domain test_schema.pg19_domains_sku add constraint "pg19_domains_sku_fmt" CHECK (test_schema.pg19_domains_is_sku(VALUE));'
add_medium="alter type test_schema.pg19_domains_level add value if not exists 'medium' before 'high';"
expect 'alter domain test_schema.pg19_domains_body drop constraint "pg19_domains_body_range";'
expect "$body_add"
expect 'alter domain test_schema.pg19_domains_gain add constraint "pg19_domains Gain ""q""" CHECK '
expect 'alter domain test_schema.pg19_domains_lose drop constraint "pg19_domains_lose_len";'
expect "$validate"
expect 'alter domain test_schema.pg19_domains_invalidate add constraint "pg19_domains_invalidate_nonneg" CHECK ((VALUE >= 0)) NOT VALID;'
expect "$sku_add"
expect "$add_medium"
if [ "$ver" -ge 190000 ]; then
  expect 'alter domain test_schema.pg19_domains_json_doc add constraint "pg19_domains_json_doc_shape" CHECK ((VALUE IS JSON OBJECT));'
fi
# Adding / validating a CHECK scans every stored value of the domain, and the
# CHECK may call a routine the migration creates: it has to come after the
# table and column holding values only the old CHECK allowed are dropped,
# and after the routine it calls exists. The old CHECK goes before the old
# routine it calls is dropped.
before 'drop table if exists test_schema.pg19_domains_legacy;' "$body_add"
before 'drop table if exists test_schema.pg19_domains_legacy;' "$validate"
before 'alter table test_schema.pg19_domains_usage drop column old_qty;' "$body_add"
before 'function test_schema.pg19_domains_is_sku(v text) returns boolean' "$sku_add"
before 'alter domain test_schema.pg19_domains_sku drop constraint "pg19_domains_sku_fmt";' 'drop function if exists test_schema.pg19_domains_old_sku'
# In a single transaction a new enum value is unusable until it commits, so
# it is added before BEGIN.
if grep -qx 'begin;' <<<"$sql"; then
  before "$add_medium" 'begin;'
fi
# The unchanged domain stays out of the diff, and no changed domain falls
# through to the "no supported changes" note.
if grep -qF 'pg19_domains_same' "$OUT"; then
  echo "::error::domains: unchanged domain test_schema.pg19_domains_same appears in the round-1 SQL"; fail=1
fi
# That note is a `--` comment, which CI's USE_COMMENTS=false strips from the
# round-1 script, so compare the same round-1 dumps again with comments on.
dump_dir="${DUMP_DIR:-$(dirname "$OUT")}"
pgc="${PGC:-./app/target/release/pgc}"
noted=$(mktemp)
if "$pgc" --command compare --from "$dump_dir/a.dump" --to "$dump_dir/b.dump" \
     --use-drop --use-comments true --grants-mode full --output "$noted" > /dev/null 2>&1; then
  if grep -qE 'Domain test_schema\.pg19_domains_[a-z_]+ requires no supported changes' "$noted"; then
    echo "::error::domains: a changed domain fell through to 'requires no supported changes'"; fail=1
  fi
else
  echo "::error::domains: could not re-run the compare with comments on ($dump_dir/a.dump, $dump_dir/b.dump)"; fail=1
fi
rm -f "$noted"
# Spot checks that the fixture values reached a (not merely a == b).
check() {  # check <label> <sql> <expected>
  got=$($PSQL -d a -c "$2" 2>&1 || true)
  if [ "$got" != "$3" ]; then echo "::error::domains: $1: expected '$3', got '$got'"; fail=1; fi
}
check "changed CHECK body" "select pg_get_constraintdef(oid) from pg_constraint where conname = 'pg19_domains_body_range' and contypid = 'test_schema.pg19_domains_body'::regtype" 'CHECK ((VALUE > 5))'
check "validated constraint" "select convalidated from pg_constraint where conname = 'pg19_domains_validate_nonneg' and contypid = 'test_schema.pg19_domains_validate'::regtype" 't'
check "constraint made NOT VALID" "select convalidated from pg_constraint where conname = 'pg19_domains_invalidate_nonneg' and contypid = 'test_schema.pg19_domains_invalidate'::regtype" 'f'
check "removed constraint" "select count(*) from pg_constraint where contypid = 'test_schema.pg19_domains_lose'::regtype" '0'
check "CHECK calling a new routine" "select pg_get_constraintdef(oid) from pg_constraint where conname = 'pg19_domains_sku_fmt' and contypid = 'test_schema.pg19_domains_sku'::regtype" 'CHECK (test_schema.pg19_domains_is_sku(VALUE))'
check "dropped table" "select to_regclass('test_schema.pg19_domains_legacy') is null" 't'
check "dropped column" "select count(*) from pg_attribute where attrelid = 'test_schema.pg19_domains_usage'::regclass and attname = 'old_qty' and not attisdropped" '0'
check "inserted enum label" "select string_agg(enumlabel, ',' order by enumsortorder) from pg_enum where enumtypid = 'test_schema.pg19_domains_level'::regtype" 'low,medium,high'
check "default using the new enum label" "select pg_get_expr(d.adbin, d.adrelid) from pg_attrdef d join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum where d.adrelid = 'test_schema.pg19_domains_usage'::regclass and a.attname = 'level'" "'medium'::test_schema.pg19_domains_level"
exit $fail
