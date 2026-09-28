#!/usr/bin/env bash
# ============================================================================
# reg:mixed - "Assert dumps of different pgc versions are refused or guarded"
# - standalone, any time after "Build pgc (release)" (env PGPASSWORD=postgres).
# Uses its own databases pgc_mixed_a / pgc_mixed_b; works on PG14-19, the
# standard_conforming_strings = off part only on PG14-18.
# ============================================================================
# CI has no pgc 1.0.x binary, so a dump "written by pgc 1.0.x" is a dump
# written by this pgc with what 1.0.x could not write taken out: the three
# top-level keys pgc 1.1 added (server_version_num, which is how pgc tells
# the two apart, standard_conforming_strings_off and available_extensions),
# and the tables 1.0.x's `tablename not like 'pg_%'` filter left out (every
# name of three or more characters starting with pg).
#   1. FROM by pgc 1.1, TO by pgc 1.0.x: refused, non-zero exit, no script.
#   2. FROM by pgc 1.0.x, TO by pgc 1.1: compared, with a warning on stderr
#      naming the table 1.0.x could not see, a comment at the top of the
#      script, and set standard_conforming_strings = on; first. Applied to a
#      database whose clients default to off, the default keeps its backslash.
#   3. Both by pgc 1.0.x: no guard, no warning (unchanged from pgc 1.0.x).
#   4. A fresh pgc 1.1 dump of the migrated database compares empty.
#   5. Subscriptions: pgc 1.0.x dumped those of every database in the cluster,
#      so its FROM dump cannot say whether FROM has a TO subscription of the
#      same name. One that only another database has is still created (and
#      named on stderr), not taken as present.
set -u
port=${PGPORT:-5432}
pgc=${PGC:-./app/target/release/pgc}
work=${MIXED_WORK:-/tmp/pgc_mixed}
mkdir -p "$work"
rm -f "$work"/*.sql "$work"/*.dump "$work"/*.log "$work"/*.err
P="psql -h 127.0.0.1 -p $port -U postgres -X -v ON_ERROR_STOP=1 -q"
ver=$($P -d postgres -tAc "select current_setting('server_version_num')::int")
fail=0
# A database holding a subscription cannot be dropped, even with (force).
drop_sub() {
  if [ -n "$($P -d postgres -tAc "select 1 from pg_database where datname = 'pgc_mixed_b'")" ]; then
    psql -h 127.0.0.1 -p "$port" -U postgres -X -q -d pgc_mixed_b \
      -c "alter subscription pgc_mixed_sub set (slot_name = none)" \
      -c "drop subscription pgc_mixed_sub" > /dev/null 2>&1 || true
  fi
}
trap drop_sub EXIT
drop_sub
for db in pgc_mixed_a pgc_mixed_b; do
  $P -d postgres -c "drop database if exists ${db} with (force)" -c "create database ${db}"
  # PostgreSQL 19 cannot turn the setting off.
  if [ "$ver" -lt 190000 ]; then
    $P -d postgres -c "alter database ${db} set standard_conforming_strings = off"
  fi
done
$P -d pgc_mixed_a -c "create table public.mixed_probe (id integer primary key)" \
  -c "create table public.pgmixed_hidden (id integer primary key)"
$P -d pgc_mixed_b -c "create table public.mixed_probe (id integer primary key, path text default E'C:\\\\temp')" \
  -c "create table public.pgmixed_hidden (id integer primary key)"

dump() {
  "$pgc" --command dump --server 127.0.0.1 --port "$port" --user postgres \
    --password "${PGPASSWORD:-postgres}" --database "$1" --scheme public --output "$2" \
    > "$2.log" 2>&1 || { echo "ERROR: dump of $1 failed"; cat "$2.log"; exit 1; }
}
legacy() {
  python3 - "$1" "$2" <<'PY'
import json, sys, zipfile
src, dst = sys.argv[1:3]
with zipfile.ZipFile(src) as z:
    d = json.loads(z.read("dump.io"))
for key in ("server_version_num", "standard_conforming_strings_off", "available_extensions"):
    d.pop(key, None)
d["tables"] = [t for t in d["tables"]
               if not (t["raw_name"].startswith("pg") and len(t["raw_name"]) >= 3)]
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("dump.io", json.dumps(d))
PY
}
compare() {
  "$pgc" --command compare --from "$1" --to "$2" --output "$3" --use-drop true \
    --use-comments "$4" > "$3.log" 2> "$3.err"
}
dump pgc_mixed_a "$work/a.dump"
dump pgc_mixed_b "$work/b.dump"
legacy "$work/a.dump" "$work/a_old.dump"
legacy "$work/b.dump" "$work/b_old.dump"

# 1. A pgc 1.0.x TO dump against a pgc 1.1 FROM dump.
if compare "$work/a.dump" "$work/b_old.dump" "$work/refused.sql" false; then
  echo "ERROR: a pgc 1.0.x TO dump was compared against a pgc 1.1 FROM dump"; fail=1
fi
if [ -e "$work/refused.sql" ]; then
  echo "ERROR: the refused compare wrote a script:"; cat "$work/refused.sql"; fail=1
fi
grep -qF "Re-dump 'to' with pgc 1.1 or later" "$work/refused.sql.err" \
  || { echo "ERROR: the refusal does not say to re-dump TO:"; cat "$work/refused.sql.err"; fail=1; }

# 2. A pgc 1.0.x FROM dump against a pgc 1.1 TO dump.
compare "$work/a_old.dump" "$work/b.dump" "$work/mixed.sql" true \
  || { echo "ERROR: the compare of a pgc 1.0.x FROM dump failed"; cat "$work/mixed.sql.err"; fail=1; }
grep -qF "Warning: the 'from' dump was written by pgc 1.0.x" "$work/mixed.sql.err" \
  || { echo "ERROR: no stderr warning:"; cat "$work/mixed.sql.err"; fail=1; }
grep -qF "public.pgmixed_hidden" "$work/mixed.sql.err" \
  || { echo "ERROR: the warning does not name public.pgmixed_hidden:"; cat "$work/mixed.sql.err"; fail=1; }
grep -qF "/* Warning: the FROM dump was written by pgc 1.0.x" "$work/mixed.sql" \
  || { echo "ERROR: no warning comment in the script"; fail=1; }
compare "$work/a_old.dump" "$work/b.dump" "$work/mixed_nc.sql" false \
  || { echo "ERROR: the compare of a pgc 1.0.x FROM dump failed"; fail=1; }
head -1 "$work/mixed_nc.sql" | grep -qx 'set standard_conforming_strings = on;' \
  || { echo "ERROR: the migration does not start with the guard:"; cat "$work/mixed_nc.sql"; fail=1; }
# Applied the way CI applies every migration. CREATE TABLE of pgmixed_hidden
# fails ('already exists'), as the warning says; everything else must apply.
$P -d pgc_mixed_a -v ON_ERROR_STOP=0 -f "$work/mixed_nc.sql" > "$work/apply.log" 2>&1
grep -F ERROR "$work/apply.log" | grep -vF 'relation "pgmixed_hidden" already exists' \
  && { echo "ERROR: the migration did not apply cleanly"; fail=1; }
v=$($P -d pgc_mixed_a -tAc "insert into public.mixed_probe (id) values (1) returning length(path) || ':' || (path = E'C:\\\\temp')")
[ "$v" = "7:true" ] \
  || { echo "ERROR: the default stored length:equal = '${v}', expected 7:true (PG ${ver})"; fail=1; }

# 3. Two pgc 1.0.x dumps compare as pgc 1.0.x did.
compare "$work/a_old.dump" "$work/b_old.dump" "$work/legacy.sql" true \
  || { echo "ERROR: the compare of two pgc 1.0.x dumps failed"; fail=1; }
if grep -qF "standard_conforming_strings" "$work/legacy.sql" \
   || grep -qF "pgc 1.0.x" "$work/legacy.sql" "$work/legacy.sql.err"; then
  echo "ERROR: two pgc 1.0.x dumps got the guard or the warning"; fail=1
fi

# 4. The migrated database now matches.
dump pgc_mixed_a "$work/a2.dump"
compare "$work/a2.dump" "$work/b.dump" "$work/round2.sql" false \
  || { echo "ERROR: the second compare failed"; fail=1; }
if [ -n "$(tr -d '[:space:]' < "$work/round2.sql")" ]; then
  echo "ERROR: second compare not empty:"; cat "$work/round2.sql"; fail=1
fi

# 5. A subscription only another database on the FROM cluster has. The
#    connect = false subscription never contacts its (unreachable) publisher.
$P -d pgc_mixed_b -c "create subscription pgc_mixed_sub connection 'host=127.0.0.1 port=1 dbname=nowhere' publication pgc_mixed_pub with (connect = false)" \
  > "$work/sub.log" 2>&1 || { echo "ERROR: could not create the subscription"; cat "$work/sub.log"; fail=1; }
dump pgc_mixed_b "$work/b_sub.dump"
# What pgc 1.0.x wrote for pgc_mixed_a when another database had pgc_mixed_sub.
python3 - "$work/a_old.dump" "$work/b_sub.dump" "$work/a_old_sub.dump" <<'PY'
import json, sys, zipfile
src, other, dst = sys.argv[1:4]
def read(path):
    with zipfile.ZipFile(path) as z:
        return json.loads(z.read("dump.io"))
d = read(src)
d["subscriptions"] = read(other)["subscriptions"]
with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("dump.io", json.dumps(d))
PY
compare "$work/a_old_sub.dump" "$work/b_sub.dump" "$work/sub.sql" false \
  || { echo "ERROR: the compare with subscriptions failed"; cat "$work/sub.sql.err"; fail=1; }
[ "$(grep -c '^CREATE SUBSCRIPTION pgc_mixed_sub ' "$work/sub.sql")" = 1 ] \
  || { echo "ERROR: pgc_mixed_sub is not created exactly once:"; cat "$work/sub.sql"; fail=1; }
grep -qF "'from' may already have them: pgc_mixed_sub." "$work/sub.sql.err" \
  || { echo "ERROR: the warning does not name pgc_mixed_sub:"; cat "$work/sub.sql.err"; fail=1; }
drop_sub

for db in pgc_mixed_a pgc_mixed_b; do
  $P -d postgres -c "drop database if exists ${db} with (force)"
done
[ "$fail" -eq 0 ] || exit 1
echo "OK: dumps of different pgc versions are refused or guarded (PG ${ver})"
