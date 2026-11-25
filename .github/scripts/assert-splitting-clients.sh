#!/usr/bin/env bash
# ============================================================================
# output:splitting-clients - "Assert a migration written for SQL clients that
# split at every ';' applies the way such a client applies it"
# - standalone, any time after "Apply schemas" and "Build pgc (release)"
#   (env PGPASSWORD=postgres). Uses its own database pgc_split_a; PG14-19.
# ============================================================================
# Clients such as the VS Code PostgreSQL extension split a script at every ';'
# outside quotes, comments and dollar quotes and send the pieces one by one;
# psql knows that a ';' in a BEGIN ATOMIC body or between the actions of a
# rule does not end the statement. With --output-for-splitting-clients pgc
# runs each statement such a client would cut through a DO block.
#   1. pgc_split_a is built from schema_a.sql, as a was, and compared with b
#      twice: as usual, and with --output-for-splitting-clients true.
#   2. The two migrations differ only by the DO blocks: taking each block
#      apart gives back the usual migration byte for byte. There is at least
#      one (every supported version has the BEGIN ATOMIC fixtures).
#   3. The migration is applied as such a client applies it: opened in an
#      editor that gives the file one kind of line ending, as VS Code does
#      (so the migration may hold no carriage return, which would become a
#      line feed: pgc writes one as chr(13) inside a DO block), then split at
#      every ';' outside quotes, comments and dollar quotes, each piece sent
#      on its own in one session (psql \g), stopping at the first error. No
#      piece may hold a positional parameter ($1) outside quotes, which such
#      a client asks a value for.
#   4. A fresh compare of pgc_split_a with b is empty.
set -u
port=${PGPORT:-5432}
pgc=${PGC:-./app/target/release/pgc}
work=${SPLIT_WORK:-/tmp/pgc_split}
db=pgc_split_a
schemes='test_schema|shared_schema|new_reporting_schema|data|test_order|public'
mkdir -p "$work"
rm -f "$work"/*.sql "$work"/*.dump "$work"/*.log
P="psql -h 127.0.0.1 -p $port -U postgres -X -q"
ver=$($P -d postgres -tAc "select current_setting('server_version_num')::int")

# The fixture's subscriptions keep the database from being dropped; they
# have no slot (slot_name = NONE), so they drop without a publisher.
drop_db() {
  if [ -n "$($P -d postgres -tAc "select 1 from pg_database where datname = '${db}'")" ]; then
    $P -d "$db" -v ON_ERROR_STOP=1 > /dev/null 2>&1 <<'SQL' || true
DO $drop$
DECLARE
    sub name;
BEGIN
    FOR sub IN SELECT subname FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    LOOP
        EXECUTE format('ALTER SUBSCRIPTION %I DISABLE', sub);
        EXECUTE format('ALTER SUBSCRIPTION %I SET (slot_name = NONE)', sub);
        EXECUTE format('DROP SUBSCRIPTION %I', sub);
    END LOOP;
END
$drop$;
SQL
    $P -d postgres -c "drop database if exists ${db} with (force)" > /dev/null
  fi
}
trap drop_db EXIT
drop_db

dump() {
  "$pgc" --command dump --server 127.0.0.1 --port "$port" --user postgres \
    --password "${PGPASSWORD:-postgres}" --database "$1" --scheme "$schemes" --output "$2" \
    > "$2.log" 2>&1 || { echo "ERROR: dump of $1 failed"; cat "$2.log"; exit 1; }
}
compare() {  # compare <out> <--output-for-splitting-clients value>
  "$pgc" --command compare --from "$work/a.dump" --to "$work/b.dump" --output "$1" \
    --use-drop true --use-comments false --grants-mode full --use-single-transaction false \
    --output-for-splitting-clients "$2" > "$1.log" 2>&1 \
    || { echo "ERROR: compare failed"; cat "$1.log"; exit 1; }
}

# 1.
$P -d postgres -v ON_ERROR_STOP=1 -c "create database ${db}"
$P -d "$db" -v ON_ERROR_STOP=0 -f data/test/schema_a.sql > "$work/schema_a.log" 2>&1
dump "$db" "$work/a.dump"
dump b "$work/b.dump"
compare "$work/usual.sql" false
compare "$work/split.sql" true

# 2. and the client-style split of 3.
python3 - "$work/usual.sql" "$work/split.sql" "$work/pieces.sql" <<'PY' || exit 1
import re, sys
usual_path, split_path, pieces_path = sys.argv[1:]
usual = open(usual_path, newline='').read()
split = open(split_path, newline='').read()
block = re.compile(r'DO (\$pgc_client\d*\$)\nBEGIN\n    EXECUTE (.*?);\nEND\n\1;', re.S)
plain = re.compile(r'(\$pgc_sql\d*\$)\n(.*)\n\1', re.S)
with_cr = re.compile(r'pg_catalog\.array_to_string\(ARRAY\[(\$pgc_sql\d*\$)\n(.*)\n\1\], pg_catalog\.chr\(13\)\)', re.S)
blocks, cr_blocks = 0, 0
def statement(m):
    global blocks, cr_blocks
    blocks += 1
    text = m.group(2)
    p = plain.fullmatch(text)
    if p:
        return p.group(2) + ';'
    c = with_cr.fullmatch(text)
    if not c:
        sys.exit("ERROR: a DO block of the migration for splitting clients is not one pgc writes: " + text[:200])
    cr_blocks += 1
    return c.group(2).replace(f"{c.group(1)}, {c.group(1)}", '\r') + ';'
if block.sub(statement, split) != usual:
    sys.exit("ERROR: the migration for splitting clients differs from the usual one by more than its DO blocks")
if cr_blocks == 0:
    sys.exit("ERROR: no DO block puts back a carriage return (the fixtures have one); the check would be vacuous")
print(f"{blocks} statements run through a DO block ({cr_blocks} with a carriage return), nothing else changed")

# Open it like such an editor: every line ending becomes the file's own, LF.
# Then split like such a client: at every ';' outside quotes, comments and
# dollar quotes, ignoring parentheses and BEGIN ATOMIC.
ident = re.compile(r'[A-Za-z_0-9$\u0080-\U0010ffff]')
dollar = re.compile(r'\$(?:[A-Za-z_\u0080-\U0010ffff][A-Za-z_0-9\u0080-\U0010ffff]*)?\$')
text = re.sub(r'\r\n?', '\n', split)
n, i, start, pieces, markers = len(text), 0, 0, [], []
significant = False
while i < n:
    c = text[i]
    prev = text[i - 1] if i else ''
    if text.startswith('--', i):
        j = text.find('\n', i)
        i = n if j < 0 else j
        continue
    if text.startswith('/*', i):
        depth, i = 1, i + 2
        while i < n and depth:
            if text.startswith('/*', i): depth += 1; i += 2
            elif text.startswith('*/', i): depth -= 1; i += 2
            else: i += 1
        continue
    if c.isspace():
        i += 1
        continue
    if c == ';':
        if significant:
            pieces.append(text[start:i])
        start, significant = i + 1, False
        i += 1
        continue
    significant = True
    if c in "'\"":
        escapes = c == "'" and prev != '' and prev in 'eE' and not (i >= 2 and ident.match(text[i - 2]))
        j = i + 1
        while j < n:
            if escapes and text[j] == '\\': j += 2; continue
            if text[j] == c:
                if j + 1 < n and text[j + 1] == c: j += 2; continue
                break
            j += 1
        i = j + 1
        continue
    if c == '$' and not (prev and ident.match(prev)):
        m = dollar.match(text, i)
        if m:
            j = text.find(m.group(0), m.end())
            i = n if j < 0 else j + len(m.group(0))
            continue
        if i + 1 < n and text[i + 1].isdigit():
            markers.append(text[max(0, i - 30):i + 10])
    i += 1
if significant:
    pieces.append(text[start:])
if markers:
    sys.exit("ERROR: positional parameters outside quotes: " + " | ".join(markers))
with open(pieces_path, 'w', newline='') as f:
    for piece in pieces:
        f.write(piece + '\n\\g\n')
print(f"{len(pieces)} pieces for a client that splits at every ';'")
PY

# 3.
if ! $P -d "$db" -v ON_ERROR_STOP=1 -f "$work/pieces.sql" > "$work/apply.log" 2>&1; then
  echo "ERROR: the migration for splitting clients failed when applied piece by piece (server_version_num $ver)"
  grep -m3 -B2 ERROR "$work/apply.log"
  exit 1
fi

# 4.
dump "$db" "$work/a.dump"
compare "$work/round2.sql" false
if [ -s "$work/round2.sql" ]; then
  echo "ERROR: after the migration for splitting clients, ${db} still differs from b (server_version_num $ver)"
  head -40 "$work/round2.sql"
  exit 1
fi
echo "OK: a client that splits at every ';' applies the migration and it converges (server_version_num $ver)"
