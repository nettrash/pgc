#!/usr/bin/env bash
# fix:replident — after applying round 1, every table of `a` must have the
# same replica identity as in `b`, including WHICH index a USING INDEX table
# uses. The round-2 diff alone cannot prove that: pgc 1.0.x kept only
# relreplident, so a lost or wrong index compared equal on both sides.
# Also checks the statement order PostgreSQL requires: leave USING INDEX
# before a key column's DROP NOT NULL, set it only after the index and its
# validated NOT NULLs exist (for a partition, after the parent index it comes
# from), and never leave an index the migration does not touch.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). TEXT_CHECKS=0 runs only the catalog check.
set -u
out="${OUTPUT_SQL:-/tmp/output.sql}"
text_checks="${TEXT_CHECKS:-1}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
ver=$(q a "show server_version_num")
fail=0
# A table left on USING INDEX without its index counts as NOTHING, as pgc
# dumps it. Only non-default tables and the fixture's own are listed, so a
# table some other fixture fails to create on this version does not count.
ri_sql="
  select format('%s.%s|%s|%s', n.nspname, c.relname,
                case when c.relreplident = 'i' and ri.relname is null then 'n'
                     else c.relreplident::text end,
                coalesce(ri.relname, '-'))
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  left join lateral (
    select ic.relname from pg_index x join pg_class ic on ic.oid = x.indexrelid
    where x.indrelid = c.oid and x.indisreplident
  ) ri on true
  where c.relkind in ('r', 'p')
    and n.nspname in ('test_schema', 'shared_schema', 'new_reporting_schema', 'data', 'test_order', 'public')
    and (c.relreplident <> 'd' or c.relname like 'pg19\_replident%')
  order by 1"
ra=$(q a "$ri_sql") || { echo "ERROR: replica identity query failed on a"; exit 1; }
rb=$(q b "$ri_sql") || { echo "ERROR: replica identity query failed on b"; exit 1; }
if [ "$ra" != "$rb" ]; then
  echo "ERROR: replica identity (relreplident + indisreplident index) differs between a and b (server_version_num $ver)"
  diff <(printf '%s\n' "$ra") <(printf '%s\n' "$rb") || true
  fail=1
fi
# Every USING INDEX table of the fixture has to be there at all.
for t in enter switch rebuild ukey ukey_rebuild pkey orphan notvalid part genrewrite new new_ukey \
         unrel pp_p1 pp_p2 pkpart_k1 pkpart_k2; do
  if ! printf '%s\n' "$rb" | grep -q "^test_schema\.pg19_replident_$t|i|"; then
    echo "ERROR: fixture table test_schema.pg19_replident_$t is not on USING INDEX in b"
    fail=1
  fi
done
if [ "$text_checks" = 1 ]; then
  line_of() { grep -n -i -- "$1" "$out" | head -n 1 | cut -d: -f1; }
  last_line_of() { grep -n -i -- "$1" "$out" | tail -n 1 | cut -d: -f1; }
  # USING INDEX -> DEFAULT with DROP NOT NULL on the key column: leave first.
  reset=$(line_of "alter table test_schema.pg19_replident_leave replica identity default;")
  dropnn=$(line_of "alter table test_schema.pg19_replident_leave alter column code drop not null;")
  if [ -z "$reset" ] || [ -z "$dropnn" ] || [ "$reset" -ge "$dropnn" ]; then
    echo "ERROR: pg19_replident_leave must leave USING INDEX (line ${reset:-none}) before DROP NOT NULL (line ${dropnn:-none})"
    fail=1
  fi
  # NOT VALID (or missing) NOT NULL -> validated + USING INDEX: the NOT NULL
  # change has to come first (PG19 and 18.7+ refuse USING INDEX otherwise).
  nn=$(last_line_of "pg19_replident_notvalid .*not null")
  using=$(line_of "alter table test_schema.pg19_replident_notvalid replica identity using index pg19_replident_notvalid_code_uk;")
  if [ -z "$nn" ] || [ -z "$using" ] || [ "$nn" -ge "$using" ]; then
    echo "ERROR: pg19_replident_notvalid USING INDEX (line ${using:-none}) must follow its NOT NULL change (line ${nn:-none})"
    fail=1
  fi
  # The rebuilt index is created before USING INDEX names it again.
  create=$(last_line_of "index .*pg19_replident_rebuild_uk on ")
  using=$(last_line_of "alter table test_schema.pg19_replident_rebuild replica identity using index pg19_replident_rebuild_uk;")
  if [ -z "$create" ] || [ -z "$using" ] || [ "$create" -ge "$using" ]; then
    echo "ERROR: pg19_replident_rebuild USING INDEX (line ${using:-none}) must follow the index rebuild (line ${create:-none})"
    fail=1
  fi
  # A new partition is created before its parent is altered: USING INDEX on
  # the index PostgreSQL creates for it from the parent has to follow the
  # parent's CREATE INDEX / ADD PRIMARY KEY.
  for pair in "pp_p2|index .*pg19_replident_ppk_idx on " "pkpart_k2|pg19_replident_pkpart_pkey primary key"; do
    t=${pair%%|*}; parent=${pair#*|}
    create=$(line_of "$parent")
    using=$(line_of "alter table test_schema.pg19_replident_$t replica identity using index")
    if [ -z "$create" ] || [ -z "$using" ] || [ "$create" -ge "$using" ]; then
      echo "ERROR: pg19_replident_$t USING INDEX (line ${using:-none}) must follow its parent's index (line ${create:-none})"
      fail=1
    fi
  done
  # An index the alter does not touch keeps the table on it throughout.
  if grep -q -i "alter table test_schema.pg19_replident_unrel replica identity" "$out"; then
    echo "ERROR: pg19_replident_unrel must stay on USING INDEX (its index is not on the recreated column)"
    fail=1
  fi
fi
[ "$fail" -eq 0 ] && echo "fix:replident assertions passed (server_version_num $ver)"
exit "$fail"
