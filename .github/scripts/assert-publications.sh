#!/usr/bin/env bash
# Step "Assert publication catalogs match" — run after "Apply diff to database `a`".
# Round 2 cannot see a publication property that both dumps lose, so compare the
# catalogs of a and b directly. Columns that older servers lack are gated on
# server_version_num (PG15: prattrs/prqual/pg_publication_namespace, PG18: pubgencols,
# PG19: puballsequences/prexcept). One row per pg_publication_rel entry, so an
# inheritance child added by a recursing SET TABLE shows up as an extra row.
set -u
PSQL="psql -h 127.0.0.1 -U postgres -X -v ON_ERROR_STOP=1 -tA"
ver=$($PSQL -d a -c "show server_version_num")
seq_col=false; except_col=false; gencols_col="'n'"; rel_spec="''"
ns_q="select 'no TABLES IN SCHEMA before PG15'"
if [ "$ver" -ge 150000 ]; then
  rel_spec="coalesce((select string_agg(att.attname, ',' order by att.attname) from pg_attribute att where att.attrelid = pr.prrelid and att.attnum = any (pr.prattrs)), '*') || ' where ' || coalesce(pg_get_expr(pr.prqual, pr.prrelid), '-')"
  ns_q="select coalesce(string_agg(p.pubname || ':' || n.nspname, E'\n' order by p.pubname, n.nspname), '') from pg_publication_namespace pn join pg_publication p on p.oid = pn.pnpubid join pg_namespace n on n.oid = pn.pnnspid"
fi
[ "$ver" -ge 180000 ] && gencols_col="p.pubgencols"
if [ "$ver" -ge 190000 ]; then seq_col="p.puballsequences"; except_col="pr.prexcept"; fi
pub_q="select coalesce(string_agg(concat_ws('|', p.pubname, p.puballtables, $seq_col, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, $gencols_col, pg_get_userbyid(p.pubowner), obj_description(p.oid, 'pg_publication')), E'\n' order by p.pubname), '') from pg_publication p"
rel_q="select coalesce(string_agg(concat_ws('|', p.pubname, pr.prrelid::regclass::text, $except_col, $rel_spec), E'\n' order by p.pubname, pr.prrelid::regclass::text), '') from pg_publication_rel pr join pg_publication p on p.oid = pr.prpubid"
status=0
for what in publications:pub_q members:rel_q schemas:ns_q; do
  name=${what%%:*}; var=${what#*:}; q=${!var}
  qa=$($PSQL -d a -c "$q") || { echo "::error::query failed on a: $q"; exit 1; }
  qb=$($PSQL -d b -c "$q") || { echo "::error::query failed on b: $q"; exit 1; }
  if ! diff <(echo "$qa") <(echo "$qb"); then
    echo "::error::publication $name differ between a and b after the migration (server_version_num $ver)"
    status=1
  fi
done
[ "$status" -eq 0 ] && echo "publication catalogs of a and b match (server_version_num $ver)"
exit $status