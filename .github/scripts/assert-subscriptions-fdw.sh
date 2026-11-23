#!/usr/bin/env bash
# The round-2 diff cannot see a property that both dumps lose (e.g. a
# PG19 subscription SERVER or FDW CONNECTION that pgc does not read), so
# compare the catalogs of a and b directly after round 1 was applied.
PSQL="psql -h 127.0.0.1 -U postgres -X -q -tA -v ON_ERROR_STOP=1"
ver=$($PSQL -d a -c "select current_setting('server_version_num')::int")
if [ "$ver" -ge 190000 ]; then
  sub_extra="coalesce(fs.srvname, '-'), s.subretaindeadtuples, s.submaxretention, s.subwalrcvtimeout"
  sub_join="left join pg_foreign_server fs on fs.oid = s.subserver"
  fdw_extra="w.fdwconnection::regprocedure::text"
else
  sub_extra="'-'"; sub_join=""; fdw_extra="'-'"
fi
snapshot() {
  $PSQL -d "$1" -c "
    select 'sub', concat_ws(' | ', s.subname, r.rolname, s.subenabled, coalesce(s.subconninfo, '<null>'),
           s.subpublications::text, ${sub_extra})
      from pg_subscription s join pg_roles r on r.oid = s.subowner ${sub_join}
     where s.subdbid = (select oid from pg_database where datname = current_database())
    union all
    select 'fdw', concat_ws(' | ', w.fdwname, pg_get_userbyid(w.fdwowner), w.fdwhandler::regprocedure::text,
           w.fdwvalidator::regprocedure::text, coalesce(w.fdwoptions::text, '{}'), ${fdw_extra})
      from pg_foreign_data_wrapper w
     where not exists (select 1 from pg_depend d where d.classid = 'pg_foreign_data_wrapper'::regclass
                        and d.objid = w.oid and d.deptype = 'e')
    union all
    select 'srv', concat_ws(' | ', s.srvname, pg_get_userbyid(s.srvowner), w.fdwname, coalesce(s.srvtype, '<null>'),
           coalesce(s.srvversion, '<null>'), coalesce(s.srvoptions::text, '{}'))
      from pg_foreign_server s join pg_foreign_data_wrapper w on w.oid = s.srvfdw
     where not exists (select 1 from pg_depend d where d.classid = 'pg_foreign_server'::regclass
                        and d.objid = s.oid and d.deptype = 'e')
    union all
    select 'um', concat_ws(' | ', s.srvname, case when m.umuser = 0 then 'PUBLIC' else pg_get_userbyid(m.umuser) end,
           coalesce(m.umoptions::text, '{}'))
      from pg_user_mapping m join pg_foreign_server s on s.oid = m.umserver
    order by 1, 2"
}
a=$(snapshot a) || exit 1
b=$(snapshot b) || exit 1
if [ "$a" != "$b" ]; then
  echo "ERROR: subscriptions / FDWs / servers / user mappings differ between a and b after the migration (server_version_num $ver)"
  diff <(echo "$a") <(echo "$b")
  exit 1
fi
if [ "$ver" -ge 190000 ]; then
  # The fixture's PG19 cases must actually exist, or the check above is vacuous.
  for want in "sub|pg19_subfdw_sub_switch | postgres | f | <null> | {pgc_pg19_remote_pub} | pg19_subfdw_srv_b | f | 0 | -1" \
              "sub|pg19_subfdw_rdt_on | postgres | f | host=127.0.0.1 port=5432 dbname=postgres user=postgres | {pgc_pg19_remote_pub} | - | t | 0 | -1" \
              "sub|pg19_subfdw_wrt_set | postgres | f | host=127.0.0.1 port=1 dbname=pgc_nowhere | {pgc_pg19_remote_pub} | - | f | 10000 | 2min" \
              "sub|pg19_subfdw_sub_owner_switch | pgc_pg19_subfdw_owner | f | <null> | {pgc_pg19_remote_pub} | pg19_subfdw_srv_owner | f | 0 | -1" \
              "fdw|pg19_subfdw_fdw_added | postgres | postgres_fdw_handler() | postgres_fdw_validator(text[],oid) | {} | postgres_fdw_connection(oid,oid,internal)"; do
    if ! grep -qxF "$want" <<<"$a"; then
      echo "ERROR: expected catalog row missing after the migration: $want"
      echo "$a"
      exit 1
    fi
  done
  for gone in "pg19_subfdw_sub_from_only" "pg19_subfdw_srv_from_only" "pg19_subfdw_fdw_from_only"; do
    if grep -q "|${gone} " <<<"$a"; then
      echo "ERROR: ${gone} survived the migration"
      exit 1
    fi
  done
fi
echo "OK: subscriptions, FDWs, servers and user mappings of a match b (server_version_num $ver)"