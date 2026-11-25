#!/usr/bin/env bash
# ============================================================================
# r4-sub - "Assert subscription replication slots match, and the migration
# creates subscriptions without contacting their publisher"
# - after round 1 of the fixture round trip was applied to a (env
#   PGPASSWORD=postgres; reads the round-1 SQL from OUTPUT_SQL).
# ============================================================================
#   1. The slot (pg_subscription.subslotname) and enabled state of every
#      subscription of a match b. pgc 1.0.x did not read the slot, so both
#      dumps lost it alike and the round-2 compare could not see a difference.
#   2. The rv_r4s fixture cases arrived: three slot changes on existing
#      subscriptions, and the TO-only subscriptions — which exist in a only
#      because the migration created them WITH (connect = false, ...), their
#      publisher being unreachable — with their slot and comment. On PG19+
#      also the TO-only SERVER subscription with its PG19 options.
#   3. The round-1 SQL creates every (disabled) fixture subscription with
#      connect = false, and no replication slot of the fixture exists.
#   4. A TO slot PostgreSQL refuses as a slot_name is never written:
#      "rv_r4s_Sub_Upper" (its own slot, upper case) and
#      rv_r4s_sub_none_alias (a slot named none, which slot_name = 'none'
#      would turn into NONE) keep FROM's slot in a, the only rows step 1
#      leaves out, and every slot_name the round-1 SQL writes is a valid one.
set -u
port=${PGPORT:-5432}
out=${OUTPUT_SQL:-/tmp/output.sql}
P="psql -h 127.0.0.1 -p $port -U postgres -X -q -tA -v ON_ERROR_STOP=1"
ver=$($P -d a -c "select current_setting('server_version_num')::int") || exit 1
fail=0

# ---- 1. a == b -------------------------------------------------------------
# The two subscriptions whose TO slot PostgreSQL refuses are checked in step 4.
refused="'rv_r4s_Sub_Upper', 'rv_r4s_sub_none_alias'"
snapshot() {
  $P -d "$1" -c "
    select concat_ws(' | ', s.subname, s.subenabled, coalesce(s.subslotname::text, '<none>'),
           coalesce(d.description, '<no comment>'))
      from pg_subscription s
      left join pg_description d on d.objoid = s.oid
           and d.classoid = 'pg_subscription'::regclass and d.objsubid = 0
     where s.subdbid = (select oid from pg_database where datname = current_database())
       and s.subname $2 in ($refused)
     order by s.subname"
}
a=$(snapshot a not) || exit 1
b=$(snapshot b not) || exit 1
if [ "$a" != "$b" ]; then
  echo "ERROR: subscription slots / enabled states differ between a and b after the migration (server_version_num $ver)"
  diff <(echo "$a") <(echo "$b")
  fail=1
fi

# ---- 2. the fixture cases are there ----------------------------------------
for want in "rv_r4s_sub_own_to_none | f | <none> | <no comment>" \
            "rv_r4s_sub_none_to_named | f | rv_r4s_named_slot | <no comment>" \
            "rv_r4s_sub_named_to_own | f | rv_r4s_sub_named_to_own | <no comment>" \
            "rv_r4s_sub_same_named | f | rv_r4s_kept_slot | <no comment>" \
            "rv_r4s_sub_to_only_none | f | <none> | <no comment>" \
            "rv_r4s_sub_to_only_own | f | rv_r4s_sub_to_only_own | <no comment>" \
            "rv_r4s_sub_to_only_named | f | rv_r4s_other_slot | rv_r4s TO-only subscription" \
            "rv_r4s_Sub_To_Only_Upper | f | rv_r4s_Sub_To_Only_Upper | <no comment>"; do
  if ! grep -qxF "$want" <<<"$a"; then
    echo "ERROR: expected subscription row missing from a after the migration: $want"
    fail=1
  fi
done
if [ "$ver" -ge 190000 ]; then
  pg19=$($P -d a -c "
    select concat_ws(' | ', s.subname, fs.srvname, s.subenabled, coalesce(s.subslotname::text, '<none>'),
           s.subretaindeadtuples, s.submaxretention, s.subwalrcvtimeout)
      from pg_subscription s join pg_foreign_server fs on fs.oid = s.subserver
     where s.subdbid = (select oid from pg_database where datname = current_database())
       and s.subname = 'rv_r4s_sub_to_only_server'") || exit 1
  if [ "$pg19" != "rv_r4s_sub_to_only_server | pg19_subfdw_srv_a | f | <none> | t | 1000 | 30s" ]; then
    echo "ERROR: rv_r4s_sub_to_only_server was not created as TO has it: '$pg19'"
    fail=1
  fi
fi

# ---- 3. nothing contacted a publisher --------------------------------------
if [ -f "$out" ]; then
  expected=4
  [ "$ver" -ge 190000 ] && expected=5
  created=$(grep -c '^CREATE SUBSCRIPTION "\?rv_r4s_' "$out")
  if [ "$created" -ne "$expected" ]; then
    echo "ERROR: the migration creates $created rv_r4s subscriptions, expected $expected"
    fail=1
  fi
  # Every fixture subscription is disabled, so none may be created connected.
  connected=$(grep "^CREATE SUBSCRIPTION " "$out" | grep -v " WITH (connect = false, ")
  if [ -n "$connected" ]; then
    echo "ERROR: a disabled subscription is created without connect = false:"
    echo "$connected"
    fail=1
  fi
  for set in "ALTER SUBSCRIPTION rv_r4s_sub_own_to_none SET (slot_name = NONE);" \
             "ALTER SUBSCRIPTION rv_r4s_sub_none_to_named SET (slot_name = 'rv_r4s_named_slot');" \
             "ALTER SUBSCRIPTION rv_r4s_sub_named_to_own SET (slot_name = 'rv_r4s_sub_named_to_own');"; do
    if ! grep -qxF "$set" "$out"; then
      echo "ERROR: the migration lacks: $set"
      fail=1
    fi
  done
else
  echo "ERROR: round-1 migration $out not found"
  fail=1
fi
slots=$($P -d postgres -c "select string_agg(slot_name, ', ') from pg_replication_slots where slot_name like 'rv\_r4s\_%'") || exit 1
if [ -n "$slots" ]; then
  echo "ERROR: replication slots of the fixture exist on the server: $slots"
  fail=1
fi

# ---- 4. slots PostgreSQL refuses are left out ------------------------------
kept_a=$(snapshot a "") || exit 1
kept_b=$(snapshot b "") || exit 1
want_a="rv_r4s_Sub_Upper | f | rv_r4s_upper_slot | <no comment>
rv_r4s_sub_none_alias | f | <none> | <no comment>"
want_b="rv_r4s_Sub_Upper | f | rv_r4s_Sub_Upper | <no comment>
rv_r4s_sub_none_alias | f | none | <no comment>"
if [ "$(sort <<<"$kept_a")" != "$(sort <<<"$want_a")" ]; then
  echo "ERROR: a subscription whose TO slot PostgreSQL refuses did not keep its FROM slot in a:"
  echo "$kept_a"
  fail=1
fi
if [ "$(sort <<<"$kept_b")" != "$(sort <<<"$want_b")" ]; then
  echo "ERROR: b does not hold the refused slots the fixture creates:"
  echo "$kept_b"
  fail=1
fi
if [ -f "$out" ]; then
  if grep -qE '^ALTER SUBSCRIPTION ("rv_r4s_Sub_Upper"|rv_r4s_sub_none_alias) ' "$out"; then
    echo "ERROR: the migration alters a subscription whose TO slot PostgreSQL refuses:"
    grep -E '^ALTER SUBSCRIPTION ("rv_r4s_Sub_Upper"|rv_r4s_sub_none_alias) ' "$out"
    fail=1
  fi
  bad=$(grep -o "slot_name = '[^']*'" "$out" | sed "s/^slot_name = '//; s/'$//" \
        | grep -vxE '[a-z0-9_]{1,63}' ; grep -o "slot_name = 'none'" "$out")
  if [ -n "$bad" ]; then
    echo "ERROR: the migration writes a slot_name PostgreSQL refuses: $bad"
    fail=1
  fi
fi

[ "$fail" -eq 0 ] && echo "OK: subscription slots of a match b, created without contacting a publisher (server_version_num $ver)"
exit "$fail"
