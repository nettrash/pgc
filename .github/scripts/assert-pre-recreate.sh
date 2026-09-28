#!/usr/bin/env bash
# ============================================================================
# pre-recreate - "Assert tables dropped and recreated come back whole" - run
# after "Apply diff to database `a`" (env PGPASSWORD=postgres, PGPORT set).
# Works on PG14-19: every fixture in this group runs on all of them.
# ============================================================================
# The rv_recreate_* fixtures (test_schema, and one table in public) drop and
# recreate tables: partition-key changes at the top and on a sub-partition,
# and plain tables TO partitions. After round 1 is applied, `a` must match
# `b` in everything that went with those DROP TABLEs:
# the partition tree (also the partitions whose own definition is unchanged),
# the serial sequences and the columns they are OWNED BY, the foreign keys
# the tables hold and the ones held against them, and the indexes, triggers,
# statistics, grants, comments and view of the unchanged partition; also
# the plain table TO attaches to a recreated table, the partition's own
# trigger named like its parent's statement trigger, a foreign key into
# public from test_schema, one referencing the parent of a recreated
# sub-partition, and mixed-case names. Round 2
# alone cannot see a partition, sequence or foreign key missing from both
# the migrated `a` and a re-dump of it; these queries read the catalogs.
set -u
fail=0
OUT="${OUTPUT_SQL:-/tmp/output.sql}"
q() { psql -h 127.0.0.1 -U postgres -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
same() {
  local name=$1 sql=$2 va vb
  va=$(q a "$sql") || { echo "ERROR: query for ${name} failed on a"; fail=1; return; }
  vb=$(q b "$sql") || { echo "ERROR: query for ${name} failed on b"; fail=1; return; }
  if [ "$va" != "$vb" ]; then
    echo "ERROR: ${name} differs between a and b"
    diff <(printf '%s\n' "$va") <(printf '%s\n' "$vb") || true
    fail=1
  else
    echo "OK: ${name} ($(printf '%s' "$va" | grep -c '') rows)"
  fi
}
rel="n.nspname in ('test_schema', 'public') and c.relname like 'rv\_recreate\_%'"

same "rv_recreate partition tree (kind, parent, bound, partition key)" \
  "select c.relname, c.relkind, coalesce(i.inhparent::regclass::text, '-'),
          coalesce(pg_get_expr(c.relpartbound, c.oid), '-'),
          case when c.relkind = 'p' then pg_get_partkeydef(c.oid) else '-' end
     from pg_class c join pg_namespace n on n.oid = c.relnamespace
     left join pg_inherits i on i.inhrelid = c.oid
    where ${rel} and c.relkind in ('r', 'p') order by 1"
same "rv_recreate sequences (type, options, OWNED BY, privileges)" \
  "select c.relname, format_type(s.seqtypid, null), s.seqstart, s.seqincrement, s.seqmin, s.seqmax,
          s.seqcache, s.seqcycle, coalesce(c.relacl::text, '-'),
          coalesce((select d.deptype::text || ':' || d.refobjid::regclass::text || '.' || quote_ident(a.attname)
                      from pg_depend d
                      join pg_attribute a on a.attrelid = d.refobjid and a.attnum = d.refobjsubid
                     where d.classid = 'pg_class'::regclass and d.objid = c.oid
                       and d.refclassid = 'pg_class'::regclass and d.deptype in ('a', 'i')), '-')
     from pg_sequence s join pg_class c on c.oid = s.seqrelid
     join pg_namespace n on n.oid = c.relnamespace
    where ${rel} order by 1"
same "rv_recreate columns of the partitioned and plain tables (type, NOT NULL, default)" \
  "select c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
          coalesce(pg_get_expr(ad.adbin, ad.adrelid), '-')
     from pg_attribute a join pg_class c on c.oid = a.attrelid
     join pg_namespace n on n.oid = c.relnamespace
     left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
    where ${rel} and c.relkind in ('r', 'p') and not c.relispartition
      and a.attnum > 0 and not a.attisdropped order by 1, 2"
same "rv_recreate foreign keys (definition, validated)" \
  "select c.relname, co.conname, pg_get_constraintdef(co.oid, true), co.convalidated
     from pg_constraint co join pg_class c on c.oid = co.conrelid
     join pg_namespace n on n.oid = c.relnamespace
    where ${rel} and co.contype = 'f' and co.conislocal order by 1, 2"
same "rv_recreate indexes" \
  "select c.relname, pg_get_indexdef(i.indexrelid)
     from pg_index i join pg_class c on c.oid = i.indrelid
     join pg_namespace n on n.oid = c.relnamespace
    where ${rel} order by 1, 2"
same "rv_recreate triggers (definition, enabled)" \
  "select c.relname, t.tgname, t.tgenabled, pg_get_triggerdef(t.oid)
     from pg_trigger t join pg_class c on c.oid = t.tgrelid
     join pg_namespace n on n.oid = c.relnamespace
    where ${rel} and not t.tgisinternal order by 1, 2"
same "rv_recreate extended statistics" \
  "select s.stxname, s.stxrelid::regclass::text, s.stxkind::text
     from pg_statistic_ext s join pg_namespace n on n.oid = s.stxnamespace
    where n.nspname = 'test_schema' and s.stxname like 'rv\_recreate\_%' order by 1"
same "rv_recreate table privileges and comments" \
  "select c.relname, coalesce(c.relacl::text, '-'), coalesce(obj_description(c.oid, 'pg_class'), '-')
     from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where ${rel} and c.relkind in ('r', 'p', 'v') order by 1"

# Not vacuous: b has the twelve partitions and the eight foreign keys.
parts=$(q b "select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace
              where ${rel} and c.relkind in ('r', 'p') and c.relispartition") || parts=error
if [ "$parts" != 12 ]; then
  echo "ERROR: expected 12 rv_recreate partitions in b, found ${parts}"
  fail=1
fi
fks=$(q b "select count(*) from pg_constraint co join pg_class c on c.oid = co.conrelid
            join pg_namespace n on n.oid = c.relnamespace
           where ${rel} and co.contype = 'f' and co.conislocal") || fks=error
if [ "$fks" != 8 ]; then
  echo "ERROR: expected 8 rv_recreate foreign keys in b, found ${fks}"
  fail=1
fi

# The serial sequences were kept across the DROP TABLE, not recreated: `a`
# still has the values the fixture set (b's are fresh, so this is `a` only).
vals=$(q a "select string_agg(sequencename || '=' || coalesce(last_value::text, 'null'), ',' order by sequencename)
             from pg_sequences where schemaname = 'test_schema' and sequencename like 'rv\_recreate\_%'") || vals=error
if [ "$vals" != "rv_recreate_events_id_seq=2,rv_recreate_orders_id_seq=500" ]; then
  echo "ERROR: rv_recreate sequence values in a are ${vals}, expected events=2 and orders=500"
  fail=1
fi

# The plain table attached to the recreated rv_recreate_events kept its row:
# attached before the DROP TABLE, it went with it.
archived=$(q a "select count(*) from test_schema.rv_recreate_aa_event_archive") || archived=error
if [ "$archived" != 1 ]; then
  echo "ERROR: test_schema.rv_recreate_aa_event_archive in a has ${archived} rows, expected 1"
  fail=1
fi

# Emitted SQL: the sequence is detached before the DROP TABLE and owned again
# after it; the unchanged partitions are created in round 1; the foreign keys
# held against the table are dropped before it. The optional "if exists" /
# "if not exists" is what OUTPUT_FOR_PRODUCTION=true adds.
for seq in rv_recreate_orders_id_seq rv_recreate_events_id_seq; do
  if ! grep -q "^alter sequence test_schema\.${seq} owned by none;" "$OUT"; then
    echo "ERROR: no owned by none for test_schema.${seq}"
    fail=1
  fi
done
for part in rv_recreate_orders_p1 rv_recreate_orders_p2a rv_recreate_orders_def; do
  if ! grep -Eq "^create table (if not exists )?test_schema\.${part} partition of " "$OUT"; then
    echo "ERROR: unchanged partition test_schema.${part} is not created in round 1"
    fail=1
  fi
done
for fk in rv_recreate_order_lines.rv_recreate_order_lines_order_fk \
          rv_recreate_p1_notes.rv_recreate_p1_notes_order_fk \
          rv_recreate_event_tags.rv_recreate_event_tags_event_fk \
          rv_recreate_pub_lines.rv_recreate_pub_lines_order_fk \
          rv_recreate_tree_refs.rv_recreate_tree_refs_fk \
          '"rv_recreate_CasedRefs"'.'"rv_recreate_Cased_FK"'; do
  if ! grep -Eq "^alter table test_schema\.${fk%%.*} drop constraint (if exists )?${fk#*.};" "$OUT"; then
    echo "ERROR: foreign key ${fk} is not dropped before the recreate"
    fail=1
  fi
done
[ "$fail" -eq 0 ] || exit 1
echo "OK: recreated tables match, with their partitions, sequences and foreign keys"
