#!/usr/bin/env bash
# r3-constraints (rv_r3c_*) — after applying round 1, database `a` must match
# `b` for the fixture's CHECK constraints (the copies a table only inherits
# included, with their enforceability on PostgreSQL 18+), its partitions and
# INHERITS parents (in order), which columns are NOT NULL (and the NOT NULL
# constraint names TO chose, moved or swapped), the foreign server that
# changes its wrapper with everything on it (its USAGE grant, a column grant,
# a foreign partition), and the text search dictionary that changes its
# template with what is built through it.
# The round-2 diff alone cannot prove it: pgc never dumped an inherited
# CHECK copy, so a property lost on both sides compares empty, and a
# migration that failed under ON_ERROR_STOP=0 can leave objects equal by
# accident.
# Run after "Apply diff to database `a`" and before round 2 (which overwrites
# /tmp/output.sql). Connection: psql -h 127.0.0.1 -U postgres, with PGPORT and
# PGPASSWORD taken from the environment. The statement checks read the
# default-mode output; set TEXT_CHECKS=0 to run only the catalog checks (e.g.
# for an OUTPUT_FOR_PRODUCTION=true run, whose statements carry IF [NOT]
# EXISTS).
set -u
out="${OUTPUT_SQL:-/tmp/output.sql}"
text_checks="${TEXT_CHECKS:-1}"
q() { psql -h 127.0.0.1 -U postgres -X -d "$1" -v ON_ERROR_STOP=1 -tAc "$2"; }
[ -r "$out" ] || { echo "ERROR: round-1 migration $out not found"; exit 1; }
ver=$(q a "show server_version_num") || { echo "ERROR: cannot reach database a"; exit 1; }
fail=0
same() { # <label> <sql>: the query must return the same rows in a and b
  local ra rb
  ra=$(q a "$2") || { echo "ERROR: $1 query failed on a"; fail=1; return; }
  rb=$(q b "$2") || { echo "ERROR: $1 query failed on b"; fail=1; return; }
  if [ "$ra" != "$rb" ]; then
    echo "ERROR: $1 differs between a and b (server_version_num $ver)"
    diff <(printf '%s\n' "$ra") <(printf '%s\n' "$rb") || true
    fail=1
  fi
}
expect() { # <label> <sql on a> <expected>
  local got
  got=$(q a "$2") || { echo "ERROR: $1 query failed on a"; fail=1; return; }
  [ "$got" = "$3" ] || { echo "ERROR: $1: expected '$3', got '$got'"; fail=1; }
}
absent() { # <label> <fixed string>: the round-1 migration must not contain it
  if grep -qiF -- "$2" "$out"; then echo "ERROR: $1: round 1 contains '$2'"; fail=1; fi
}
rels="n.nspname = 'test_schema' and c.relname like 'rv\\_r3c\\_%'"
if [ "$ver" -ge 180000 ]; then enforced="c.conenforced"; else enforced="true"; fi

# (1) + (2) CHECK constraints, local and inherited copies alike, with their
# enforceability; and the partitions with their parents and bounds.
same "rv_r3c CHECK constraints" "
  select format('%s|%s|%s|%s|%s|%s|%s', c.conrelid::regclass, c.conname, c.conislocal,
                c.coninhcount, $enforced, c.convalidated, pg_get_constraintdef(c.oid))
  from pg_constraint c join pg_class r on r.oid = c.conrelid
  join pg_namespace n on n.oid = r.relnamespace
  where n.nspname = 'test_schema' and r.relname like 'rv\\_r3c\\_%' and c.contype = 'c'
  order by 1"
same "rv_r3c partitions and parents" "
  select format('%s|%s|%s|%s', c.oid::regclass, coalesce(i.inhparent::regclass::text, '-'),
                coalesce(i.inhseqno, 0), coalesce(pg_get_expr(c.relpartbound, c.oid), '-'))
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  left join pg_inherits i on i.inhrelid = c.oid
  where $rels and c.relkind in ('r', 'p', 'f')
  order by 1"
expect "rv_r3c attached rows" "
  select string_agg(format('%s:%s', id, a), ',' order by id) from test_schema.rv_r3c_at" "10:5,20:5,30:5,40:5"
expect "rv_r3c_atc rows" "
  select string_agg(format('%s:%s:%s', id, a, coalesce(c::text, '-')), ',' order by id)
  from test_schema.rv_r3c_atc" "1:1:1,11:1:-,21:1:-"
same "rv_r3c_atc columns" "
  select format('%s.%s|%s', a.attrelid::regclass, a.attname, format_type(a.atttypid, a.atttypmod))
  from pg_attribute a join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'test_schema' and c.relname like 'rv\\_r3c\\_atc%'
    and a.attnum > 0 and not a.attisdropped
  order by 1"
same "rv_r3c_atc_sub_1 indexes" "
  select pg_get_indexdef(indexrelid) from pg_index
  where indrelid = 'test_schema.rv_r3c_atc_sub_1'::regclass order by 1"
expect "rv_r3c_inh_join rows reach its parents" "
  select string_agg(format('%s:%s', id, a), ',' order by id) from test_schema.rv_r3c_inh_g" "1:1,2:2"

# (3) NOT NULL: which columns are NOT NULL (a DROP CONSTRAINT of a drifted
# name cleared it), and, on PostgreSQL 18+, the NOT NULL constraint of each
# column. Names PostgreSQL generated are anonymous and may differ; the one TO
# chose (rv_r3c_nn_named_nn) has to arrive.
same "rv_r3c NOT NULL columns" "
  select format('%s.%s|%s', a.attrelid::regclass, a.attname, a.attnotnull)
  from pg_attribute a join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and c.relkind in ('r', 'p') and a.attnum > 0 and not a.attisdropped
  order by 1"
if [ "$ver" -ge 180000 ]; then
  # INHERIT keeps the NOT NULL constraint it needs the column to have
  # already local (a known limitation): only whether one is there counts for
  # the table that joins rv_r3c_inh_p.
  same "rv_r3c NOT NULL constraints" "
    select format('%s|%s|%s', c.conrelid::regclass,
                  (select a.attname from pg_attribute a
                    where a.attrelid = c.conrelid and a.attnum = c.conkey[1]),
                  r.relname = 'rv_r3c_inh_join' or c.conislocal or c.coninhcount = 0)
    from pg_constraint c join pg_class r on r.oid = c.conrelid
    join pg_namespace n on n.oid = r.relnamespace
    where n.nspname = 'test_schema' and r.relname like 'rv\\_r3c\\_%' and c.contype = 'n'
    order by 1"
  expect "rv_r3c_nn_named NOT NULL name" "
    select string_agg(conname, ',') from pg_constraint
    where conrelid = 'test_schema.rv_r3c_nn_named'::regclass and contype = 'n'" "rv_r3c_nn_named_nn"
  same "rv_r3c moved and swapped NOT NULL names" "
    select format('%s|%s|%s', c.conrelid::regclass, c.conname,
                  (select a.attname from pg_attribute a
                    where a.attrelid = c.conrelid and a.attnum = c.conkey[1]))
    from pg_constraint c
    where c.conrelid in ('test_schema.rv_r3c_nn_move'::regclass, 'test_schema.rv_r3c_nn_swap'::regclass)
      and c.contype = 'n' and c.conname not like '%\\_not\\_null'
    order by 1"
fi
if [ "$ver" -ge 190000 ]; then
  expect "rv_r3c_ie3_c2 merged copy stays enforced" "
    select conenforced from pg_constraint
    where conrelid = 'test_schema.rv_r3c_ie3_c2'::regclass and conname = 'rv_r3c_ie3_ck'" "t"
fi

# (4) The foreign server that changes its wrapper, with its user mapping, the
# foreign table on it (options, comment, privileges) and the view over it.
same "rv_r3c foreign server" "
  select format('%s|%s|%s|%s|%s', s.srvname, w.fdwname, s.srvoptions::text,
                coalesce(obj_description(s.oid, 'pg_foreign_server'), '-'), s.srvacl::text)
  from pg_foreign_server s join pg_foreign_data_wrapper w on w.oid = s.srvfdw
  where s.srvname = 'rv_r3c_srv'"
same "rv_r3c user mappings" "
  select format('%s|%s|%s', srvname, usename, umoptions::text)
  from pg_user_mappings where srvname = 'rv_r3c_srv' order by 1"
same "rv_r3c foreign table" "
  select format('%s|%s|%s|%s|%s|%s', c.oid::regclass, s.srvname, f.ftoptions::text,
                coalesce(obj_description(c.oid, 'pg_class'), '-'), c.relacl::text,
                c.relispartition)
  from pg_foreign_table f join pg_class c on c.oid = f.ftrelid
  join pg_foreign_server s on s.oid = f.ftserver
  join pg_namespace n on n.oid = c.relnamespace
  where $rels
  order by 1"
same "rv_r3c foreign table column grants" "
  select format('%s.%s|%s', a.attrelid::regclass, a.attname, a.attacl::text)
  from pg_attribute a join pg_class c on c.oid = a.attrelid
  join pg_namespace n on n.oid = c.relnamespace
  where $rels and c.relkind = 'f' and a.attnum > 0 and a.attacl is not null
  order by 1"
same "rv_r3c views" "
  select format('%s|%s', c.oid::regclass, pg_get_viewdef(c.oid))
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where $rels and c.relkind = 'v' order by 1"

# (5) The text search dictionary that changes its template, the configuration
# mapping it, and the generated column and index built through that.
same "rv_r3c text search dictionary" "
  select format('%s|%s|%s|%s', d.dictname, t.tmplname, coalesce(d.dictinitoption, '-'),
                coalesce(obj_description(d.oid, 'pg_ts_dict'), '-'))
  from pg_ts_dict d join pg_ts_template t on t.oid = d.dicttemplate
  where d.dictnamespace = 'test_schema'::regnamespace and d.dictname like 'rv\\_r3c\\_%'"
expect "rv_r3c no temporary dictionary left" "
  select count(*) from pg_ts_dict where dictname like 'pgc\\_new\\_%'" "0"
same "rv_r3c text search configuration" "
  select format('%s|%s|%s', m.maptokentype, m.mapseqno, d.dictname)
  from pg_ts_config_map m join pg_ts_dict d on d.oid = m.mapdict
  where m.mapcfg = 'test_schema.rv_r3c_tscfg'::regconfig
  order by m.maptokentype, m.mapseqno"
same "rv_r3c_ts_docs columns" "
  select format('%s|%s|%s', a.attname, a.attgenerated, pg_get_expr(ad.adbin, ad.adrelid))
  from pg_attribute a left join pg_attrdef ad on ad.adrelid = a.attrelid and ad.adnum = a.attnum
  where a.attrelid = 'test_schema.rv_r3c_ts_docs'::regclass and a.attnum > 0 and not a.attisdropped
  order by a.attnum"
same "rv_r3c_ts_docs indexes" "
  select pg_get_indexdef(indexrelid) from pg_index
  where indrelid = 'test_schema.rv_r3c_ts_docs'::regclass order by 1"
expect "rv_r3c_ts_docs rows keep their tsvector" "
  select count(*) from test_schema.rv_r3c_ts_docs where body_tsv is not null" "1"

# The statements that took the dependents along, or failed.
if [ "$text_checks" = 1 ]; then
  absent "server dropped with CASCADE" "DROP SERVER IF EXISTS rv_r3c_srv CASCADE;"
  absent "dictionary dropped with CASCADE" "DROP TEXT SEARCH DICTIONARY IF EXISTS test_schema.rv_r3c_dict CASCADE;"
  absent "merged CHECK dropped after the attach" "alter table test_schema.rv_r3c_at_same drop constraint"
  absent "merged CHECK dropped after the attach" "alter table test_schema.rv_r3c_at_like drop constraint"
  absent "drifted NOT NULL dropped" "alter table test_schema.rv_r3c_nn_col drop constraint"
  absent "drifted NOT NULL dropped" "alter table test_schema.rv_r3c_nn_tab drop constraint"
  absent "drifted NOT NULL dropped" "alter table test_schema.rv_r3c_nn_ident drop constraint"
  absent "drifted NOT NULL dropped" "alter table test_schema.rv_r3c_nn_custom drop constraint"
  absent "moved NOT NULL dropped" "alter table test_schema.rv_r3c_nn_move drop constraint"
  absent "swapped NOT NULL dropped" "alter table test_schema.rv_r3c_nn_swap drop constraint"
  absent "joined INHERITS parent's CHECK dropped for good" "alter table test_schema.rv_r3c_inh_merge drop constraint"
fi

exit $fail
