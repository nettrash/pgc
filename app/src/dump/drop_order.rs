//! The drop order of the clear command
//! ([`Dump::generate_clear_script`](super::core::Dump::generate_clear_script)),
//! taken from `pg_depend`.
//!
//! PostgreSQL refuses to drop an object while another object depends on it
//! (short of `CASCADE`), and those dependencies cross every section of the
//! clear script: a domain `CHECK` calls a function, a table's rule reads a
//! view, a classic `INHERITS` child blocks its parent, a cast or an operator
//! blocks the function it uses, a foreign server blocks the wrapper an
//! extension provides. So the clear command reads the dependencies that
//! matter for dropping straight from the catalog, once, when it inspects the
//! database: [`build_drop_dependencies_query`] turns every hard `pg_depend`
//! row into a [`DropDependency`] between two droppable objects, named by the
//! keys below. The script builds its drops in its usual section order and then
//! reorders only what one of these edges forbids
//! ([`stable_topological_order`]), so a schema without such a dependency keeps
//! the script it always had.
//!
//! # Keys
//!
//! Both sides are keyed the same way — `<kind>:<identity>` — the SQL from the
//! catalog and the clear command from the dumped objects, whose names are
//! already `quote_ident`-ed by the dump queries:
//!
//! | Kind | Objects | Identity |
//! | --- | --- | --- |
//! | `rel` | tables, views, materialized views, foreign tables, sequences | `schema.name` |
//! | `fk` | foreign-key constraints | `schema.table.constraint` |
//! | `proc` | routines | [`Routine::dependency_key`](super::routine::Routine::dependency_key) |
//! | `type` | enums, composites, domains, ranges, multiranges | `schema.name` |
//! | `stx` | extended statistics | `schema.name` |
//! | `op` | operators | [`Operator::key`](super::operator::Operator::key) |
//! | `coll`, `tscfg`, `tsdict` | collations, text search configurations and dictionaries | `schema.name` |
//! | `cast` | casts | `(source AS target)` |
//! | `evt`, `fdw`, `srv`, `sub` | event triggers, foreign-data wrappers, servers, subscriptions | name |
//! | `um` | user mappings | `user SERVER server` |
//! | `ext`, `nsp` | extensions, schemas | name |
//!
//! A sub-object is keyed as the object it is dropped with: a column default,
//! a non-foreign-key constraint, a trigger, a rule, a policy or an index as
//! its table (a domain constraint as its domain), an array type as its
//! element type, a relation's row type as the relation, and any member of an
//! extension as the extension. A foreign key keeps a key of its own because
//! the script drops it by itself, ahead of the tables.
//!
//! An object inside a schema the dump did not inspect has no key, and its
//! dependencies are left out — except on a database-wide object (a
//! foreign-data wrapper, server, user mapping, subscription or event
//! trigger), where it shows as [`OUTSIDE_KEY`]: the clear command must leave
//! such an object alone, because dropping it cannot succeed (a foreign table
//! in another schema still uses the server) and its dependents that can be
//! dropped (the server's user mappings) would be lost for nothing.

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::BTreeSet;
use std::io::Error;

/// The `before` key of a [`DropDependency`] whose dependent object lies
/// outside the inspected schemas (a foreign table in another schema on a
/// foreign server). Reported only for a database-wide `after` object — a
/// foreign-data wrapper, server, user mapping, subscription or event trigger
/// — which the clear script then does not drop. No real key has this form:
/// every other kind carries a non-empty identity after its colon.
pub const OUTSIDE_KEY: &str = "outside:";

/// A hard ordering constraint between two objects the clear script drops:
/// `before` has to be dropped before `after`.
///
/// Both are keys in the format the module documentation describes. For an
/// ordinary (`n`) dependency `before` is the dependent object; for an internal
/// (`i`) one between two objects that are dropped separately — a multirange
/// type and its range, an identity sequence and its table — it is the owner,
/// whose drop removes the other.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DropDependency {
    /// Key of the object dropped first.
    pub before: String,
    /// Key of the object dropped after it.
    pub after: String,
    /// A statement that removes the dependency without dropping `before`,
    /// when it is held by a part of `before` that can be dropped on its own:
    /// a `CHECK` constraint, a column default, a stored generated column's
    /// expression (`DROP EXPRESSION`, which keeps the column and so every
    /// view reading it; a virtual one cannot drop its expression and is
    /// dropped whole, although PostgreSQL 18 and 19 let no virtual column
    /// call a user-defined function), a trigger, a rule, a policy or a
    /// standalone index of the table `before`. Without a trailing semicolon. The clear script runs
    /// it only to break a dependency cycle — a table whose `CHECK` calls a
    /// function whose SQL-standard body reads that table — which no drop order
    /// can satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breaker: Option<String>,
}

impl DropDependency {
    /// An edge dropping `before` ahead of `after`, with no breaker.
    pub fn new(before: impl Into<String>, after: impl Into<String>) -> Self {
        Self {
            before: before.into(),
            after: after.into(),
            breaker: None,
        }
    }

    /// The same edge, which `breaker` can remove (see
    /// [`DropDependency::breaker`]).
    pub fn with_breaker(mut self, breaker: impl Into<String>) -> Self {
        self.breaker = Some(breaker.into());
        self
    }
}

/// Key of a table, view, materialized view, foreign table or sequence.
pub fn relation_key(schema: &str, name: &str) -> String {
    format!("rel:{schema}.{name}")
}

/// Key of a foreign-key constraint on `schema.table`.
pub fn foreign_key_key(schema: &str, table: &str, name: &str) -> String {
    format!("fk:{schema}.{table}.{name}")
}

/// Key of a routine, from its
/// [`Routine::dependency_key`](super::routine::Routine::dependency_key).
pub fn routine_key(dependency_key: &str) -> String {
    format!("proc:{dependency_key}")
}

/// Key of a user-defined type (enum, composite, domain, range, multirange).
pub fn type_key(schema: &str, name: &str) -> String {
    format!("type:{schema}.{name}")
}

/// Key of an extended statistics object.
pub fn statistic_key(schema: &str, name: &str) -> String {
    format!("stx:{schema}.{name}")
}

/// Key of an operator, from its [`Operator::key`](super::operator::Operator::key).
pub fn operator_key(operator_key: &str) -> String {
    format!("op:{operator_key}")
}

/// Key of a collation.
pub fn collation_key(schema: &str, name: &str) -> String {
    format!("coll:{schema}.{name}")
}

/// Key of a text search configuration.
pub fn ts_config_key(schema: &str, name: &str) -> String {
    format!("tscfg:{schema}.{name}")
}

/// Key of a text search dictionary.
pub fn ts_dict_key(schema: &str, name: &str) -> String {
    format!("tsdict:{schema}.{name}")
}

/// Key of a cast, from its `format_type`-rendered source and target types.
pub fn cast_key(source: &str, target: &str) -> String {
    format!("cast:({source} AS {target})")
}

/// Key of an event trigger.
pub fn event_trigger_key(name: &str) -> String {
    format!("evt:{name}")
}

/// Key of a foreign-data wrapper.
pub fn fdw_key(name: &str) -> String {
    format!("fdw:{name}")
}

/// Key of a foreign server.
pub fn server_key(name: &str) -> String {
    format!("srv:{name}")
}

/// Key of a user mapping; `user` is `PUBLIC` or a `quote_ident`-ed role name.
pub fn user_mapping_key(user: &str, server: &str) -> String {
    format!("um:{user} SERVER {server}")
}

/// Key of a subscription.
pub fn subscription_key(name: &str) -> String {
    format!("sub:{name}")
}

/// Key of an extension.
pub fn extension_key(name: &str) -> String {
    format!("ext:{name}")
}

/// Key of a schema.
pub fn schema_key(name: &str) -> String {
    format!("nsp:{name}")
}

/// The drop dependencies of a database, as `before_key` / `after_key` /
/// `breaker` rows (see the module documentation for the keys, and
/// [`DropDependency::breaker`]).
///
/// `schema_filter` is the dump's `('a', 'b')` list of the inspected schemas:
/// an object inside a schema gets a key only when it is in one of them, so the
/// result stays within what the clear script can drop. Database-wide objects —
/// casts, event triggers, foreign-data wrappers, servers, user mappings,
/// subscriptions, extensions — are keyed wherever they are; the clear command
/// decides which of them it drops. A dependent without a key (outside the
/// inspected schemas) of a foreign-data wrapper, server, user mapping,
/// subscription or event trigger is reported as [`OUTSIDE_KEY`], so that the
/// clear command leaves that object alone.
///
/// Only the dependency types that make a plain `DROP` fail count: normal
/// (`n`) and internal (`i`). An automatic one (`a`: a partition on its parent,
/// an owned sequence on its column, a constraint on its table) goes away with
/// the object it depends on, and an extension membership (`e`) is folded into
/// the keys instead. One automatic dependency does make a `DROP` fail:
/// PostgreSQL 14 records a generated column's use of an object (a function in
/// its expression) as an automatic dependency of the column itself, so
/// dropping that object drops the column — which fails while a view, or
/// anything else, depends normally on the column. Each such dependent of the
/// column is reported as a dependent of the object too. PostgreSQL 15 and
/// later record the use on the column's default expression (`pg_attrdef`)
/// instead, as a normal dependency, and that part of the query finds nothing.
///
/// Every catalog and column it reads exists on each supported server
/// (PostgreSQL 14+; the newest, `pg_trigger.tgparentid`, is from 13), so
/// nothing is version-gated. User mappings are read through the
/// `pg_user_mappings` view, which any role can read, rather than the
/// superuser-only `pg_user_mapping` catalog; from `pg_subscription` only the
/// publicly readable `oid` and `subname` columns are used. Run it with the
/// session's default `search_path`, like the routine, cast and operator
/// queries, so the argument and operand types come out as they render them.
pub fn build_drop_dependencies_query(schema_filter: &str) -> String {
    format!(
        "with recursive
dep as (
    select d.classid, d.objid, d.refclassid, d.refobjid, d.deptype
    from pg_catalog.pg_depend d
    where d.deptype in ('n', 'i')
  union
    select u.classid, u.objid, g.refclassid, g.refobjid, 'n'::\"char\"
    from pg_catalog.pg_depend g
    join pg_catalog.pg_attribute a on a.attrelid = g.objid and a.attnum = g.objsubid
    join pg_catalog.pg_depend u on u.refclassid = g.classid and u.refobjid = g.objid
        and u.refobjsubid = g.objsubid and u.deptype = 'n'
    where g.classid = 'pg_catalog.pg_class'::regclass and g.objsubid > 0
      and g.deptype = 'a' and a.attgenerated <> ''
      and not (g.refclassid = 'pg_catalog.pg_class'::regclass and g.refobjid = g.objid)
),
obj as (
    select classid, objid from dep
    union
    select refclassid, refobjid from dep
),
walk (classid, objid, cur_class, cur_obj, depth) as (
    select o.classid, o.objid, o.classid, o.objid, 0 from obj o
  union all
    select w.classid, w.objid, p.pclass, p.pobj, w.depth + 1
    from walk w
    cross join lateral (
        select 'pg_catalog.pg_class'::regclass::oid, i.indrelid
        from pg_catalog.pg_index i
        where w.cur_class = 'pg_catalog.pg_class'::regclass and i.indexrelid = w.cur_obj
      union all
        select 'pg_catalog.pg_type'::regclass::oid, c.reltype
        from pg_catalog.pg_class c
        where w.cur_class = 'pg_catalog.pg_class'::regclass and c.oid = w.cur_obj
          and c.relkind = 'c'
      union all
        select 'pg_catalog.pg_class'::regclass::oid, a.adrelid
        from pg_catalog.pg_attrdef a
        where w.cur_class = 'pg_catalog.pg_attrdef'::regclass and a.oid = w.cur_obj
      union all
        select case when c.conrelid <> 0 then 'pg_catalog.pg_class'::regclass::oid
                    else 'pg_catalog.pg_type'::regclass::oid end,
               case when c.conrelid <> 0 then c.conrelid else c.contypid end
        from pg_catalog.pg_constraint c
        where w.cur_class = 'pg_catalog.pg_constraint'::regclass and c.oid = w.cur_obj
          and not (c.contype = 'f' and c.conrelid <> 0)
          and (c.conrelid <> 0 or c.contypid <> 0)
      union all
        select 'pg_catalog.pg_class'::regclass::oid, t.tgrelid
        from pg_catalog.pg_trigger t
        where w.cur_class = 'pg_catalog.pg_trigger'::regclass and t.oid = w.cur_obj
      union all
        select 'pg_catalog.pg_class'::regclass::oid, r.ev_class
        from pg_catalog.pg_rewrite r
        where w.cur_class = 'pg_catalog.pg_rewrite'::regclass and r.oid = w.cur_obj
      union all
        select 'pg_catalog.pg_class'::regclass::oid, p.polrelid
        from pg_catalog.pg_policy p
        where w.cur_class = 'pg_catalog.pg_policy'::regclass and p.oid = w.cur_obj
      union all
        select 'pg_catalog.pg_type'::regclass::oid, e.oid
        from pg_catalog.pg_type t
        join pg_catalog.pg_type e on e.oid = t.typelem and e.typarray = t.oid
        where w.cur_class = 'pg_catalog.pg_type'::regclass and t.oid = w.cur_obj
      union all
        select 'pg_catalog.pg_class'::regclass::oid, t.typrelid
        from pg_catalog.pg_type t
        join pg_catalog.pg_class c on c.oid = t.typrelid
        where w.cur_class = 'pg_catalog.pg_type'::regclass and t.oid = w.cur_obj
          and c.relkind <> 'c'
    ) p(pclass, pobj)
    where w.depth < 4
      and not exists (
          select 1 from pg_catalog.pg_depend m
          where m.classid = w.cur_class and m.objid = w.cur_obj and m.objsubid = 0
            and m.deptype = 'e'
      )
),
top as (
    select distinct on (classid, objid) classid, objid, cur_class, cur_obj
    from walk
    order by classid, objid, depth desc
),
keyed as (
    select t.classid, t.objid,
        coalesce(
            (select 'ext:' || quote_ident(x.extname)
             from pg_catalog.pg_depend m
             join pg_catalog.pg_extension x on x.oid = m.refobjid
             where m.classid = t.cur_class and m.objid = t.cur_obj and m.objsubid = 0
               and m.deptype = 'e' and m.refclassid = 'pg_catalog.pg_extension'::regclass
             limit 1),
            case t.cur_class
            when 'pg_catalog.pg_class'::regclass then (
                select 'rel:' || quote_ident(n.nspname) || '.' || quote_ident(c.relname)
                from pg_catalog.pg_class c
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                where c.oid = t.cur_obj and c.relkind in ('r', 'p', 'v', 'm', 'f', 'S')
                  and n.nspname in {schema_filter})
            when 'pg_catalog.pg_proc'::regclass then (
                select 'proc:' || quote_ident(n.nspname) || '.' || quote_ident(p.proname)
                    || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ')'
                from pg_catalog.pg_proc p
                join pg_catalog.pg_namespace n on n.oid = p.pronamespace
                where p.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_type'::regclass then (
                select 'type:' || quote_ident(n.nspname) || '.' || quote_ident(ty.typname)
                from pg_catalog.pg_type ty
                join pg_catalog.pg_namespace n on n.oid = ty.typnamespace
                where ty.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_constraint'::regclass then (
                select 'fk:' || quote_ident(n.nspname) || '.' || quote_ident(c.relname)
                    || '.' || quote_ident(k.conname)
                from pg_catalog.pg_constraint k
                join pg_catalog.pg_class c on c.oid = k.conrelid
                join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                where k.oid = t.cur_obj and k.contype = 'f' and n.nspname in {schema_filter})
            when 'pg_catalog.pg_operator'::regclass then (
                select 'op:' || quote_ident(n.nspname) || '.' || o.oprname || '('
                    || coalesce(case when o.oprleft <> 0
                        then pg_catalog.format_type(o.oprleft, null) end, 'NONE')
                    || ', '
                    || coalesce(case when o.oprright <> 0
                        then pg_catalog.format_type(o.oprright, null) end, 'NONE')
                    || ')'
                from pg_catalog.pg_operator o
                join pg_catalog.pg_namespace n on n.oid = o.oprnamespace
                where o.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_collation'::regclass then (
                select 'coll:' || quote_ident(n.nspname) || '.' || quote_ident(c.collname)
                from pg_catalog.pg_collation c
                join pg_catalog.pg_namespace n on n.oid = c.collnamespace
                where c.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_ts_config'::regclass then (
                select 'tscfg:' || quote_ident(n.nspname) || '.' || quote_ident(c.cfgname)
                from pg_catalog.pg_ts_config c
                join pg_catalog.pg_namespace n on n.oid = c.cfgnamespace
                where c.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_ts_dict'::regclass then (
                select 'tsdict:' || quote_ident(n.nspname) || '.' || quote_ident(d.dictname)
                from pg_catalog.pg_ts_dict d
                join pg_catalog.pg_namespace n on n.oid = d.dictnamespace
                where d.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_statistic_ext'::regclass then (
                select 'stx:' || quote_ident(n.nspname) || '.' || quote_ident(s.stxname)
                from pg_catalog.pg_statistic_ext s
                join pg_catalog.pg_namespace n on n.oid = s.stxnamespace
                where s.oid = t.cur_obj and n.nspname in {schema_filter})
            when 'pg_catalog.pg_cast'::regclass then (
                select 'cast:(' || pg_catalog.format_type(c.castsource, null) || ' AS '
                    || pg_catalog.format_type(c.casttarget, null) || ')'
                from pg_catalog.pg_cast c
                where c.oid = t.cur_obj)
            when 'pg_catalog.pg_event_trigger'::regclass then (
                select 'evt:' || quote_ident(e.evtname)
                from pg_catalog.pg_event_trigger e
                where e.oid = t.cur_obj)
            when 'pg_catalog.pg_foreign_data_wrapper'::regclass then (
                select 'fdw:' || quote_ident(w.fdwname)
                from pg_catalog.pg_foreign_data_wrapper w
                where w.oid = t.cur_obj)
            when 'pg_catalog.pg_foreign_server'::regclass then (
                select 'srv:' || quote_ident(s.srvname)
                from pg_catalog.pg_foreign_server s
                where s.oid = t.cur_obj)
            when 'pg_catalog.pg_user_mapping'::regclass then (
                select 'um:'
                    || case when u.umuser = 0 then 'PUBLIC' else quote_ident(u.usename) end
                    || ' SERVER ' || quote_ident(u.srvname)
                from pg_catalog.pg_user_mappings u
                where u.umid = t.cur_obj)
            when 'pg_catalog.pg_subscription'::regclass then (
                select 'sub:' || quote_ident(s.subname)
                from pg_catalog.pg_subscription s
                where s.oid = t.cur_obj)
            when 'pg_catalog.pg_extension'::regclass then (
                select 'ext:' || quote_ident(x.extname)
                from pg_catalog.pg_extension x
                where x.oid = t.cur_obj)
            when 'pg_catalog.pg_namespace'::regclass then (
                select 'nsp:' || quote_ident(n.nspname)
                from pg_catalog.pg_namespace n
                where n.oid = t.cur_obj and n.nspname in {schema_filter})
            end
        ) as key
    from top t
)
select distinct
    case when d.deptype = 'i' then rk.key else coalesce(dk.key, '{OUTSIDE_KEY}') end as before_key,
    case when d.deptype = 'i' then dk.key else rk.key end as after_key,
    case when d.deptype = 'n' and dk.key is not null then (
        case d.classid
        when 'pg_catalog.pg_constraint'::regclass then (
            select format('alter table if exists %I.%I drop constraint if exists %I',
                          n.nspname, c.relname, k.conname)
            from pg_catalog.pg_constraint k
            join pg_catalog.pg_class c on c.oid = k.conrelid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where k.oid = d.objid and k.contype = 'c' and k.conislocal and k.coninhcount = 0)
        when 'pg_catalog.pg_attrdef'::regclass then (
            select case
                when a.attgenerated = 's'
                then format('alter table if exists %I.%I alter column %I drop expression if exists',
                            n.nspname, c.relname, a.attname)
                when a.attgenerated <> ''
                then format('alter table if exists %I.%I drop column if exists %I',
                            n.nspname, c.relname, a.attname)
                else format('alter table if exists %I.%I alter column %I drop default',
                            n.nspname, c.relname, a.attname)
                end
            from pg_catalog.pg_attrdef ad
            join pg_catalog.pg_attribute a on a.attrelid = ad.adrelid and a.attnum = ad.adnum
            join pg_catalog.pg_class c on c.oid = ad.adrelid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where ad.oid = d.objid and a.attislocal and a.attinhcount = 0)
        when 'pg_catalog.pg_trigger'::regclass then (
            select format('drop trigger if exists %I on %I.%I', tg.tgname, n.nspname, c.relname)
            from pg_catalog.pg_trigger tg
            join pg_catalog.pg_class c on c.oid = tg.tgrelid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where tg.oid = d.objid and not tg.tgisinternal and tg.tgparentid = 0)
        when 'pg_catalog.pg_rewrite'::regclass then (
            select format('drop rule if exists %I on %I.%I', r.rulename, n.nspname, c.relname)
            from pg_catalog.pg_rewrite r
            join pg_catalog.pg_class c on c.oid = r.ev_class
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where r.oid = d.objid and r.rulename <> '_RETURN')
        when 'pg_catalog.pg_policy'::regclass then (
            select format('drop policy if exists %I on %I.%I', p.polname, n.nspname, c.relname)
            from pg_catalog.pg_policy p
            join pg_catalog.pg_class c on c.oid = p.polrelid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where p.oid = d.objid)
        when 'pg_catalog.pg_class'::regclass then (
            select format('drop index if exists %I.%I', n.nspname, c.relname)
            from pg_catalog.pg_class c
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where c.oid = d.objid and c.relkind in ('i', 'I')
              and not exists (select 1 from pg_catalog.pg_constraint k where k.conindid = c.oid)
              and not exists (select 1 from pg_catalog.pg_inherits h where h.inhrelid = c.oid))
        end)
    end as breaker
from dep d
join keyed dk on dk.classid = d.classid and dk.objid = d.objid
join keyed rk on rk.classid = d.refclassid and rk.objid = d.refobjid
where rk.key is not null
  and ((dk.key is not null and dk.key <> rk.key)
    or (dk.key is null and d.deptype = 'n'
        and split_part(rk.key, ':', 1) in ('fdw', 'srv', 'um', 'sub', 'evt')))
order by 1, 2, 3;"
    )
}

/// Turns JIT off for the rest of the transaction. The dependency query is a
/// few milliseconds of catalog lookups whose row estimates are large enough to
/// cross the default JIT thresholds, and compiling it costs PostgreSQL 14
/// three quarters of a second.
const JIT_OFF_SQL: &str = "set local jit = off";

/// Run [`build_drop_dependencies_query`] over `pool`.
///
/// Called once by [`Dump::inspect`](super::core::Dump::inspect), after the
/// fill, so it never competes with the fill's parallel branches for a
/// connection. Both statements run in one short read-only transaction, which
/// is what scopes the `SET LOCAL`.
pub async fn fetch_drop_dependencies(
    pool: &PgPool,
    schema_filter: &str,
) -> Result<Vec<DropDependency>, Error> {
    let fail = |e: sqlx::Error| Error::other(format!("Failed to fetch object dependencies: {e}."));
    let mut tx = pool.begin().await.map_err(fail)?;
    sqlx::query(JIT_OFF_SQL)
        .execute(&mut *tx)
        .await
        .map_err(fail)?;
    let rows = sqlx::query(build_drop_dependencies_query(schema_filter).as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(fail)?;
    tx.commit().await.map_err(fail)?;
    Ok(rows
        .into_iter()
        .map(|row| DropDependency {
            before: row.get("before_key"),
            after: row.get("after_key"),
            breaker: row.get("breaker"),
        })
        .collect())
}

/// The result of [`stable_topological_order`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologicalOrder {
    /// Every node, each exactly once.
    pub order: Vec<usize>,
    /// Nodes placed ahead of an edge they could not satisfy, because they sit
    /// on a dependency cycle; empty when every edge holds.
    pub forced: Vec<usize>,
}

/// Order nodes `0..n` so that for every `(before, after)` edge `before` comes
/// first, changing the given order `0..n` as little as possible.
///
/// Kahn's algorithm that always takes the lowest-numbered ready node, so when
/// `0..n` already satisfies every edge it is returned unchanged; otherwise a
/// node moves only as far as an edge forces it. A cycle cannot be satisfied:
/// when nothing is ready, the lowest-numbered node of a cycle that nothing
/// outside it is still waiting for is placed anyway and recorded in
/// [`TopologicalOrder::forced`], and the rest follows as usual. Edges naming a
/// node outside `0..n`, and self-edges, are ignored.
pub fn stable_topological_order(n: usize, edges: &[(usize, usize)]) -> TopologicalOrder {
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut in_degree = vec![0usize; n];
    let mut seen: BTreeSet<(usize, usize)> = BTreeSet::new();
    for &(before, after) in edges {
        if before < n && after < n && before != after && seen.insert((before, after)) {
            successors[before].push(after);
            in_degree[after] += 1;
        }
    }

    let mut placed = vec![false; n];
    let mut ready: BTreeSet<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    let mut forced = Vec::new();
    while order.len() < n {
        let next = match ready.pop_first() {
            Some(node) => node,
            None => {
                let node = first_node_of_a_source_cycle(&successors, &placed);
                forced.push(node);
                node
            }
        };
        placed[next] = true;
        order.push(next);
        for &after in &successors[next] {
            in_degree[after] = in_degree[after].saturating_sub(1);
            if in_degree[after] == 0 && !placed[after] {
                ready.insert(after);
            }
        }
    }
    TopologicalOrder { order, forced }
}

/// The lowest-numbered unplaced node in a strongly connected component of the
/// unplaced nodes that no other unplaced component has an edge into.
///
/// Called only when Kahn's algorithm stalls, so every unplaced node waits for
/// another one and such a source component is a genuine cycle: placing one of
/// its nodes breaks exactly one cycle, and never jumps a node ahead of
/// something that is merely queued behind a cycle.
fn first_node_of_a_source_cycle(successors: &[Vec<usize>], placed: &[bool]) -> usize {
    let active: Vec<bool> = placed.iter().map(|p| !p).collect();
    let component = strongly_connected_components(successors, &active);
    let count = component
        .iter()
        .filter(|&&c| c != usize::MAX)
        .max()
        .map_or(0, |&c| c + 1);
    let mut has_incoming = vec![false; count];
    for (node, succs) in successors.iter().enumerate() {
        if !active[node] {
            continue;
        }
        for &succ in succs {
            if active[succ] && component[succ] != component[node] {
                has_incoming[component[succ]] = true;
            }
        }
    }
    (0..successors.len())
        .find(|&node| active[node] && !has_incoming[component[node]])
        .or_else(|| (0..successors.len()).find(|&node| active[node]))
        .expect("a stalled sort has unplaced nodes")
}

/// For each of `edges`, whether it lies on a dependency cycle: both its nodes
/// are in the same strongly connected component of the graph on `0..n`.
/// Edges naming a node outside `0..n`, and self-edges, never do.
pub fn edges_on_cycles(n: usize, edges: &[(usize, usize)]) -> Vec<bool> {
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(before, after) in edges {
        if before < n && after < n && before != after {
            successors[before].push(after);
        }
    }
    let component = strongly_connected_components(&successors, &vec![true; n]);
    edges
        .iter()
        .map(|&(before, after)| {
            before < n && after < n && before != after && component[before] == component[after]
        })
        .collect()
}

/// The strongly connected component of every node the `active` mask keeps,
/// numbered from 0 (`usize::MAX` for the others), over the edges between
/// active nodes. Tarjan's algorithm, kept iterative so a long dependency
/// chain cannot overflow the stack.
fn strongly_connected_components(successors: &[Vec<usize>], active: &[bool]) -> Vec<usize> {
    const UNVISITED: usize = usize::MAX;
    let n = successors.len();
    let mut index = vec![UNVISITED; n];
    let mut low = vec![0usize; n];
    let mut on_stack = vec![false; n];
    let mut component = vec![UNVISITED; n];
    let mut stack: Vec<usize> = Vec::new();
    let mut next_index = 0usize;
    let mut next_component = 0usize;

    for root in 0..n {
        if !active[root] || index[root] != UNVISITED {
            continue;
        }
        // (node, position of the next successor to look at)
        let mut call: Vec<(usize, usize)> = vec![(root, 0)];
        index[root] = next_index;
        low[root] = next_index;
        next_index += 1;
        stack.push(root);
        on_stack[root] = true;
        while let Some(&(node, pos)) = call.last() {
            if let Some(&succ) = successors[node].get(pos) {
                if let Some(frame) = call.last_mut() {
                    frame.1 += 1;
                }
                if !active[succ] {
                    continue;
                }
                if index[succ] == UNVISITED {
                    index[succ] = next_index;
                    low[succ] = next_index;
                    next_index += 1;
                    stack.push(succ);
                    on_stack[succ] = true;
                    call.push((succ, 0));
                } else if on_stack[succ] {
                    low[node] = low[node].min(index[succ]);
                }
                continue;
            }
            call.pop();
            if let Some(&(parent, _)) = call.last() {
                low[parent] = low[parent].min(low[node]);
            }
            if low[node] == index[node] {
                while let Some(member) = stack.pop() {
                    on_stack[member] = false;
                    component[member] = next_component;
                    if member == node {
                        break;
                    }
                }
                next_component += 1;
            }
        }
    }
    component
}

#[cfg(test)]
#[path = "tests/drop_order.rs"]
mod tests;
