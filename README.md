# PostgreSQL Database Comparer

![rust workflow](https://github.com/nettrash/pgc/actions/workflows/rust.yml/badge.svg)

## Story

We're faced with vital need to have a proper working free tool for comparing two PostgreSQL databases and preparing the delta as SQL script.

We have multiple PostgreSQL database repositories and need to have a properly working deployment pipelines for our databases.

## Glossary

- `FROM` - this database we will use a target database, we want to apply final patch here.
- `TO` - this database we will use as an etalon schema, and prepare delta script that can be applied on FROM database to make schema equals to schema of database `TO`.

## How to build

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features
cargo test  
cargo build
```

## How to run

```bash
cargo run [options]
pgc [options]
```

We have two opportunities to run:

- use configuration file;
- use command line arguments.

Configuration file can help us to put different functions in a chain.
Command line arguments can be used to execute just one function in one time.

## Supported PostgreSQL versions

pgc supports PostgreSQL **14 through 19**. CI runs the full round trip (dump both fixture databases, compare, apply the migration, compare again — the second diff must be empty) against every major version in that range. Until PostgreSQL 19 is released, its leg runs on the newest 19 pre-release image and moves to `postgres:19` on its own once that tag is published.

The migration is applied to the `FROM` database, so the SQL pgc writes is chosen for the server `FROM` was dumped from. Every dump written by pgc 1.1 or later records that server's `server_version_num`, and DDL that only newer servers accept is emitted only when `FROM` is new enough — for example PostgreSQL 19's `ALTER PUBLICATION ... SET ALL TABLES` and `ALTER CONSTRAINT ... [NOT] ENFORCED` on a `CHECK` constraint, or `ALTER COLUMN ... SET EXPRESSION` (PostgreSQL 17+). Otherwise pgc falls back to a form every supported server accepts, usually a drop and re-create. A dump written by pgc 1.0.x carries no version and gets the fallback. **Dump `FROM` from the server you are going to migrate.**

Comparing across major versions works, with these caveats:

- An object that only a newer server can represent (a PostgreSQL 19 `FOR ALL SEQUENCES` publication, a `SERVER` subscription, an `oid8` column, …) produces SQL an older `FROM` server cannot run.
- Contrib extensions ship different versions on different majors, and PostgreSQL 19 cannot install some older ones at all (`btree_gist` below 1.9). pgc records the versions the `FROM` server can install and never pins a version it cannot: such an extension is created at the server's default version, or its update is skipped, with a comment in the script. Dumps written by pgc 1.0.x do not carry this list.
- **PostgreSQL 19 changes the default GiST operator class for `inet` and `cidr`.** With `btree_gist` 1.9, `gist_inet_ops` and `gist_cidr_ops` are no longer the default; the core `inet_ops` is. PostgreSQL prints an operator class in an index or constraint definition only when it is not the default, so `USING gist (addr)` reads the same on 18 and 19 but uses a different operator class, and an index that names its operator class reads differently on the two majors although nothing changed. pgc compares definitions as text: after a compare across 18 and 19, check GiST indexes and exclusion constraints on `inet`/`cidr` columns by hand, e.g. with `select indexrelid::regclass, opc.opcname from pg_index i, unnest(i.indclass) c(oid) join pg_opclass opc on opc.oid = c.oid where opc.opcintype in ('inet'::regtype, 'cidr'::regtype);`.
- Some expressions deparse differently on PostgreSQL 19 (for example a `JSON_ARRAY(SELECT ...)` that older versions stored as a `JSON_ARRAYAGG` subquery). Such a view shows up as changed and is replaced, which is correct: its behaviour differs on 19.

pgc 1.1 reads dumps written by any earlier version, but re-dump both sides after upgrading pgc rather than mixing versions. A dump written by pgc 1.1 can contain object kinds that pgc 1.0.x does not know, and a dump written by pgc 1.0.x lacks what that version could not read: tables whose names start with `pg` but not `pg_` (such as `pgbench_accounts`), SQL-standard routine bodies, publication row filters, column lists, schemas and options, and whether the server defaults to `standard_conforming_strings = off`. It also holds the subscriptions of the cluster's other databases. A migration toward such a dump would drop, empty, strip or create those objects, so `pgc --command compare` refuses a `TO` dump written by pgc 1.0.x when `FROM` was dumped by pgc 1.1 or later: it exits non-zero and writes no script. The opposite pairing, a `FROM` dump written by pgc 1.0.x with a `TO` dump written by pgc 1.1, is compared with a warning on stderr and a comment at the top of the script. That migration re-applies what pgc 1.0.x could not read: it re-creates SQL-standard routines, sets publications and replica identities again, and rewrites literals holding a backslash, starting with `set standard_conforming_strings = on;` because the old dump does not say whether `FROM` needs it. It also creates the tables pgc 1.0.x could not see and, because the old dump cannot say which of the cluster's subscriptions are `FROM`'s, every `TO` subscription; stderr names the ones `FROM` may already have. Where `FROM` has one, its `CREATE` fails with `already exists`. With `--output-for-production`, `create table if not exists` skips such a table's `CREATE TABLE`, but the constraints added to it separately still fail with `already exists`, and any other difference in that table is silently left unapplied, so re-dump `FROM` rather than rely on it. Two dumps written by pgc 1.0.x compare exactly as before, and the `--config` chain always dumps both sides with the running pgc.

## Command line arguments

`--command {dump|compare|clear}` - the command name, dump - to create a dump file, compare - to compare two dumps, clear - to generate a script that drops all objects found in the database.

`--server {server name}` - to specify `server name` for a command, without it tool will use localhost as a host for command.

`--port {port number}` - to specify `port number` for a command, without it tool will use 5432 as a host for command.

`--user {user name}` - to specify `user name` for a command, without it tool will use information from pgpass file.

`--password {user password}` - to specify `user password` for a command, without it tool will use information from pgpass file.

`--database {databasename}` - to specify `database name` for the command, without it tool will use `postgres` as a database name.

`--scheme {schemaname}` - to specify concrete scheme for the command, without it all schemas will be used for command.

`--output {filename}` - to specify output file name for the command, without it the tool will use `data.out` as a file name for output file.

`--from {filename}` - to specify dump file of the `FROM` databadse for the comparer, default value for this property `dump.from`.

`--to {filename}` - to specify dump file of the `TO` databadse for the comparer, default value for this property is `dump.to`.

`--config {filename}` - this argument avoid usage of any other arguments, and tell comparer to use command chain from this file. Default: `pgc.conf`.

`--use_ssl` - specify this argument to use SSL for PostgreSQL connection.

`--use-drop` - execute the `DROP` statements of the migration. Without it (the default) every `DROP` is written commented out instead — visible only with `--use-comments true`, which is also the default — and that is not limited to objects that exist only in `FROM`. It covers every drop a change to an object present on both sides needs: the `DROP ... CASCADE` ahead of re-creating a routine whose return type, arguments or kind change; the drop of a changed `CHECK`, `UNIQUE`, primary- or foreign-key constraint, index, rule, policy or operator before its new definition is added; the drop of a table that is re-created (a partition-key change, a table that becomes a view); the views, rules and SQL-standard routines dropped ahead of a column type change; foreign keys dropped ahead of `SET UNLOGGED`; and `ALTER ... DROP DEFAULT`, `DROP NOT NULL`, `DROP IDENTITY` and `DROP EXPRESSION`. The statements that depend on such a drop are still emitted and fail when they run (`already exists`, `cannot change return type of existing function`, `cannot alter type of a column used by a view or rule`, …), so the script applies only partly, and a second compare still shows those differences. Changes that are nothing but a drop and a re-create of one object are commented out whole instead, with a `use_drop=false` note, and leave that object as it is: a generated column whose expression, type or kind changes where `SET EXPRESSION` cannot do it, a view or materialized view that has to be re-created, a changed domain constraint, a range type, extended statistics, a foreign table or a publication that has to be re-created (an event trigger that has to be re-created gets only the note). What it guarantees is narrower than "nothing is removed": no `DROP` statement runs, and none of the `ALTER ... DROP` column, constraint, default, `NOT NULL`, identity or expression clauses above, nor `DETACH PARTITION` or `ALTER DEFAULT PRIVILEGES ... REVOKE`. Other removals are not drops and still run: removing a comment (`COMMENT ON ... IS NULL`) from any object but those listed next; `REVOKE` under `--grants-mode full`; taking tables out of a publication (`ALTER PUBLICATION ... SET TABLE`, or `DROP TABLE` / `DROP TABLES IN SCHEMA` when none is left); `ALTER TEXT SEARCH CONFIGURATION ... DROP MAPPING`; `OPTIONS (DROP ...)` on foreign-data wrappers, servers, user mappings and foreign tables; and `DISABLE ROW LEVEL SECURITY`. Removing a comment from a cast, collation, event trigger, foreign-data wrapper, server, publication, subscription, operator or text search configuration or dictionary is left out of the script entirely, without a note. Other statements still change the database too (a type change still rewrites a column, and rejects values that do not fit). Use it to review the destructive part of a migration and apply it by hand; use `--use-drop` for a migration that converges.

`--use-single-transaction` - use this flag to wrap resulting diff file within explicit `begin;` and `commit;` statements (i.e. single transaction).

`--use-comments {true|false}` - set to `false` to strip SQL comments from the generated script; set to `true` (default) to include comments.

`--grants-mode {ignore|addonly|full}` - controls how grants (privileges) are handled during comparison. `ignore` (default) skips grants entirely; `addonly` adds grants that exist in TO but not in FROM; `full` makes grants identical by adding missing and revoking extra.

`--output-for-production {true|false}` - set to `true` to generate a migration script that is convenient to run against a live production database (default `false` — output is unchanged). See [Production-friendly output](#production-friendly-output).

`--guard-sql-routine-bodies {true|false}` - set to `true` to emit `set check_function_bodies = false;` as the migration's first statement, so PostgreSQL does not resolve the names inside a routine body when the routine is created (default `false` — output is unchanged). See [Routine ordering](#routine-ordering).

`--max-connections {number}` - maximum number of connections in the PostgreSQL connection pool. Default: `16`. Used by all concurrent introspection queries; table metadata is pulled schema-wide in one query per resource kind (columns, indexes, constraints, triggers, policies, partition info, definitions) so connection count mostly matters for the sibling queries (extensions, sequences, routines, views, etc.) running in parallel.

`--use-cascade` - add `CASCADE` to every `DROP` statement in the clear script. **Warning:** `CASCADE` can silently drop dependent objects that live outside the selected schema(s) (e.g., foreign keys or views in other schemas referencing the dropped objects). Use only when you are certain no cross-schema dependencies should survive. Without this flag the drops follow the dependencies PostgreSQL records between the objects being dropped (see [Generate a clear (drop-all) script](#generate-a-clear-drop-all-script-for-a-database)), and the script fails cleanly where an object outside the selected schema(s) depends on one of them.

## Functionality

### Create database schema dump

```bash
pgc  --command dump --server {host} --database {database} --scheme {scheme} --output {file}
```

As a result if this command we will have a dump file with all needed information to compile delta between two databases. This file should be used for the `FROM` or `TO` sides in `compare` command.

### Create delta script between two dumps

```bash
pgc --command compare --from {from_dump} --to {to_dump} --output {file} --use-drop
```

This command comparing two dumps and produce SQL script for the `FROM` database to be equal to `TO` database after applying it.
If we add `--use-drop` argument comparer will add drop scripts for all items that non exists in target database, otherwise drop scripts will be ignored.  
By default, comparer ignore drops.

### Production-friendly output

```bash
pgc --command compare --from {from_dump} --to {to_dump} --output {file} --use-single-transaction --output-for-production
```

By default the delta script favours brevity and is meant to be applied to an idle database. With `--output-for-production true` (config key `OUTPUT_FOR_PRODUCTION=true`) the comparer instead emits a script that minimises locking on a live database:

- **Indexes are built concurrently** — `CREATE INDEX` becomes `CREATE INDEX CONCURRENTLY` (the `UNIQUE` keyword is preserved), so building an index does not block writes.
- **Partitioned tables are handled correctly** — `CONCURRENTLY` is not allowed directly on a partitioned table, so for a partitioned parent the comparer emits `CREATE INDEX ... ON ONLY {parent}` (in the transaction), then `CREATE INDEX CONCURRENTLY` on each partition followed by `ALTER INDEX ... ATTACH PARTITION` (after the transaction). When the partition layout cannot be expanded safely (no known partitions, or multi-level/sub-partitioned children) it falls back to a single non-concurrent `CREATE INDEX` on the parent and explains why in a comment.
- **Indexes are dropped concurrently** — `DROP INDEX` becomes `DROP INDEX CONCURRENTLY` (kept non-concurrent for indexes on partitioned tables, where concurrent drop is illegal).
- **Foreign keys are validated separately** — a new foreign key is added `NOT VALID` inside the transaction (a fast, metadata-only operation) and a matching `VALIDATE CONSTRAINT` (the long, scan-heavy step) is emitted afterwards so it does not hold the lock for the whole migration.
- **DDL is emitted defensively so the migration is re-runnable** — every statement PostgreSQL supports a guard for is generated idempotently, so a migration that was applied partially (e.g. a post-commit step failed) can be replayed without errors. `CREATE [UNLOGGED] TABLE`, `CREATE [UNLOGGED] SEQUENCE`, `CREATE MATERIALIZED VIEW` and `CREATE [UNIQUE] INDEX [CONCURRENTLY]` gain `IF NOT EXISTS`; `CREATE VIEW` becomes `CREATE OR REPLACE VIEW`; `ALTER TABLE ... ADD COLUMN` gains `IF NOT EXISTS`; and `ALTER TABLE ... DROP COLUMN` / `DROP CONSTRAINT` gain `IF EXISTS`. `CREATE TYPE` and `ALTER TABLE ... ADD CONSTRAINT` are left unguarded because PostgreSQL has no idempotency clause for them. The rewrite is SQL-aware (it skips string literals, quoted identifiers, comments — including commented-out drops — and dollar-quoted function bodies), so only the structural keyword of each statement is touched.

Because `CREATE/DROP INDEX CONCURRENTLY`, `VALIDATE CONSTRAINT` and `ALTER INDEX ... ATTACH PARTITION` **cannot run inside a transaction block**, every such statement is moved to a clearly marked `Production post-commit` section emitted **after** the `commit;`. The rest of the migration still runs inside the single transaction when `--use-single-transaction` is set. Each post-commit statement runs in its own implicit transaction; most are safe to re-run thanks to idempotency guards, but `ALTER INDEX ... ATTACH PARTITION` has no built-in guard, so already-successful ATTACH steps may need to be skipped when replaying the section.

`--output-for-production` defaults to `false`; when off the output is byte-for-byte identical to previous behaviour.

### Routine ordering

Routines have to be created before the routines that call them, and PostgreSQL's catalog does not record what a function body calls — `pg_depend` carries no edge for a call inside `prosrc`. So pgc infers the order by reading each body's text and looking for qualified `schema.name` references, then topologically sorting what it finds.

Text can only approximate a call. Two kinds of name are read past deliberately, because neither is resolved when the routine is created:

- **Comments.** A body that documents its relationship to another routine (`-- mirrors schema.other's rules`) names it without calling it.
- **String literals**, single-quoted or dollar-quoted. A name inside dynamic SQL is resolved when that SQL is executed, not when the routine is created.

Counting either as a dependency invents a graph edge, and an invented edge pointing back along a real one closes a cycle. A cycle cannot be topologically sorted, so the routines in it — and everything blocked behind it — fall back to plain name order, which is how a migration could come to create a routine before the one it calls (issue #240).

When a cycle does survive, pgc names the routines or views involved on **stderr**. A genuine cycle is possible: mutually recursive routines really do depend on each other, and no ordering can satisfy them.

`--guard-sql-routine-bodies true` (config `GUARD_SQL_ROUTINE_BODIES=true`) is the defensive answer to both cases. It emits

```sql
set check_function_bodies = false;
```

as the migration's first statement, which tells PostgreSQL not to resolve the names inside a routine body at `CREATE` time — so a `sql`-language routine is created even if its callee is not there yet. The trade is that a genuine mistake in a routine body is no longer caught while the migration runs; it waits until something calls it. That is why the flag is opt-in.

The statement is `SET`, not `SET LOCAL`, so it also covers the post-commit section of a production-mode script. It stays in effect for the rest of the **session**, which matters only if the same connection is reused for other work after the migration — the usual one-shot `psql -f` replay ends the session with the script. A migration that turns out to have nothing to do drops the statement again, so an empty diff stays byte-for-byte what it is with the flag off.

### Generate a clear (drop-all) script for a database

```bash
pgc --command clear --server {host} --database {database} --scheme {scheme} --output {file} --use-single-transaction --use-comments
```

This command connects to the specified database, discovers all objects in the given schema(s) and produces a SQL script that drops every found object. The drops are grouped in this order:

1. Event triggers (see below), ahead of every drop that would fire them: an event trigger that logs DDL into a table of the schema being cleared would otherwise fail each drop from the moment that table is gone
2. Views and materialized views (topologically sorted by `table_relation`; tie-break: materialized before regular, then alphabetical), together with the routines whose SQL-standard body (`BEGIN ATOMIC ... END` / `RETURN expr`) reads a relation the script drops
3. Foreign key constraints, then tables (partitions before their parents)
4. Foreign tables
5. Subscriptions, user mappings, foreign servers and foreign-data wrappers (see below)
6. Extended statistics
7. Casts and operators
8. Routines (functions, procedures, aggregates; an aggregate before its support functions)
9. Sequences
10. Types (enums, composites, domains, ranges)
11. Text search configurations, text search dictionaries and collations
12. Extensions: those installed in the selected schema(s), and every extension installed in `public`, whether `public` is selected or not (as in every earlier release)
13. Schemas

That order is then corrected wherever PostgreSQL records that one object cannot be dropped before another. The command reads those dependencies from `pg_depend` once, after it has read the schema, and moves a drop only as far as one of them requires: a domain whose `CHECK` calls a function goes before the function, a domain before the domain it is based on, a classic `INHERITS` child before its parent, a table whose rule reads a view before the view, a table whose constraint or default calls a SQL-standard function before that function, a cast or an operator before its function, an extension before the extension it requires. A schema without such dependencies gets exactly the order above; where a dependency moves a drop into another group, that group's comment banner is repeated.

A dependency cycle cannot be ordered: a table whose `CHECK` constraint, default, trigger, rule, policy or index calls a function whose SQL-standard body reads that same table, or a rule on a table that reads a view over the table. The script breaks such a cycle by dropping that part of the table first (`alter table if exists ... drop constraint if exists ...`, `alter table if exists ... alter column ... drop default`, `drop rule if exists ... on ...`, and so on; a stored generated column loses only its expression, `alter table if exists ... alter column ... drop expression if exists`, so a view reading the column does not block it), each under a `Break dependency cycle` comment. PostgreSQL 14 records a generated column's use of a function differently — dropping the function drops the column with it, so there is no cycle — and there the script drops whatever reads the column (a view) before the function instead. A cycle with no such part is left in the order above and named on stderr; without `--use-cascade` the script fails there.

Database-wide objects belong to no schema, and the script drops one only when it depends on something the script drops inside the selected schema(s): a foreign-data wrapper that uses a handler, validator or connection function of an extension installed in a selected schema or of a routine being dropped, a foreign server on such a wrapper, the user mappings and PostgreSQL 19 subscriptions (`CREATE SUBSCRIPTION ... SERVER`) on such a server, and an event trigger that calls a routine being dropped. An extension that is dropped only because it is installed in an unselected `public` does not count: `--scheme app` never drops the servers and user mappings built on a `postgres_fdw` in `public`, and its `drop extension` fails on them instead. Nor is a database-wide object dropped while something outside the selected schema(s) still depends on it — a foreign table in another schema on the server — together with the wrapper under it: dropping them could only fail, after the server's user mappings were gone. stderr names such an object, and without `--use-cascade` the drop it blocks fails. `DROP SUBSCRIPTION` of a subscription that has a replication slot connects to the publisher to drop the slot, and cannot run inside a transaction block (`--use-single-transaction`). Publications are never dropped: a table leaves the publications it belongs to when it is dropped.

An object in a schema outside the selected schema(s) that depends on one being dropped (a view reading a table, a foreign key referencing it) is not dropped: without `--use-cascade` the script fails cleanly on it.

The resulting script can be applied on another database that shares the same schema to fully remove all objects originating from it.

Optional flags:
- `--use-single-transaction` wraps the script in `BEGIN` / `COMMIT`.
- `--use-comments` (default `true`) adds explanatory comments before each drop statement.
- `--use-cascade` appends `CASCADE` to every `DROP` statement so that dependent objects outside the inspected schema(s) are removed automatically. **Use with caution** — this can drop objects you did not intend to remove (see `--use-cascade` description above).

## Configuration file

The configuration file is pretty simple key-value configuration file. All rows starts from `#` will be interpreted as a comments. Also will be ignored all empty or whitespace rows.

All configuration file should based on the following one:

```conf
# 
# PostgreSQL Comparer Configuration
# 

# FROM
FROM_HOST=localhost
FROM_DATABASE=service
FROM_SCHEME=service
FROM_SSL=true
FROM_DUMP=from.dump

# TO
TO_HOST=localhost
TO_DATABASE=service
TO_SCHEME=service
TO_SSL=false
TO_DUMP=to.dump

# OUTPUT
OUTPUT=delta.sql

# ADDITIONAL PROPERTIES
USE_DROP=true
USE_SINGLE_TRANSACTION=true
USE_COMMENTS=false
GRANTS_MODE=ignore
MAX_CONNECTIONS=16
OUTPUT_FOR_PRODUCTION=false
GUARD_SQL_ROUTINE_BODIES=false
```

`OUTPUT_FOR_PRODUCTION` (default `false`) is the configuration-file equivalent of the `--output-for-production` flag described in [Production-friendly output](#production-friendly-output).

`GUARD_SQL_ROUTINE_BODIES` (default `false`) is the configuration-file equivalent of the `--guard-sql-routine-bodies` flag described in [Routine ordering](#routine-ordering).

## Choosing `MAX_CONNECTIONS`

`MAX_CONNECTIONS` caps the connection pool the tool opens per dump. Independent introspection queries (extensions, sequences, routines, views, types, tables, etc.) are fired concurrently via `tokio::try_join!`, and the table-level data (columns, indexes, constraints, triggers, policies, partition info, definitions) is pulled with **schema-wide** queries — one query per sub-resource, independent of table count. So the connection count mostly bounds how many of the ~18 concurrent sibling queries run without queueing.

### Key constraints

- **Server budget** — never exceed half of the server's `max_connections` (check with `SHOW max_connections;`). The PostgreSQL default is 100.
- **Two pools** — when dumping both `FROM` and `TO` against the same server the combined usage is `2 × MAX_CONNECTIONS`; halve your budget per pool.
- **Memory overhead** — each PostgreSQL connection costs roughly 5–10 MB of RAM on the server.
- **Diminishing returns** — beyond ~24 connections there is nothing left to parallelize; the dump runs at most ~18 queries concurrently.

### Recommended values

| Schema size                 | Suggested `MAX_CONNECTIONS` |
|-----------------------------|-----------------------------|
| Local or low-latency server | 16 (default)                |
| High-latency / remote       | 20 – 24                     |
| Extremely small schema      | 8 – 12 (still fine)         |

The default of 16 is comfortable for most workloads. Raising it above ~24 rarely helps: the tool only has a small fixed set of sibling query batches to run.
