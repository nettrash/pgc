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

`--use-drop` - specify this argument if you want to use DROPs in output script, otherwise no DROPs will be used.

`--use-single-transaction` - use this flag to wrap resulting diff file within explicit `begin;` and `commit;` statements (i.e. single transaction).

`--use-comments {true|false}` - set to `false` to strip SQL comments from the generated script; set to `true` (default) to include comments.

`--grants-mode {ignore|addonly|full}` - controls how grants (privileges) are handled during comparison. `ignore` (default) skips grants entirely; `addonly` adds grants that exist in TO but not in FROM; `full` makes grants identical by adding missing and revoking extra.

`--max-connections {number}` - maximum number of connections in the PostgreSQL connection pool. Default: `16`. Used by all concurrent introspection queries; table metadata is pulled schema-wide in one query per resource kind (columns, indexes, constraints, triggers, policies, partition info, definitions) so connection count mostly matters for the sibling queries (extensions, sequences, routines, views, etc.) running in parallel.

`--use-cascade` - add `CASCADE` to every `DROP` statement in the clear script. **Warning:** `CASCADE` can silently drop dependent objects that live outside the selected schema(s) (e.g., foreign keys or views in other schemas referencing the dropped objects). Use only when you are certain no cross-schema dependencies should survive. Without this flag the generated drops rely on the explicit dependency ordering and will fail cleanly if unresolved dependencies exist.

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

### Generate a clear (drop-all) script for a database

```bash
pgc --command clear --server {host} --database {database} --scheme {scheme} --output {file} --use-single-transaction --use-comments
```

This command connects to the specified database, discovers all objects in the given schema(s) and produces a SQL script that drops every found object in dependency-safe order:

1. Views (topologically sorted by `table_relation`; tie-break: materialized before regular, then alphabetical)
2. Foreign key constraints
3. Tables
4. Routines (functions, procedures, aggregates)
5. Sequences
6. Types (enums, composites, domains)
7. Extensions
8. Schemas

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
```

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
