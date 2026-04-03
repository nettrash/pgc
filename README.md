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

`--command {dump|compare}` - the command name, dump - to create a dump file, compare - to compare two dumps.

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

`--max-connections {number}` - maximum number of connections in the PostgreSQL connection pool. Default: `8`.

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
MAX_CONNECTIONS=8
```

## Choosing `MAX_CONNECTIONS`

The `MAX_CONNECTIONS` setting controls how many concurrent PostgreSQL connections the tool opens per dump. During a dump the tool runs several independent queries in parallel (extensions, sequences, routines, types, views) and additionally fills each table concurrently — so the connection count directly affects wall-clock time on remote or high-latency servers.

### Key constraints

- **Server budget** — never exceed half of the server's `max_connections` (check with `SHOW max_connections;`). The PostgreSQL default is 100.
- **Two pools** — when dumping both `FROM` and `TO` against the same server the combined usage is `2 × MAX_CONNECTIONS`; halve your budget per pool.
- **Memory overhead** — each PostgreSQL connection costs roughly 5–10 MB of RAM on the server.
- **Diminishing returns** — beyond ~24–32 connections, query planner contention and lock overhead typically negate the parallelism gains.

### Recommended values

| Tables in schema | Suggested `MAX_CONNECTIONS` |
|------------------|-----------------------------|
| < 20             | 8 (default)                 |
| 20 – 50          | 12 – 16                     |
| 50 – 200         | 16 – 24                     |
| 200+             | 24 – 32                     |

### Formula

As a rule of thumb:

```
max_connections = min(max(⌈tables / 2⌉ + 5, 8), pg_max_connections / 2, 32)
```

Where `tables` is the number of tables in the target schema(s) and `pg_max_connections` is the server's `max_connections` value.

You can query both values at connect time:

```sql
-- Server limit
SHOW max_connections;

-- Table count for the target schema(s)
SELECT count(*) FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
```
