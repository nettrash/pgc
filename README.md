# PostgreSQL Database Comparer

![rust workflow](https://github.com/nettrash/pgc/actions/workflows/rust.yml/badge.svg)

## Story

We're faced with vital need to have a proper working free tool for comparing two PostgreSQL databases and preparing the delta as SQL script.

We have multiple PostgreSQL database repositories and need to have a properly working deployment pipelines for our databases.

## How to build

```bash
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

## Functionality

### Create database schema dump

```bash
pgc  --command dump --server {host} --database {database} --scheme {scheme} --output {file}
```

As a result if this command we will have a dump file with all needed information to compile delta between two databases. This file should be used for the `FROM` or `TO` sides in `compare` command.

### Create delta script between two dumps

```bash
pgc --command compare --from {from_dump} --to {to_dump} --output {file}
```

This command comparing two dumps and produce SQL script for the `FROM` database to be equal to `TO` database after applying it.

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
```
