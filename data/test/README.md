# Database Schema Comparison Test Scripts

This folder contains two comprehensive PostgreSQL schemas designed to test all possible differences that the PGC (PostgreSQL Database Comparer) tool can detect.

## Files

- `schema_a.sql` - The "FROM" database schema
- `schema_b.sql` - The "TO" database schema
- `clear_test.sql` - Schema for testing the `clear` command (drop-all script generation)
- `clear_expected.sql` - Expected structure of the `clear` command output

## Testing Coverage

These schemas are designed to test comparison capabilities for the following PostgreSQL objects:

### 1. Schemas
- **Added**: `new_reporting_schema` in Schema B
- **Unchanged**: `test_schema`, `shared_schema`

### 2. Extensions
- **Added**: `hstore` extension in Schema B
- **Removed**: `pgcrypto` extension in Schema B
- **Unchanged**: `uuid-ossp`, `pg_trgm`, `btree_gist`, `postgres_fdw`

### 3. Custom Types

#### Enums
- **Modified**: `test_schema.status_type` — added `'suspended'` value in Schema B
- **Removed**: `test_schema.priority_type` in Schema B (including dependent routines)
- **Added**: `test_schema.contact_type` in Schema B

#### Composite Types
- **Modified**: `shared_schema.address_type` — added `region` field in Schema B
- **Modified**: `test_schema.user_profile` — added `phone` field in Schema B
- **Removed**: `test_schema.test_type_A` in Schema B
- **Added**: `test_schema.test_type_B` in Schema B

#### Domain Types
- **Modified**: `test_schema.positive_integer` — added upper limit constraint (`VALUE <= 1000000`) in Schema B

### 4. Sequences
- **Modified**: `test_schema.user_id_seq` — START changed from 1000 → 2000
- **Modified**: `shared_schema.global_counter_seq` — CACHE changed from 1 → 5
- **Modified**: `test_schema.minvalue_raise_seq` — START WITH and MINVALUE both raised from 1 → 10000000; NO CYCLE → CYCLE. The comparer must emit `RESTART WITH 10000000` alongside `START WITH 10000000` to prevent PostgreSQL from implicitly restarting from the old recorded start value (1), which would violate the new MINVALUE and produce: `ERROR: RESTART value (1) cannot be less than MINVALUE (10000000)`
- **Removed**: `test_schema.order_id_seq` in Schema B
- **Added**: `new_reporting_schema.report_id_seq` in Schema B

### 5. Tables

#### Modified Tables
- **users**: column `username` VARCHAR(50) → VARCHAR(60); added columns (`preferred_contact`, `timezone`, `two_factor_enabled`)
- **categories**: added columns (`category_code`, `icon_url`)
- **products**: column `name` VARCHAR(255) → VARCHAR(300); `price` DECIMAL(10,2) → DECIMAL(12,2); removed columns (`weight`); added columns (`barcode`, `manufacturer`, `warranty_months`, `is_featured`)
- **audit_logs**: added columns (`session_id`, `request_id`)
- **generated_pricing**: column `total` changed from plain NOT NULL to `GENERATED ALWAYS AS (quantity * unit_price) STORED`
- **identity_update_test**: identity column parameters changed (START WITH 1 → 100, INCREMENT BY 1 → 5)
- **trigger_order_test**: added column (`updated_at`)
- **special$table**: added column (`"new#col" INT`)
- **"table with spaces"**: column type changed (`TEXT` → `VARCHAR(255)`)
- **composite_fk**: FK `fk_composite` gains `ON DELETE CASCADE`
- **part_type_change_parent**: partition key type changed (`TEXT` → `UUID`; requires table recreation)
- **partition_bound_test_active**: partition bound changed (`'active'` → `'inactive'`)
- **events_2023**: changed from leaf partition to sub-partition parent (adds `PARTITION BY LIST (region)`)
- **customers** (partitioned parent): column `name` gains DEFAULT; column `email` SET NOT NULL; column `phone` added; column `legacy` dropped; constraint `chk_customers_name` modified; constraint `chk_customers_email` added — DDL must target parent only, partitions (`customers_2024`, `customers_2025`) must NOT receive column or constraint DDL
- **expenses** (partitioned parent, Issue #118): column `amount` `NUMERIC(10,2)` → `NUMERIC(15,4)`; partition key is `expense_date` — only `ALTER COLUMN` on parent, partition `expenses_2024_01` must NOT be dropped/recreated

#### Removed Tables
- **orders**: completely removed
- **order_items**: completely removed
- **drop_parent**: partitioned table removed
- **drop_child**: partition removed
- **drop_orders**: table with FK and trigger removed
- **sensor_data**: 3-level partition hierarchy removed (`sensor_data` → `sensor_data_eu` → `sensor_data_eu_1`)

#### Added Tables
- **reviews**: product reviews table with FK to products and users
- **daily_stats**: reporting table in `new_reporting_schema`
- **table1**: named primary key constraint (`pk_table1_id`)
- **user_preferences**: uses `hstore` extension type
- **data.test**: `BIGINT GENERATED ALWAYS AS IDENTITY` with explicit parameters
- **test_serial**, **test_bigserial**, **test_identity**: serial/bigserial/identity column variants
- **issue_serial_test**, **issue_identity_always_test**, **issue_identity_default_test**: auto-sequence column tests
- **data.events_2023_us**, **data.events_2023_eu**: new leaf partitions under repartitioned events_2023
- **data.metrics** hierarchy: new 3-level partitioned table (RANGE by year → LIST by type → leaf)
- **data.partition_test** + **partition_test_default**: new partitioned table with DEFAULT partition
- **data.logs_2025**: new partition added to existing `data.logs` parent
- **data.tagged_items** + partitions (**alpha**, **beta**): new partitioned table with parent index

#### Modified Tables (constraint diff only)
- **check_literal_case_test**: `chk_priority_label` modified (added `'P5-Informational'`); `chk_category_values` unchanged (tests mixed-case string literal case preservation)

### 6. Indexes
- **Added**: `idx_users_preferred_contact`, `idx_users_timezone`, `idx_products_manufacturer`, `idx_products_is_featured`, `idx_products_barcode`, `idx_reviews_*` (5 indexes), `idx_audit_logs_session_id`, `idx_audit_logs_request_id`, `idx_tagged_items_detail`, `idx_user_preferences_prefs`
- **Modified**: `idx_audit_logs_table_op_changed_at` — unique index gains `record_id` column
- **Removed**: all indexes on removed tables (`orders`, `order_items`)
- **Unchanged**: existing indexes on `users`, `products`, `audit_logs`, `logs`

### 7. Foreign Keys
- **Added**: FKs on `reviews` (to products, users), FK on `user_preferences` (to users)
- **Modified**: `composite_fk.fk_composite` gains `ON DELETE CASCADE`
- **Removed**: FKs on removed tables (`orders`, `order_items`, `drop_orders`)
- **Unchanged**: FKs on `categories`, `products`

### 8. Constraints
- **Added**: `chk_products_warranty_reasonable`, `chk_reviews_content_not_empty`, `chk_reviews_helpful_count_positive`, inline `rating` check on reviews
- **Modified**: `chk_priority_label` — added `'P5-Informational'` value in Schema B
- **Removed**: `chk_products_weight_positive` (column removed), `chk_orders_dates`, `chk_orders_delivery_dates` (table removed)
- **Unchanged**: `chk_users_email_format`, `chk_category_values` (mixed-case string literals preserved), inline checks on `products`, `audit_logs`

### 9. Functions

#### Modified Functions
- **generate_order_number()**: prefix changed `'ORD-'` → `'REF-'`
- **fn_order_from()**: default text changed; added `updated_at` assignment
- **get_users_by_status()**: return type adds `created_at` column
- **fn_dollar_from()**: dollar quoting `$b$` → `$c$`, inner text changed, added `-v2` suffix
- **lookup_username()**: flags `STABLE STRICT PARALLEL SAFE` → `VOLATILE PARALLEL UNSAFE`
- **tax_label()**: flags `IMMUTABLE PARALLEL SAFE` → `STABLE PARALLEL RESTRICTED`
- **calculate_tax()**: new parameter `currency varchar` added; logic updated
- **generate_token()**: implementation changed from `gen_random_bytes` (pgcrypto) to `md5`-based
- **get_active_usernames_sql()**: return type adds `preferred_contact` column
- **product_price_with_tax_sql()**: new parameter `p_currency varchar DEFAULT 'USD'`

#### Removed Functions
- **calculate_order_total()**: depends on removed `orders` table
- **get_user_order_count()**: depends on removed `orders` table
- **drop_fn()**: trigger function for removed `drop_orders` table
- **get_products_by_priority()**: depends on removed `priority_type` enum
- **normalize_email_from()**: FROM-only function (IMMUTABLE LEAKPROOF PARALLEL SAFE)
- **concat_agg_sfunc()**: support function for removed `concat_agg` aggregate

#### Added Functions
- **calculate_average_rating()**: average rating for a product
- **get_user_review_count()**: review count for a user
- **update_daily_stats()**: updates daily reporting stats (in `new_reporting_schema`)
- **normalize_email_to()**: IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE
- **get_user_count()**: returns user count
- **report_user_stats()**: reads `v_user_stats` view
- **r_base_value()**, **x_step_one()**, **a_middle_layer()**: routine dependency chain
- **product_agg_sfunc()**: support function for `weighted_sum` aggregate

#### Unchanged Functions
- **update_timestamp()**, **audit_trigger()**, **get_secure_setting()**, **running_sum_sfunc()**, **new_entity_id()**, **get_session_user_safe()** (with SET search_path)

#### Modified Function Configuration Parameters (SET / proconfig)
- **secure_lookup()**: FROM has `SET search_path = 'public'`; TO changes to `SET search_path = 'public, pg_temp'` and adds `SET lock_timeout = '5s'` → config-only diff triggers `CREATE OR REPLACE`

### 10. Aggregate Functions
- **Added**: `test_schema.weighted_sum(numeric, numeric)` — with support function and comment
- **Removed**: `test_schema.concat_agg(text)` — with support function
- **Unchanged**: `test_schema.running_sum(integer)`

### 11. Procedures

#### Modified Procedures
- **admin_reset_counters()**: `SECURITY DEFINER` flag removed

#### Removed Procedures
- **cleanup_old_orders()**: depends on removed `orders` table

#### Added Procedures
- **cleanup_old_reviews()**: cleans old reviews (730-day threshold)
- **print_user_stats()**: reads `v_user_stats` view
- **z_final_report()**: calls `a_middle_layer()` (dependency chain)
- **apply_secure_settings(text)**: procedure with `SET search_path = 'public, pg_temp'` and `SET lock_timeout = '5s'` (tests new routine with configuration parameters)

#### Unchanged Procedures
- **notify_event(uuid, varchar, jsonb)**: 3-param overload, identical in both schemas
- **notify_event(uuid, varchar, varchar, jsonb, jsonb)**: 5-param overload, identical in both schemas (tests overloaded routine matching by argument signature)
- **format_csv_line(varchar, varchar)**: procedure with a comma-in-string default (`DEFAULT ','`) - identical in both schemas; no diff should be emitted (Issue #154 regression test)
- **dollar_newline_test()**: procedure body contains intentional runs of 3+ consecutive blank lines inside `$$`; identical in both schemas. Tests that `--use-comments=false` newline collapsing does not corrupt dollar-quoted content (false-positive diff regression test)

### 12. Triggers
- **Added**: `trigger_reviews_update_timestamp`, `trigger_reviews_audit` on `reviews` table
- **Removed**: `trigger_orders_audit` (orders table removed), `trg_drop_orders` (drop_orders table removed)
- **Unchanged**: `trigger_users_update_timestamp`, `trigger_products_update_timestamp`, `trigger_users_audit`, `trg_order_from`

### 13. Views

#### Regular Views
- **Modified**: `product_inventory` — added `manufacturer`, `is_featured` columns; 'Low Stock' threshold changed from 10 → 5
- **Removed**: `user_order_summary` (orders table removed)
- **Added**: `user_review_summary`, `product_review_stats`, `v_user_stats`

#### Materialized Views
- **Modified**: `active_users_mat` — added `status` column
- **Removed**: `from_only_mat` (FROM-only)
- **Added**: `product_stock_mat` (TO-only)
- **Unchanged**: `user_count_mat`

### 14. Row-Level Security Policies
- **Modified**: `users_rls_select` — changed to `RESTRICTIVE`, role changed to `tenant_reader`, added `AND two_factor_enabled = TRUE` condition
- **Added**: `users_rls_update` — FOR UPDATE, TO `tenant_editor`, with cross-check on `preferred_contact`

### 15. Owner Changes
Objects change ownership from `pgc_owner_from` → `pgc_owner_to`:
- Schema: `test_schema`
- Type: `test_schema.status_type`
- Domain: `test_schema.positive_integer`
- Sequence: `test_schema.user_id_seq`
- Table: `test_schema.users`
- Function: `test_schema.update_timestamp()`
- View: `test_schema.product_inventory`
- Materialized view: `test_schema.active_users_mat`

### 16. Comments
- **Modified**: 9 comments updated (schemas, tables, types, domains, sequences, views, functions)
- **Removed**: comments on `orders` table, `get_products_by_priority()`, `concat_agg()`
- **Added**: comments on `new_reporting_schema`, `reviews` table, new columns, `weighted_sum` aggregate
- **Unchanged**: comments on `users.metadata`, `products.dimensions`

### 17. Grants (ACL)
Grant comparison test using roles `pgc_grant_reader` and `pgc_grant_writer`.

#### Unchanged Grants
- `SELECT` on `test_schema.users` → `pgc_grant_reader`
- `SELECT` on `test_schema.product_inventory` (view) → `pgc_grant_reader`
- `USAGE` on `test_schema` (schema) → `pgc_grant_reader`

#### Modified Grants
- `test_schema.users` → `pgc_grant_writer`: FROM has `SELECT, INSERT, UPDATE`; TO has `SELECT, INSERT` (UPDATE revoked)
- `test_schema.products` → `pgc_grant_reader`: FROM has `SELECT`; TO has `SELECT, INSERT` (INSERT added)

#### Added Grants
- `SELECT, UPDATE` on `test_schema.products` → `pgc_grant_writer` (new grantee)
- `SELECT` on `test_schema.product_inventory` (view) → `pgc_grant_writer` (new grantee)
- `USAGE, CREATE` on `test_schema` (schema) → `pgc_grant_writer` (new grantee)
- `EXECUTE` on `test_schema.calculate_average_rating(UUID)` → `pgc_grant_reader` (new function)

#### Removed Grants
- `USAGE` on `test_schema.user_id_seq` (sequence) → `pgc_grant_reader` (no grant in TO)
- `EXECUTE` on `test_schema.update_timestamp()` (function) → `pgc_grant_reader` (no grant in TO)

### 18. Exclusion Constraints (btree_gist)
- **Extension**: `btree_gist` added in both schemas
- **Modified**: `test_schema.reservations` — exclusion constraint unchanged, but table gains `guest_name` column in Schema B
- **Removed**: `test_schema.shift_schedule` — FROM-only table with exclusion constraint
- **Added**: `test_schema.booking_slots` — TO-only table with exclusion constraint

### 19. NULLS NOT DISTINCT (PG15+)
- **Modified**: `test_schema.unique_nulls_test` — unique constraint `uq_unique_nulls_code` changes from standard UNIQUE (allows multiple NULLs) to `UNIQUE NULLS NOT DISTINCT` (single NULL) in Schema B

### 20. NO INHERIT Constraint Flag
- **Modified**: `test_schema.categories` constraint `chk_categories_sort_order` — regular CHECK in FROM, `CHECK ... NO INHERIT` in TO

### 21. Column STORAGE and COMPRESSION (PG14+)
- **Modified**: `test_schema.storage_test` — columns `payload` and `blob` have default STORAGE in FROM; TO sets STORAGE to EXTERNAL on both. Compression (`lz4`) is commented out but can be enabled on PG14+ compiled with `--with-lz4`

### 22. SECURITY INVOKER Views (PG15+)
- **Modified**: `test_schema.security_invoker_view` — same query in both schemas; TO adds `WITH (security_invoker = true)` option

### 23. Range Types
- **Modified**: `test_schema.float_range` — FROM has `SUBTYPE_DIFF = float8mi`; TO removes it (triggers drop+recreate since ranges cannot be altered in-place)
- **Removed**: `test_schema.old_range` — FROM-only range type
- **Added**: `test_schema.int_range` — TO-only range type

### 24. Foreign Tables (postgres_fdw)
- **Extensions**: `postgres_fdw` added in both schemas; `test_foreign_server` created in both schemas
- **Modified**: `test_schema.foreign_users` — `username` VARCHAR(50) → VARCHAR(100); added `status` column in TO
- **Removed**: `test_schema.foreign_logs` — FROM-only foreign table
- **Added**: `test_schema.foreign_products` — TO-only foreign table

### 25. Extended Statistics (PG10+)
- **Modified**: `test_schema.stat_users_email_status` — FROM has `(dependencies, ndistinct)`; TO adds `mcv` kind (triggers drop+recreate)
- **Removed**: `test_schema.stat_products_old` — FROM-only statistics
- **Added**: `test_schema.stat_products_new` — TO-only statistics on products table

### 26. NOT ENFORCED Constraints (PG18+)
- **Modified**: `test_schema.products` constraint `chk_products_sku_format` — enforced CHECK in FROM; `NOT ENFORCED` in TO. Requires PostgreSQL 18+

### 27. Virtual Generated Columns (PG18+)
- **Modified**: `test_schema.virtual_gen_test` — column `full_name` is a plain NOT NULL column in FROM; becomes `GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED` in TO. On PG18+ this can test VIRTUAL generation; on earlier versions, STORED is used

### 28. UNLOGGED Tables
- **Modified**: `test_schema.unlogged_test` — regular (logged) table in FROM; UNLOGGED in TO
- Verifies `ALTER TABLE SET UNLOGGED` / `SET LOGGED` generation

### 29. Storage Parameters (reloptions)
- **Modified**: `test_schema.storage_params_test` — `fillfactor=70` in FROM; `fillfactor=90, autovacuum_enabled=false` in TO
- Verifies `ALTER TABLE RESET (...)` + `SET (...)` generation for storage parameter changes

### 30. REPLICA IDENTITY
- **Modified**: `test_schema.replica_identity_test` — REPLICA IDENTITY DEFAULT in FROM; REPLICA IDENTITY FULL in TO
- Verifies `ALTER TABLE REPLICA IDENTITY FULL/NOTHING/DEFAULT` generation

### 31. FORCE ROW LEVEL SECURITY
- **Modified**: `test_schema.force_rls_test` — RLS enabled but not forced in FROM; RLS enabled and forced in TO
- Verifies `ALTER TABLE FORCE ROW LEVEL SECURITY` / `NO FORCE ROW LEVEL SECURITY` generation

### 32. Classical Inheritance (INHERITS)
- **Tables**: `test_schema.inheritance_parent` (parent), `test_schema.inheritance_child` (child INHERITS parent)
- FROM has `child_data TEXT`; TO has `child_data VARCHAR(255)` — column type change on inherited child

### 33. Typed Tables (OF type)
- **Tables**: `test_schema.typed_table_test OF test_schema.address_type`
- Same typed table in both FROM and TO; verifies OF type clause in CREATE TABLE

### 34. Per-Column Statistics Target
- **Modified**: `test_schema.col_stats_test` — column `searchable_data` has STATISTICS 100 in FROM; STATISTICS 500 in TO
- Verifies `ALTER COLUMN SET STATISTICS` generation

### 35. Function COST and ROWS
- **Modified**: `test_schema.cost_rows_test` — COST 100 ROWS 1000 in FROM; COST 200 ROWS 500 in TO
- Verifies COST/ROWS clause changes in function CREATE OR REPLACE

### 36. Table Rules
- **Unchanged**: `test_schema.rule_products_no_delete` — `ON DELETE DO INSTEAD NOTHING` identical in both schemas; no diff expected
- **Removed**: `test_schema.rule_users_soft_delete` — FROM-only rule on users DELETE (INSTEAD UPDATE)
- **Added**: `test_schema.rule_reviews_audit` — TO-only rule on reviews INSERT (DO ALSO audit log insert)

### 37. Event Triggers
Event triggers are global (not schema-scoped). Require superuser to create.
- **Unchanged**: `test_etrig_unchanged` — fires on `ddl_command_start` via `etrig_log_ddl()`; identical in both schemas
- **Removed**: `test_etrig_from_only` — FROM-only, fires on `ddl_command_end` via `etrig_from_only_fn()`
- **Added**: `test_etrig_to_only` — TO-only, fires on `ddl_command_end` via `etrig_to_only_fn()`

### 38. Collations
- **Unchanged**: `test_schema.test_coll_unchanged` — `libc` provider, locale `C`; identical in both schemas
- **Removed**: `test_schema.test_coll_from_only` — FROM-only collation
- **Added**: `test_schema.test_coll_to_only` — TO-only collation

### 39. Text Search Objects

#### Text Search Dictionaries
- **Unchanged**: `test_schema.test_dict_unchanged` — `pg_catalog.simple` template with `STOPWORDS = english`; same in both schemas
- **Removed**: `test_schema.test_dict_from_only` — FROM-only dictionary
- **Modified**: `test_schema.test_dict_modified` — FROM has `STOPWORDS = english`; TO removes the stopwords option
- **Added**: `test_schema.test_dict_to_only` — TO-only dictionary

#### Text Search Configurations
- **Unchanged**: `test_schema.test_tsconfig_unchanged` — `pg_catalog.default` parser; same in both schemas
- **Removed**: `test_schema.test_tsconfig_from_only` — FROM-only configuration
- **Added**: `test_schema.test_tsconfig_to_only` — TO-only configuration

### 40. Casts
User-defined casts involve a source type, target type, and optionally a function.
- **Removed**: `test_schema.test_type_A → text` — FROM-only (cast disappears because `test_type_A` is FROM-only); implemented via `test_type_a_to_text()` function
- **Unchanged**: `test_schema.user_profile → text` — explicit cast using `user_profile_to_text()`; same function name and signature in both schemas
- **Added**: `test_schema.test_type_B → text` — TO-only (cast created because `test_type_B` is TO-only); implemented via `test_type_b_to_text()` function

### 41. Operators
User-defined binary operators on `(text, integer)` returning `boolean`.
- **Unchanged**: `test_schema.~<(text, integer)` — `text_shorter_than()` backing function; same in both schemas
- **Removed**: `test_schema.~>(text, integer)` — FROM-only, backed by `text_longer_than()`
- **Added**: `test_schema.~=(text, integer)` — TO-only, backed by `text_equals_length()`

### 42. Default Privileges
Default ACL entries in `test_schema` using the existing test roles.
- **Unchanged**: `pgc_grant_reader` gets `SELECT` on new tables — same in both schemas
- **Modified**: `pgc_grant_writer` gets `INSERT` only in FROM; gets `SELECT, INSERT, UPDATE` in TO

### 43. Foreign Servers and User Mappings
These test cases complement the foreign table tests (Section 24) which already use `test_foreign_server`.
The `postgres_fdw` extension and `test_foreign_server` created for Section 24 are reused here.

#### Foreign Servers
- **Unchanged**: `test_foreign_server` — same definition in both schemas (used for foreign table tests)
- **Modified**: `test_server_modified` — `host 'server-host-a', dbname 'db_a'` in FROM; `host 'server-host-b', dbname 'db_b'` in TO
- **Removed**: `test_server_from_only` — FROM-only server (`host 'legacy-host', dbname 'legacy_db'`)
- **Added**: `test_server_to_only` — TO-only server (`host 'new-host', dbname 'new_db'`)

#### User Mappings
- **Unchanged**: `PUBLIC` on `test_foreign_server` with `user 'readonly_user'` — same in both schemas
- **Removed**: `PUBLIC` on `test_server_from_only` — dropped when server is removed
- **Added**: `PUBLIC` on `test_server_to_only` with `user 'new_readonly_user'` — created with new server

### 44. Publications
Requires `wal_level = logical`. Comment out these statements if the test server does not have logical replication enabled.
- **Removed**: `test_pub_from_only` — FROM-only publication on `test_schema.users`
- **Unchanged**: `test_pub_unchanged` — publication for `test_schema.users, test_schema.products`; same in both schemas
- **Added**: `test_pub_to_only` — TO-only publication `FOR ALL TABLES`

> **Note**: Subscriptions require a logical replication slot on a source server and are not included in the test schemas.

> **Note**: Foreign data wrappers (`CREATE FOREIGN DATA WRAPPER`) require a shared library and are not testable via pure SQL. The `postgres_fdw` extension-owned FDW is excluded from comparison. Only user-created FDWs would appear, which require C-level code.

### 46. Special Test Scenarios

#### CHECK Constraint String Literal Case Preservation
- `chk_category_values` contains mixed-case string literals (`'Electronics'`, `'Home & Garden'`, `'Books'`) identical in both schemas
- Verifies that `lowercase_outside_literals()` preserves literal case so no false diff is generated
- `chk_priority_label` contains mixed-case literals and is intentionally modified in Schema B to verify real diffs are still detected

#### Dollar-Quoting
- `fn_dollar_from()` uses nested `$$` inside custom delimiters (`$b$`/`$c$`) to test correct quoting

#### Partition Scenarios
- **Type change**: `part_type_change_parent` partition key TEXT → UUID (forces recreation)
- **Leaf to sub-parent**: `events_2023` becomes sub-partition parent with new leaf partitions
- **Hierarchy removal**: `sensor_data` 3-level hierarchy removed
- **Hierarchy creation**: `metrics` new 3-level hierarchy created
- **Bound change**: `partition_bound_test_active` value `'active'` → `'inactive'`
- **New partition on existing parent**: `logs_2025` added to `data.logs`
- **New partitioned table with index**: `tagged_items` (indexes on parent must not duplicate to partitions)
- **Default partition**: `partition_test_default`
- **DDL inheritance**: `customers` parent modified (add/drop columns, alter NOT NULL/DEFAULT, add/modify constraints); partitions `customers_2024`/`customers_2025` must NOT receive inherited DDL (ADD COLUMN, DROP COLUMN, SET NOT NULL, SET DEFAULT, constraint add/drop)
- **Non-partition-key column type change** (Issue #118): `expenses` parent partitioned by `expense_date`; column `amount` changed from `NUMERIC(10,2)` → `NUMERIC(15,4)`. Only `ALTER COLUMN` on the parent must be generated; partition `expenses_2024_01` must NOT be dropped/recreated

#### Identity/Serial Columns
- Tests `SERIAL`, `BIGSERIAL`, `GENERATED ALWAYS AS IDENTITY`, `GENERATED BY DEFAULT AS IDENTITY`
- Identity parameter changes (START, INCREMENT)
- Auto-created sequences must not be emitted separately

#### Extension Object Exclusion
- Extension-owned objects (functions, types, operators) must not appear as individual creates/drops
- User-defined objects referencing extension types (e.g., `user_preferences` using `hstore`) must still be compared

#### Overloaded Routines
- `test_schema.notify_event` has two overloads (3-param and 5-param) identical in both schemas
- Verifies that overloads are matched by `(schema, name, arguments)` — not just `(schema, name)`
- No diff should be emitted for either overload

#### Routine Dependency Ordering
- View ↔ routine cross-dependencies: `get_user_count()` → `v_user_stats` → `report_user_stats()` / `print_user_stats()`
- Routine chain: `r_base_value()` → `x_step_one()` → `a_middle_layer()` → `z_final_report()`

#### Comma-in-String Default (Issue #154)
- `test_schema.format_csv_line(p_value varchar, p_delimiter varchar DEFAULT ',')` is present and identical in both schemas
- PostgreSQL stores the default separately via `pg_get_expr(proargdefaults, 0)`, which returns `','::character varying`
- The comma inside the quoted string literal must not be treated as a delimiter when splitting the defaults string
- The comparer must produce no diff for this procedure, confirming the round-trip is correct

#### Routine SET Configuration Parameters (proconfig)
- **Unchanged**: `get_session_user_safe()` has `SET search_path = 'public, pg_temp'` in both schemas — no diff expected
- **Modified config**: `secure_lookup(key text)` has `SET search_path = 'public'` in FROM; TO changes to `SET search_path = 'public, pg_temp'` and adds `SET lock_timeout = '5s'` — `CREATE OR REPLACE` with new SET clauses expected
- **New with config**: `apply_secure_settings(IN pvalue text)` exists only in TO with `SET search_path = 'public, pg_temp'` and `SET lock_timeout = '5s'` — `CREATE OR REPLACE` with SET clauses expected
- PostgreSQL stores these in `pg_proc.proconfig` as an array (e.g. `{search_path=public\, pg_temp,lock_timeout=5s}`)

---

## Clear Command Test (`clear_test.sql`)

The `clear_test.sql` file creates a self-contained set of database objects across two schemas (`clear_app` and `clear_shared`) to verify the `pgc --command clear` drop-all script generation.

### Objects Created

| Object Type          | Schema         | Count | Names                                                                                    |
|----------------------|----------------|-------|------------------------------------------------------------------------------------------|
| Schemas              | —              | 2     | `clear_app`, `clear_shared`                                                              |
| Extensions           | public         | 2     | `uuid-ossp`, `pg_trgm`                                                                   |
| Enum Types           | clear_app      | 1     | `order_status`                                                                           |
| Composite Types      | clear_app      | 1     | `full_name`                                                                              |
| Domain Types         | clear_app      | 1     | `positive_int`                                                                           |
| Sequences            | both           | 2     | `customer_id_seq`, `audit_id_seq`                                                        |
| Tables               | both           | 6     | `customers`, `categories`, `orders`, `order_items`, `audit_log`, `employees`              |
| Foreign Keys         | clear_app      | 4     | on `orders` (×2), `order_items`, `employees` (self-ref)                                  |
| Indexes              | both           | 11    | various B-tree and GIN indexes                                                           |
| Functions            | both           | 4     | `update_timestamp`, `get_customer_order_total`, `format_audit_entry`, `active_customer_count` |
| Procedures           | clear_app      | 1     | `cleanup_old_orders`                                                                     |
| Triggers             | clear_app      | 2     | `trg_customers_timestamp`, `trg_orders_timestamp`                                        |
| Views                | clear_app      | 2     | `v_customer_summary`, `v_top_customers` (depends on first)                               |
| Materialized Views   | both           | 2     | `mv_daily_orders`, `mv_audit_stats`                                                      |
| Comments             | both           | 8     | on schemas, tables, columns, functions, views                                            |

### Dependency Chains Tested

- **FK chain**: `order_items` → `orders` → `customers`, `orders` → `categories`
- **Self-reference**: `employees.manager_id` → `employees.id`
- **View chain**: `v_top_customers` → `v_customer_summary` → `customers` + `orders`
- **Trigger → function**: `trg_customers_timestamp` → `update_timestamp()`
- **Sequence → table**: `customer_id_seq` → `customers.id`
- **Type → table**: `order_status`, `full_name`, `positive_int` → `customers`

### Expected Drop Order

The generated clear script must drop objects in this order to avoid dependency errors:

1. **Views** (topologically sorted by `table_relation`; tie-break: materialized before regular, then alphabetical by `schema.name`)
   - `v_top_customers` depends on `v_customer_summary`, so it is dropped first
   - Materialized views with no view-dependencies appear before regular views at the same level
2. **Foreign key constraints** (all FKs across all tables)
3. **Tables** (`customers`, `categories`, `orders`, `order_items`, `audit_log`, `employees`)
4. **Routines** (`update_timestamp`, `get_customer_order_total`, `format_audit_entry`, `active_customer_count`, `cleanup_old_orders`)
5. **Sequences** (`customer_id_seq`, `audit_id_seq`)
6. **Types** (`order_status`, `full_name`, `positive_int`)
7. **Extensions** (`uuid-ossp`, `pg_trgm`)
8. **Schemas** (`clear_app`, `clear_shared`)

### How to Run

```bash
# 1. Create a test database and apply the schema
createdb pgc_clear_test
psql -d pgc_clear_test -f data/test/clear_test.sql

# 2. Generate the clear script
pgc --command clear \
    --database pgc_clear_test \
    --scheme "clear_app|clear_shared" \
    --output data/test/clear_output.sql \
    --use-single-transaction \
    --use-comments

# 3. Review the output against the expected structure
diff data/test/clear_expected.sql data/test/clear_output.sql

# 4. Apply the clear script to verify it executes cleanly
psql -d pgc_clear_test -f data/test/clear_output.sql

# 5. Verify that no objects remain
psql -d pgc_clear_test -c "
    SELECT schemaname, tablename FROM pg_tables
    WHERE schemaname IN ('clear_app', 'clear_shared');
"
# Expected: 0 rows

# 6. Cleanup
dropdb pgc_clear_test
```

### Validation Checklist

- [ ] All materialized views are dropped before regular views
- [ ] All regular views are dropped (inter-view dependencies respected)
- [ ] All foreign keys are dropped before their parent tables
- [ ] All tables are dropped
- [ ] All functions and procedures are dropped
- [ ] All sequences are dropped (including those used by SERIAL columns)
- [ ] All custom types (enum, composite, domain) are dropped
- [ ] All extensions are dropped
- [ ] All schemas are dropped
- [ ] Script executes without errors when `--use-single-transaction` is set
- [ ] Script is idempotent (uses `IF EXISTS` on all drop statements)
- [ ] Comments option controls presence of `/* ... */` annotations

---

## Usage

To test the comparison tool with these schemas:

1. Create two separate PostgreSQL databases
2. Run `schema_a.sql` on the first database (FROM)
3. Run `schema_b.sql` on the second database (TO)
4. Use the PGC tool to compare the schemas:

```bash
# Create dumps
pgc --command dump --database db_a --scheme "test_schema|shared_schema|new_reporting_schema|data" --output schema_a.dump
pgc --command dump --database db_b --scheme "test_schema|shared_schema|new_reporting_schema|data" --output schema_b.dump

# Compare
pgc --command compare --from schema_a.dump --to schema_b.dump --output comparison_result.sql

# Compare without comments
pgc --command compare --from schema_a.dump --to schema_b.dump --output comparison_result.sql --use-comments false
```

The resulting comparison script should contain SQL statements to transform the "FROM" database structure to match the "TO" database structure, including all the differences listed above.

## Expected Comparison Results

The comparison should detect and generate SQL for:
- Dropping removed extensions, types, tables, functions, aggregates, etc.
- Adding new schemas, extensions, types, tables, functions, aggregates, etc.
- Modifying existing structures (ALTER statements for columns, constraints, identity parameters, owner changes)
- Updating constraints, indexes, triggers, views, materialized views, and RLS policies
- Handling dependencies correctly (drop in reverse dependency order, create in dependency order)
- Correctly handling partitioned tables (creation order: parent → mid → leaf; drop order: leaf → mid → parent)
- Not emitting explicit CREATE INDEX for partitions (PostgreSQL propagates parent indexes automatically)
- Not emitting inherited DDL (column add/drop/alter, non-FK constraint changes) for partition children (PostgreSQL propagates structural changes from parent automatically)
- Not emitting extension-owned objects as individual creates/drops
- Using serial/bigserial types instead of separate sequences where appropriate
- Stripping SQL comments from output when `--use-comments false` is specified (preserving comments inside function bodies)
- Preserving blank lines inside dollar-quoted bodies when `--use-comments false` is specified (no false-positive diffs)
- Handling exclusion constraints (create, drop, alter via table changes)
- Detecting NULLS NOT DISTINCT changes on unique constraints (PG15+)
- Detecting NO INHERIT flag changes on CHECK constraints
- Detecting column STORAGE and COMPRESSION changes (PG14+)
- Handling SECURITY INVOKER view option changes (PG15+)
- Handling range type changes via drop+recreate (ranges cannot be altered in-place)
- Detecting routine SET configuration parameter (proconfig) changes, additions, and removals
- Creating, dropping, and altering foreign tables (column add/drop/alter, server changes, options)
- Creating, dropping, and altering extended statistics (kind changes via drop+recreate)
- Detecting NOT ENFORCED constraint flag changes (PG18+)
- Handling virtual/stored generated column transitions (PG18+)
- Detecting UNLOGGED ↔ LOGGED table persistence changes
- Detecting storage parameters (reloptions/WITH clause) changes (fillfactor, autovacuum settings, etc.)
- Detecting REPLICA IDENTITY changes (DEFAULT, NOTHING, FULL)
- Detecting FORCE ROW LEVEL SECURITY changes
- Handling classical inheritance (INHERITS) in table creation
- Handling typed tables (OF type) in table creation
- Detecting per-column statistics target changes (SET STATISTICS)
- Detecting function/procedure COST and ROWS clause changes
- Handling SUPPORT function and TRANSFORM FOR TYPE clauses on routines
- Creating, dropping, and altering table rules (ON INSERT/UPDATE/DELETE, DO INSTEAD / DO ALSO)
- Creating, dropping, and altering event triggers (global scope, ddl_command_start/end/sql_drop)
- Creating, dropping, and altering collations (libc/icu provider, locale, deterministic flag)
- Creating, dropping, and altering text search dictionaries (template, options)
- Creating, dropping, and altering text search configurations (parser, token mappings)
- Creating and dropping user-defined casts (function-based, binary-coercible, I/O conversion)
- Creating and dropping user-defined operators (binary infix, backing function, operand types)
- Creating, modifying, and revoking default privileges (ALTER DEFAULT PRIVILEGES per role/schema/object type)
- Creating, modifying, and dropping foreign servers and user mappings (OPTIONS changes, add/remove entries)
- Creating, modifying, and dropping publications (FOR TABLE list, FOR ALL TABLES, publish operations)
