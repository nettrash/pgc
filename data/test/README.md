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
- **Added**: `test_order` (issue #180 FK-ordered SET LOGGED/UNLOGGED) in both schemas — entire chain flips from LOGGED in Schema A to UNLOGGED in Schema B
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

#### Inline NOT NULL is not a named constraint (Issue #218)
- **notnull_recreate**: FROM has `nn_col` with inline NOT NULL and a nullable `null_col`; TO drops `nn_col` and makes `null_col` NOT NULL. On PG18 a column's inline NOT NULL is surfaced in `pg_constraint` as the auto-generated `{table}_{col}_not_null` (contype='n'); on PG14–17 it lives only in `pg_attribute.attnotnull`. The diff must be exactly `alter column null_col set not null` and `drop column nn_col` — never `ADD`/`DROP CONSTRAINT` for the invented name. `DROP CONSTRAINT ..._not_null` errors on PG14–17 ("constraint does not exist"), and `ADD CONSTRAINT ... NOT NULL` is PG18-only syntax that leaves a named constraint the source never declared, so the second diff would keep re-emitting it. This case only bites when the dump is taken from PG18, so the round-trip must close on the PG18 leg of the matrix specifically

### 6. Indexes
- **Added**: `idx_users_preferred_contact`, `idx_users_timezone`, `idx_products_manufacturer`, `idx_products_is_featured`, `idx_products_barcode`, `idx_reviews_*` (5 indexes), `idx_audit_logs_session_id`, `idx_audit_logs_request_id`, `idx_tagged_items_detail`, `idx_user_preferences_prefs`
- **Modified**: `idx_audit_logs_table_op_changed_at` — unique index gains `record_id` column
- **Removed**: all indexes on removed tables (`orders`, `order_items`)
- **Unchanged**: existing indexes on `users`, `products`, `audit_logs`, `logs`
- **Partial index with a non-idempotent predicate** (Issue #226): `ix_innorm_trust` (TO-only) has a `WHERE action_type IN ('FOO','BAR')` predicate. PostgreSQL deparses the `IN`-list into an array-level cast on first render and an element-level cast once re-parsed; comparing the raw `indexdef` re-emits `DROP`+`CREATE` every run. The predicate is canonicalized before comparison so the second diff is empty. Paired with the `innorm_mv` matview (§13) which exercises the same non-idempotency in a view definition
- **Index on a partitioned parent** (Issue #223): `idx_partidx_value` is added to the existing partitioned parent `partidx` (two partitions, identical in both schemas). `pg_get_indexdef` renders such an index with `ON ONLY`, which builds only the invalid metadata index on the parent. The default (non-production) output must emit a plain `CREATE INDEX ... ON` so PostgreSQL builds and attaches every partition's index and the parent is valid immediately. The round-2 diff cannot catch this on its own — an invalid-but-present index still round-trips empty — so the integration job additionally asserts that no index is left `indisvalid = false` after applying the migration

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
- **Typmod IN-list** (Issue #226): `chk_innorm_typmod` (TO-only) — `code IN ('A'::varchar(10), 'B'::varchar(10))`. The explicit typmod survives both pretty `pg_get_constraintdef` renderings and flips between the array-level (`ARRAY[...]::text[]`) and element-level (`ARRAY['A'::character varying(10)::text, ...]`) cast forms exactly like the bare-varchar IN-list, so the canonicalizer must converge the typmod'd renderings too or the constraint re-emits `DROP`+`ADD` on every run. Companion to the `innorm_mv`/`ix_innorm_trust` cases (§6, §13)

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
- **cascade_compute(integer)**: return type `INTEGER` → `BIGINT` — return type change requires `DROP FUNCTION ... CASCADE` (PostgreSQL has no `ALTER FUNCTION` for return types). The argument list (`integer`) is unchanged, so the function's signature in PostgreSQL terms is identical between FROM and TO; the cascade still fires. Exercises Phase 7 of `compare_routines_and_views`, which must re-emit every dependent (CHECK / functional index / generated column / DEFAULT / RLS policy) silently dropped by CASCADE; see _CASCADE-Drop Dependent Recreation (Issue #179)_ in section 46.
- **cascade_part_fn(text)**: same return-type-change cascade, but the dependent is a functional index on the **partitioned parent** `cascade_part`. Phase 7's recreate must leave that index valid (guarded by the integration job's `indisvalid` assertion): default mode emits a plain `CREATE INDEX` that builds and attaches every partition, and production mode routes the recreate through the same `ON ONLY` + per-partition `CONCURRENTLY`/`ATTACH PARTITION` split as any other index creation — a plain blocking build (or, before issue #223, a permanently invalid `ON ONLY` parent) is not acceptable there.

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
- **Incompatible column change** (Issue #227): `v227_base` gains `kind` inserted *before* `profile_id`. `CREATE OR REPLACE VIEW` only allows appending columns at the end (inserting/reordering/renaming/retyping fails with "cannot change name of view column"), so the diff must emit `DROP VIEW` + `CREATE` instead. The textually unchanged dependent `v227_dep` reads `v227_base`, and `DROP VIEW` runs without `CASCADE`, so the dependent must be pulled into the drop set, dropped first, and recreated after. Compatibility is decided from per-view column data (name, type incl. typmod, collation) captured at dump time; dumps from older pgc versions carry no column data and keep the historical `OR REPLACE` behavior
- **Modified**: `vw_from_function` — body gains a `WHERE` clause. The view selects
  only from `fn_only_view_source()` and never touches a table, so it has no rows in
  `information_schema.view_table_usage`; dumping views through a join against that
  table silently omitted the view and made the change undetectable (issue #219).
  `vw_from_function` itself reads no relation, so its `table_relation` is empty —
  the case the old join could not represent at all. `vw_on_function_view` selects
  from it and so carries `test_schema.vw_from_function`, covering the view→view
  dependency edge that has to survive on top of a view with no table of its own.
- **Unchanged**: `vw_with_rule` — a view carrying a `DO INSTEAD` rule that writes to
  `vw_rule_audit`, a table its definition never reads. `table_relation` must list only
  `vw_rule_base`: the dependency walk follows the view's `_RETURN` rule alone, and
  counting the targets of user-defined rules would invent drop-ordering edges. Its base
  table is identical in both schemas on purpose — see the note in `schema_a.sql`.
- **Modified**: `vw_check_opt` — `WITH CHECK OPTION` goes from `LOCAL` to `CASCADED`,
  driving the `CREATE OR REPLACE` path. Like `WITH NO DATA` (issue #220) this is a
  trailing clause, so it has to be written inside the statement; emitted after the
  definition's semicolon it parses as a separate statement and is a syntax error.

#### Materialized Views
- **Modified**: `active_users_mat` — added `status` column
- **Removed**: `from_only_mat` (FROM-only)
- **Added**: `product_stock_mat` (TO-only)
- **Unchanged**: `user_count_mat`
- **Added**: `mv_no_data` (TO-only) — created `WITH NO DATA` and never refreshed. The
  clause is not part of the stored definition; it survives only as
  `pg_class.relispopulated`, so a diff built from the definition alone recreates the
  view populated and applying it runs the query the clause exists to defer (issue #220).
  The generated `CREATE MATERIALIZED VIEW` must carry `WITH NO DATA`, and it must sit
  before the statement's terminating semicolon rather than after it.
- **Unchanged**: `mv_with_data` — a populated materialized view over the same base
  table, which must never acquire a `WITH NO DATA` clause.
- **Added**: `innorm_mv` (TO-only) — a materialized view whose `WHERE` uses
  `label IN ('FOO','BAR')` (issue #226). PostgreSQL deparses an `IN`-list as an
  array-level cast `(ARRAY[...])::text[]` on first render, then as an element-level
  cast `ARRAY[(...)::text, ...]` once that form is re-parsed. The two are equivalent
  but differ textually, so comparing the raw definition re-emits `DROP`+`CREATE` every
  run. Definitions are canonicalized (array-level cast rewritten to the element-level
  fixed point) before hashing/comparison, so the second diff is empty. The base table
  `innorm` is identical in both schemas; the matview exists only in TO so the diff
  creates it and applying it re-parses the expression. The companion partial index
  `ix_innorm_trust` (§6) exercises the same non-idempotency in an index predicate.
- **Indexes on a materialized view** (issue #235): `pg_indexes` rows for a
  materialized view were only ever distributed into the dump's *table* list, so they
  matched nothing and never reached the dump. Nothing re-emitted them either, and
  because `DROP MATERIALIZED VIEW` takes the indexes with it, a recreated view came
  back unindexed — silently, since neither dump carried the indexes and the second
  diff was therefore empty. Three views over the shared base table `mv235_base` cover
  the paths:
  - **Modified**: `mv235_recreated` — gains the `amount` column, so it is dropped and
    rebuilt; its unique index, its partial index and that index's comment must all be
    recreated with it.
  - **Unchanged**: `mv235_stable` — same definition in both, so the view survives and
    its indexes are reconciled in place: `ix_mv235_stable_drop` removed,
    `ix_mv235_stable_redef` redefined `ASC` → `DESC` (drop + rebuild, not a
    comment-only edit), `ix_mv235_stable_cmt` re-commented without touching the index,
    and `ix_mv235_stable_added` created. Index changes stay out of `View::hash` on
    purpose: hashing them would turn "an index was added" into a full rebuild of the
    view's contents.
  - **Added**: `mv235_new` (TO-only) — its index is emitted with the initial CREATE.

  In production mode every one of these builds runs `CREATE INDEX CONCURRENTLY` (and
  drops run `DROP INDEX CONCURRENTLY`) in the post-commit section, exactly as a
  table's indexes do.

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

#### Former-owner explicit grant after owner change (v1.0.18 regression)
- `SELECT` on `test_schema.users` → `pgc_owner_from` (TO-only): the table's owner changes from `pgc_owner_from` to `pgc_owner_to`, and the TO side retains an explicit grant to the previous owner. Pre-v1.0.18 the comparer filtered both old and new owners as implicit privilege holders and silently dropped this grant from the diff; post-fix only the TO-side owner is filtered, so the grant survives.

#### Foreign table grants (v1.0.18 regression)
- `SELECT` on `test_schema.foreign_users` → `pgc_grant_reader` (TO-only): pre-v1.0.18 `FOREIGN TABLE` ACL parsing fell through to the empty default privilege set, so any explicit grant on a foreign table was silently filtered out of the diff. Post-fix `FOREIGN TABLE` shares the `TABLE` privilege set (`SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, MAINTAIN`).
- `MAINTAIN` on `test_schema.foreign_users` → `pgc_grant_writer` (commented out, PG17+): uncomment on PG17+ to additionally exercise the new MAINTAIN privilege bit on a foreign table.

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
- On PG19 the flag change is an in-place `ALTER TABLE ... ALTER CONSTRAINT chk_products_sku_format NOT ENFORCED`; on PG18 it is a drop + re-add (see section 56)

### 27. Virtual Generated Columns (PG18+)
- **Modified**: `test_schema.virtual_gen_test` — column `full_name` is a plain NOT NULL column in FROM; becomes `GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED` in TO. On PG18+ this can test VIRTUAL generation; on earlier versions, STORED is used

### 28. UNLOGGED Tables
- **Modified**: `test_schema.unlogged_test` — regular (logged) table in FROM; UNLOGGED in TO
- Verifies `ALTER TABLE SET UNLOGGED` / `SET LOGGED` generation
- **FK-chain conversion (Issue #180)**: `test_order.grandparent`, `test_order.parent`, `test_order.child` — three LOGGED tables with FK chain `child -> parent -> grandparent` in Schema A; all three UNLOGGED in Schema B. Verifies the comparer emits `SET UNLOGGED` in FK-leaf-first order (`child`, then `parent`, then `grandparent`); the alphabetical order PostgreSQL would otherwise reject with `ERROR: could not change table "grandparent" to unlogged because it references logged table "parent"`. Each table's `serial` PK auto-creates an owned sequence (`*_id_seq`) — these must NOT receive an explicit `ALTER SEQUENCE ... SET UNLOGGED`, since `ALTER TABLE SET UNLOGGED` propagates to owned sequences automatically.
- **FK-chain conversion with unqualified `pg_get_constraintdef` targets (Issue #190)**: `public.persistence_chain_parent` and `public.persistence_chain_child` — two LOGGED tables with FK chain `child -> parent` in Schema A; both UNLOGGED in Schema B. The companion to the `test_order` case above, deliberately anchored in `public` so that `pg_get_constraintdef` deparses the FK as `REFERENCES persistence_chain_parent(id)` (unqualified — `public` is on the role's default `search_path`). Before the fix, `parse_fk_referenced_table` returned None for the unqualified form, the FK was dropped from the persistence-flip adjacency, and the `SET UNLOGGED` ordering fell back to alphabetical — exactly the order PostgreSQL rejects (`could not change table "persistence_chain_parent" to unlogged because it is referenced by ...`). The fix resolves unqualified targets against the FK owner's schema (`public` in this case), and this fixture verifies the integration path that the unit tests in `core_tests.rs` can only approximate.
- **FK-cycle persistence conversion (Issue #191)**: `test_order.cycle_a` and `test_order.cycle_b` — two LOGGED tables with mutual `DEFERRABLE INITIALLY DEFERRED` FKs (`cycle_a → cycle_b` and `cycle_b → cycle_a`) in Schema A; both UNLOGGED in Schema B. PostgreSQL only permits this shape with deferrable FKs (otherwise the first INSERT on either side has nothing to reference), and the cycle has NO valid SET LOGGED|UNLOGGED order — `SET UNLOGGED cycle_a` requires `cycle_b` to already be UNLOGGED and vice versa. Before the fix, `kahn_toposort` silently appended cyclic nodes alphabetically, the migration emitted the SETs in that order, and PG rejected the second SET at apply time. The fix detects the cycle, emits `ALTER TABLE … DROP CONSTRAINT` for both FKs BEFORE the SETs, runs the SETs (order no longer matters), then re-emits the FKs from their TO definitions AFTER. This fixture exercises that drop+SET+re-add pattern against PG's real deparser output so the integration round-trip catches any syntax mismatch the unit tests miss.

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
- **Modified**: `test_schema.test_dict_modified` — FROM has `STOPWORDS = english`; TO removes the stopwords option. This is an options-only change. It is emitted in place as `ALTER TEXT SEARCH DICTIONARY test_schema.test_dict_modified (stopwords);` (a bare option name removes that option), not as `DROP ... CASCADE` + `CREATE`, and it goes out before the tables (see section 47).
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

#### Membership and options (`pg19_pub_*`)
These tables are identical in both schemas:
- `test_schema.tbl_pg19_pub_items`, `tbl_pg19_pub_orders` and `"tbl_pg19_pub_Mixed"`;
- the partitioned `tbl_pg19_pub_events`, with partition `tbl_pg19_pub_events_a`;
- the classical-inheritance pair `tbl_pg19_pub_parent` / `tbl_pg19_pub_child`.

Two tables differ:
- `tbl_pg19_pub_rebuilt` changes its partition key (RANGE → LIST), so the migration drops and recreates it.
- `tbl_pg19_pub_dropped` exists only in FROM.

A statement the server cannot parse fails harmlessly under `ON_ERROR_STOP=0` and leaves that publication out of one side.

All versions:
- **Modified**: `pg19_pub_all_to_list` goes from `FOR ALL TABLES` to `FOR TABLE`, and `pg19_pub_all_to_empty` goes from `FOR ALL TABLES` to empty. No ALTER can leave FOR ALL TABLES, so both are dropped and recreated. Before this change the first emitted a failing `SET TABLE` on every run and the second emitted nothing.
- **Modified**: `pg19_pub_list_to_empty` goes from a table list to empty with `ALTER PUBLICATION ... DROP TABLE ONLY` for the members still there. The FROM-only `tbl_pg19_pub_dropped` is left out, because its row went with the table.
- **Modified**: `pg19_pub_empty_to_all` and `pg19_pub_list_to_all` go from empty or a list to `FOR ALL TABLES`. Before PG19 that is a drop+recreate. On PG19 it happens in place: `DROP TABLE ONLY` for the members, then `SET ALL TABLES`.
- **Unchanged, member recreated**: `pg19_pub_rebuilt_member` has the same list in both schemas. `tbl_pg19_pub_rebuilt` is recreated by the migration and loses its `pg_publication_rel` row, so `SET TABLE` puts it back.
- **Modified**: `pg19_pub_only_parent_list` gains the inheritance parent alone (`TABLE ONLY`). The member is written `SET TABLE ..., ONLY test_schema.tbl_pg19_pub_parent`, because without `ONLY` the child would become a member too.

PG15+:
- **Modified**: `pg19_pub_filter_change` changes its column list from `(id, qty)` to `(id, note)` and its row filter from `qty > 0` to `qty > 10`. `pg19_pub_filter_drop` loses its row filter, and its other member is the quoted `"tbl_pg19_pub_Mixed"`. Both use `SET TABLE`.
- **Modified**: `pg19_pub_list_to_schema` goes from `FOR TABLE` to `FOR TABLES IN SCHEMA shared_schema`. `pg19_pub_schema_to_mixed` goes from a schema to `TABLE ..., TABLES IN SCHEMA ...`. `pg19_pub_schema_to_empty` is emptied with `DROP TABLES IN SCHEMA`.
- **Modified**: `pg19_pub_viaroot_on` and `pg19_pub_viaroot_off` add or remove a row filter on the partitioned `tbl_pg19_pub_events` together with `publish_via_partition_root`. The option is set before the filter when it is turned on, and after the filter is removed when it is turned off.
- **Modified**: `pg19_pub_only_parent_filter` gives the inheritance parent alone a row filter with `SET TABLE ONLY ... WHERE (...)`.
- **Added**: `pg19_pub_parent_filter_to_only` is created recursively in TO (`FOR TABLE parent WHERE (...)`), so it has a filtered row for the parent and one for the child. pgc recreates it as `FOR TABLE child WHERE (...), ONLY parent WHERE (...)`. Without `ONLY` the server refuses it: "conflicting or redundant WHERE clauses".

PG18+:
- **Modified**: `pg19_pub_gencols_on` and `pg19_pub_gencols_off` switch `publish_generated_columns` between `none` and `stored`.

PG19+:
- **Removed / Unchanged / Added**: `pg19_pub_seq_from_only`, `pg19_pub_seq_unchanged` and `pg19_pub_seq_to_only` are `FOR ALL SEQUENCES` publications.
- **Modified** in place with `ALTER PUBLICATION ... SET ALL ...`:
  - `pg19_pub_seq_add`: `ALL TABLES` → `ALL TABLES, ALL SEQUENCES`;
  - `pg19_pub_seq_remove`: the reverse;
  - `pg19_pub_seq_to_tables`: `ALL SEQUENCES` → `ALL TABLES`, with a publish change and a new comment.
- **Modified**: `pg19_pub_seq_to_list` goes from `ALL SEQUENCES` to `FOR TABLE`, which needs a drop+recreate. `pg19_pub_list_to_seq` goes from `FOR TABLE` to `ALL SEQUENCES` with `DROP TABLE ONLY` and then `SET ALL SEQUENCES`.
- **`FOR ALL TABLES EXCEPT (TABLE ONLY ...)`**:
  - `pg19_pub_except_unchanged`: unchanged;
  - `pg19_pub_except_change`: the list is replaced;
  - `pg19_pub_except_clear`: `EXCEPT (...), ALL SEQUENCES` → `ALL TABLES, ALL SEQUENCES`;
  - `pg19_pub_except_add`: plain `FOR ALL TABLES` → EXCEPT;
  - `pg19_pub_except_to_only`: TO-only, EXCEPT on a partitioned root plus `ALL SEQUENCES`.
- **Unchanged, EXCEPT table recreated**: in `pg19_pub_except_rebuilt`, `tbl_pg19_pub_rebuilt` loses its EXCEPT row when it is recreated, and `SET ALL TABLES EXCEPT (...)` puts it back.
- **Modified**: `pg19_pub_mixed_to_except` goes from `FOR TABLE ... WHERE (...), TABLES IN SCHEMA ...` to `FOR ALL TABLES EXCEPT (...), ALL SEQUENCES`. Its member and schema are dropped first, then `SET ALL ...` runs.

Publication rows that would block a table change are released ahead of the tables and put back afterwards (see section 70). This covers a column type change or column drop on a column used by a row filter or column list, `SET UNLOGGED` of a member or, on PG19, of an EXCEPT entry, and on PG19 `ATTACH PARTITION` of an EXCEPT entry. A publication the migration keeps as it is gets its own list set back.

> **Note**: Subscriptions need no publisher and no replication slot when they are created `WITH (connect = false, slot_name = NONE)`; section 45 uses that. A fixture subscription must still exist on both sides: pgc's own `CREATE SUBSCRIPTION` connects to the publisher.

> **Note**: User-created foreign data wrappers are testable in pure SQL by reusing the `postgres_fdw` extension's handler, validator and (PG19+) connection functions; see section 45. The extension-owned `postgres_fdw` FDW itself is excluded from comparison.

### 45. Subscriptions and Foreign-Data Wrapper CONNECTION Functions (PG19+)
Requires PostgreSQL 19+ (`postgres_fdw` 1.3 supplies `postgres_fdw_connection`). The PG19-only statements fail on PG14–18 on both sides alike: `CREATE/ALTER SUBSCRIPTION ... SERVER`, `CONNECTION` / `NO CONNECTION` on a wrapper, and the `retain_dead_tuples`, `max_retention_duration` and `wal_receiver_timeout` options. Every subscription is created `WITH (connect = false, slot_name = NONE)`, so it is disabled, owns no slot and can be dropped without a publisher. `pg_subscription` is a shared catalog; the `a` and `b` rows stay apart only because pgc filters it by `subdbid`.

#### Foreign Data Wrappers (CONNECTION function)
- **Unchanged**: `pg19_subfdw_fdw_same` — `CONNECTION public.postgres_fdw_connection` on both sides
- **Modified**: `pg19_subfdw_fdw_added` — no connection function in FROM; TO adds one (`ALTER FOREIGN DATA WRAPPER ... CONNECTION`). It also carries TO's `pg19_subfdw_srv_b`, so the ALTER must run before the subscription moves to that server
- **Modified**: `pg19_subfdw_fdw_removed` — has a connection function in FROM; TO removes it (`ALTER FOREIGN DATA WRAPPER ... NO CONNECTION`)
- **Removed**: `pg19_subfdw_fdw_from_only` — carries the FROM-only server; dropped after the subscription changes
- **Added**: `pg19_subfdw_fdw_to_only` — connection function only (`CREATE ... NO HANDLER NO VALIDATOR CONNECTION ...`)

#### Foreign Servers and User Mappings
- **Unchanged**: `pg19_subfdw_srv_a` (on `postgres_fdw`) and its `PUBLIC` mapping
- **Unchanged**: `pg19_subfdw_srv_owner` (on `postgres_fdw`), whose only mapping is for the role `pgc_pg19_subfdw_owner` and carries a password, plus a `USAGE` grant to that role. The role is created idempotently. The grant exists on both sides because pgc does not compare foreign-server privileges
- **Removed**: `pg19_subfdw_srv_from_only` (on `pg19_subfdw_fdw_from_only`) and its `PUBLIC` mapping. A subscription blocks `DROP SERVER` / `DROP FOREIGN DATA WRAPPER` even with `CASCADE`, so both drops follow the subscription section
- **Added**: `pg19_subfdw_srv_b` (on `pg19_subfdw_fdw_added`) and its `PUBLIC` mapping — they must exist before `ALTER SUBSCRIPTION ... SERVER pg19_subfdw_srv_b`

#### Subscriptions
- **Modified**: `pg19_subfdw_sub_to_server` — `CONNECTION` → `SERVER pg19_subfdw_srv_a`. On PG14–18 only FROM can create it, so the diff there is `DROP SUBSCRIPTION`
- **Modified**: `pg19_subfdw_sub_switch` — `SERVER pg19_subfdw_srv_from_only` → `SERVER pg19_subfdw_srv_b`
- **Modified**: `pg19_subfdw_sub_to_conn` — `SERVER pg19_subfdw_srv_a` → `CONNECTION`. FROM creates it with `CONNECTION` and switches it by `ALTER`, so PG14–18 see identical subscriptions
- **Modified**: `pg19_subfdw_sub_owner_switch` — owner `postgres` → `pgc_pg19_subfdw_owner`, and `SERVER pg19_subfdw_srv_a` → `SERVER pg19_subfdw_srv_owner`, where only the new owner has a mapping.
  - `ALTER SUBSCRIPTION ... SERVER` checks the current owner's user mapping, `USAGE` and (for a non-superuser) password, so `OWNER TO` must run first.
  - On PG16–18 the diff is the `OWNER TO` alone. PG14/15 refuse a non-superuser subscription owner on both sides, so they show no diff
- **Removed**: `pg19_subfdw_sub_from_only` — on the FROM-only server; dropped before every other section
- **Modified**: `pg19_subfdw_rdt_off` — `retain_dead_tuples` true → false, `max_retention_duration` 60000 → 5000, and the conninfo moves from this server to an unreachable host. While `retain_dead_tuples` is on, a source change contacts the new publisher, so the `SET` must come first
- **Modified**: `pg19_subfdw_rdt_on` — `retain_dead_tuples` false → true, and the conninfo moves from an unreachable host to this server (`host=127.0.0.1 port=5432 user=postgres`, trust-authenticated inside the official image). Turning it on always contacts the publisher, so the `SET` must follow the `CONNECTION` change
- **Modified**: `pg19_subfdw_wrt_changed` — `wal_receiver_timeout` `'30s'` → `'10s'`
- **Modified**: `pg19_subfdw_wrt_reset` — `'45s'` → default (`SET (wal_receiver_timeout = '-1')`; there is no `RESET` form)
- **Modified**: `pg19_subfdw_wrt_set` — default → `'2min'` together with `max_retention_duration` 0 → 10000, in one `SET (...)`
- **Unchanged**: `pg19_subfdw_wrt_same` — `'1min'` on both sides

> **Note**: Changing `retain_dead_tuples` on an enabled subscription needs a running apply worker and a live slot, so the fixtures leave it to section 77's CI step and unit tests. This covers the `DISABLE` first, the wait for the worker, the changes made while disabled, the `REFRESH PUBLICATION` after the `ENABLE`, and the `--use-single-transaction` warning. In a fixture it would make CI depend on a background worker, and the slots would keep `a` and `b` from being dropped.

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
- **Reloptions on recreated partitions** (Issue #216): `reloptions_to_partitioned` goes regular → partitioned, which forces DROP + CREATE. Its partitions are then built with `CREATE TABLE ... PARTITION OF`, which takes `WITH (...)` like any other `CREATE TABLE`; emitting it without the clause leaves every partition with no reloptions and the second diff is non-empty. `reloptions_to_partitioned_2025` (autovacuum settings, `FOR VALUES` bound) and `reloptions_to_partitioned_default` (`fillfactor`, `DEFAULT` bound) cover both bound forms with different option sets. `reloptions_to_regular` covers the inverse direction (partitioned → regular), which already carried its reloptions through the plain `CREATE TABLE` `WITH (...)`

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
- **Unquoted list round-trip** (Issue #217): `set_config_roundtrip(p_id integer)` has no config in FROM; TO adds `SECURITY DEFINER`, an **unquoted** list-valued `SET search_path = test_schema, pg_temp`, a scalar `SET lock_timeout = '5s'`, and a numeric `SET statement_timeout = 30000`. `search_path` is a `GUC_LIST_QUOTE` parameter, so its `proconfig` value is a bare comma-separated list; emitting it as a single-quoted literal (`'test_schema, pg_temp'`) makes PostgreSQL re-store it as one schema named `test_schema, pg_temp`, and the second diff keeps re-emitting the routine. The list must be emitted verbatim (or element by element as literals when an element would not survive re-lexing, see below), the scalars as literals. The pre-existing `'public, pg_temp'` fixtures above could not catch this: a single-quoted value stores as one element and round-trips regardless
- PostgreSQL stores these in `pg_proc.proconfig` as an array (e.g. `{search_path=public\, pg_temp,lock_timeout=5s}`)

#### Routine SET for List Parameters: Empty, Long and Numeric Elements, TO NULL (PG19+)
- **Modified**: `test_schema.pg19_routines_guc_empty_string()`: `SET search_path = public` in FROM, `SET search_path = ''` in TO. The TO value is stored as `search_path=""` and must be emitted as `SET search_path TO ''`. pgc 1.0.x pasted it back as `SET search_path = ""`, which every version rejects ("zero-length delimited identifier").
- **Modified**: `test_schema.pg19_routines_guc_empty_element()`: TO has `SET search_path = '', public`. Expect `SET search_path TO '', 'public'`.
- **Modified**: `test_schema.pg19_routines_guc_long_element()`: TO has a 67-byte schema name plus `public`. It is emitted as literals, because the lexer would truncate an identifier to 63 bytes.
- **Modified**: `test_schema.pg19_routines_guc_numeric()`: TO has `SET search_path = public, 42`. A number is stored bare (`search_path=public, 42`) and must be emitted verbatim, `SET search_path = public, 42`. As the literal `'42'` it would be stored back as `"42"` and re-emitted on every compare.
- **Modified**: `test_schema.pg19_routines_guc_numeric_mixed()`: TO has `SET search_path = '', 7`. Expect the literal form with the number kept bare: `SET search_path TO '', 7`.
- **Modified**: `test_schema.pg19_routines_guc_output_plugins()`: `output_plugin_libraries` (a `GUC_LIST_QUOTE` parameter) goes from `pgoutput` to the unquoted list `pgoutput, test_decoding`. It must stay a two-element list (emitted verbatim), not become one literal.
- **Modified (PG19+)**: `test_schema.pg19_routines_guc_to_null()`: `SET search_path = ''` in FROM, `SET search_path TO NULL` in TO, which is stored as a bare `search_path=`. `TO NULL` is emitted only when the migrated (FROM) server is PG19+; otherwise the portable `= ''` is used.
- **Modified (PG19+)**: `test_schema.pg19_routines_guc_from_null()`: the reverse, `TO NULL` in FROM and `= public` in TO.
- **Added (PG19+)**: `test_schema.pg19_routines_guc_null_proc()`: TO-only procedure with `SET temp_tablespaces TO NULL`.
- On PG14–18 the `TO NULL` statements are syntax errors, so those routines exist on only one side and are dropped or created.
- A list whose every element survives re-lexing as the identifier or number it was stored as is still emitted verbatim, byte-identical to earlier releases.

#### Identity Dropped Before the Column's Other ALTERs (Issue #243)

- `test_schema.i243_identity_drop` has three `bigint GENERATED BY DEFAULT AS IDENTITY` columns in FROM. In TO each of them drops the identity and relaxes one of the three things an identity column guarantees: `goes_nullable` becomes nullable, `gains_default` takes a `DEFAULT 42`, and `leaves_integer` becomes `text`.
- PostgreSQL holds an identity column NOT NULL, forbids it a default, and restricts it to an integer type — and enforces each at the moment of the `ALTER`, not at the end of the transaction. So the identity has to come off first, or the statement that relaxes the guarantee is rejected: `column "..." is an identity column` for the first two, `identity column type must be smallint, integer, or bigint` for the third.
- `TableColumn::get_alter_script` appends one column's statements to a single list in fixed source order — type, default, nullability, identity — so the drop came last and all three failed. Only the nullability case was reported; the other two are the same defect through the other statements. The mirror image (adding identity) needs the opposite order and already had it: `SET NOT NULL` and `DROP DEFAULT` must both precede `ADD GENERATED ... AS IDENTITY`, which is why the fix hoists only the drop rather than swapping the blocks.
- The round-2 diff **does** catch this one, unlike issue #242: the failed statements leave the columns unmigrated, so the second compare re-emits them (349 bytes before the fix, 0 after), on top of 4 apply errors.

#### Views Dragged Along by an Unread Column Change (Issue #242)

- `test_schema.i242_orders` differs in two ways between FROM and TO: it gains `unrelated_2`, and `status` goes `text` → `varchar(50)`.
- Three views sit on it. `i242_mv_untouched` (materialized, with index `ix_i242_mv_untouched_id`) and `i242_v_untouched` (regular) read only `id`. `i242_mv_touched` reads `status`.
- PostgreSQL refuses exactly two things while a view depends on a column: dropping it and retyping it. Adding a column, or dropping/retyping a column the view never reads, succeeds with the view in place (verified live on PG 16). So the diff must drop `i242_mv_touched` and recreate it, and must leave the other two — and the index — completely alone.
- Before the fix the drop set was decided at *table* granularity: any view over a table whose hash differed at all was dropped first. Both `i242_mv_untouched` and `i242_v_untouched` were dropped and rebuilt, and the materialized one's index was rebuilt with `CREATE INDEX CONCURRENTLY`, purely because an unrelated column was added to a table they happen to read.
- **The round-2 diff does not catch this** — an unnecessary drop and recreate still converges on the right schema, so the second compare is empty either way (verified: 0 bytes with the bug present). The guard is the dedicated `Assert unread-column changes did not drag views along` step in `.github/workflows/rust.yml`, which greps the round-1 script directly, in the same spirit as the existing invalid-index assertion.
- The dependency data comes from `View::column_relation`, captured from `pg_depend` for the view's `_RETURN` rule. It covers every position a column can appear in, not just the select list — `WHERE`, `JOIN ... ON` and `GROUP BY` references are all recorded (verified live).

#### Extension Moved Between Schemas (Issue #241)

- `fuzzystrmatch` is installed `WITH SCHEMA shared_schema` in FROM and `WITH SCHEMA public` in TO. Nothing else about it changes.
- An extension's name is its identity — `pg_extension` carries a unique index on `extname` — and its schema only records where it was installed. Matching FROM against TO on `(schema, name)` therefore never paired the two rows, and the move was emitted as two unrelated events: `CREATE EXTENSION IF NOT EXISTS ... WITH SCHEMA public` followed by `DROP EXTENSION IF EXISTS fuzzystrmatch`.
- That pair is destructive and silent. The `CREATE` is a no-op because the name is already taken, so the `DROP` removes the only copy: the migration applies with **no errors** and the extension is simply gone afterwards, taking every function it provided with it. The round-2 diff catches it (79 bytes before the fix, 0 after) because the extension is then missing from `a` entirely.
- Expected output is a single `alter extension fuzzystrmatch set schema public;`. `Extension::get_alter_script` has always emitted that branch; before the fix it was unreachable for this case.
- The existing extension fixtures cover the other three transitions and guard against over-matching: `uuid-ossp` and `pg_trgm` are unchanged in both, `pgcrypto` is FROM-only (dropped), `hstore` is TO-only (created).
- **Not covered, and not fixable by name-matching alone:** a *non-relocatable* extension (`extrelocatable = false`, e.g. `xml2`, `plperl`, the PL languages) cannot be moved by `ALTER EXTENSION ... SET SCHEMA` at all. Such a move now produces a script that stops with `extension "..." does not support SET SCHEMA`, leaving the extension where it was — a loud failure in place of the old silent deletion, but still not an applicable migration. The dump does not capture `extrelocatable`, so pgc cannot currently tell the two apart.

#### Routine Named in a Comment or a Literal (Issue #240)

- Three TO-only routines form one real call chain — `i240_util_a` <- `i240_helper_d` <- `i240_z_caller` — with a false back-edge planted at each link: `i240_util_a`'s body **comment** names `i240_helper_d`, and `i240_helper_d`'s body carries a string **literal** naming `i240_z_caller`.
- pgc infers routine creation order by scanning each body's text for qualified `schema.name` references, because PostgreSQL records no `pg_depend` edge for a call inside `prosrc`. Neither a comment nor a literal is resolved when the routine is created, so neither is a creation-order dependency — but a text scan cannot tell them from a call, and each false edge closes a 2-cycle with the real one beside it.
- Kahn's sort cannot order a cycle: it appends the cyclic nodes, and everything blocked behind them, in plain `(priority, schema, name)` order. That puts `i240_helper_d` first, where it fails with `function test_schema.i240_util_a(text) does not exist`, and the round-2 diff is then non-empty (566 bytes before the fix, 0 after).
- The two false edges are deliberately of different kinds so one fixture covers both doors into the bug; the chain is three deep so the fixture also exercises a routine that is merely *blocked behind* a cycle rather than in it.

#### CASCADE-Drop Dependent Recreation (Issue #179)

- `test_schema.cascade_compute(integer)` returns `INTEGER` in FROM and `BIGINT` in TO. The argument list (`integer`) is unchanged, so the routine's signature in PostgreSQL terms is identical between FROM and TO; the return-type change still requires `DROP FUNCTION ... CASCADE` (PostgreSQL has no `ALTER FUNCTION` for return types). The cascade silently removes every object whose `pg_depend` row points at the function.
- `test_schema.cascade_items` carries one of each affected dependent kind, all referencing `cascade_compute`:
  - **CHECK constraint**: `chk_cascade_compute CHECK (cascade_compute(value) > 0)`
  - **Functional index**: `idx_cascade_compute ON (cascade_compute(value))`
  - **Generated column**: `gen_col GENERATED ALWAYS AS (cascade_compute(value)) STORED`
  - **Column DEFAULT**: `def_col … DEFAULT cascade_compute(0)`
  - **RLS policy**: `pol_cascade_items USING (cascade_compute(value) > 0)`
- The dependent definitions are intentionally identical (modulo the integer→bigint type bumps that follow from the new return type), so the regular table/index/policy diffs emit nothing for them. Phase 7 of `compare_routines_and_views` must re-emit each one — without it the migration leaves them missing and a second `pgc compare` run would notice the drift.
- Generated columns are re-added via `alter table … add column` (Postgres drops generated columns wholesale on CASCADE); column DEFAULTs are restored via `alter column … set default` (the column survives, only the DEFAULT clause is dropped).

#### Secondary Dependents of CASCADE-Dropped Columns (Issue #188)

- `test_schema.cascade_compute_v2(integer)` returns `INTEGER` in FROM and `BIGINT` in TO. Same DROP+CREATE pattern as `cascade_compute`, but the dependents under test sit one hop further down the dependency chain: PostgreSQL CASCADE-drops the **generated column** `gen_total` (because its expression references the function), and that column drop in turn CASCADEs to objects attached to the column.
- `test_schema.cascade_col_dep_items` carries four secondary dependents that the issue #179 text-based scanner cannot detect, because their definitions reference only the column, not the function:
  - **Plain index on the generated column**: `idx_cascade_col_dep_gen_total ON (gen_total)` — no `cascade_compute_v2(...)` text anywhere in `pg_get_indexdef`.
  - **CHECK constraint on the generated column**: `chk_cascade_col_dep_gen_total CHECK (gen_total >= 0)` — names only the column.
  - **UNIQUE constraint on the generated column**: `uq_cascade_col_dep_gen_total UNIQUE (gen_total)` — the comparer's dedup must skip the constraint's same-named backing index because `ADD CONSTRAINT ... UNIQUE` recreates the index implicitly.
  - **FOREIGN KEY on a different table**: `test_schema.cascade_col_dep_children` has `fk_cascade_col_dep_children_ref_total` referencing `cascade_col_dep_items(gen_total)`. The FK's pg_depend row anchors on the *parent* column but `conrelid` points at the *child* — the asymmetric `dep_schema`/`dep_table` case. The FK must be emitted AFTER its UNIQUE target (two-pass ordering in `recreate_column_dependents`).
- The fix walks the column-dependent graph harvested from `pg_catalog.pg_depend` at dump time (new `Dump::column_dependents` field). Phase 7 emits the column recreate (`ADD COLUMN IF NOT EXISTS gen_total …`) and then re-emits the index, CHECK, UNIQUE, and FK from the graph entries anchored on `gen_total`. The same machinery covers the `TableColumn::get_alter_script` virtual-generated-column drop+add path (issue #181) — column dependents are re-emitted right after the `DROP COLUMN` + `ADD COLUMN`, wrapped in a `/* Recreate dependents dropped by virtual-column rewrite */` block.

#### Extension Version Not Installable on the Migrated Server (PG19 btree_gist 1.9)
- PostgreSQL 19 ships `btree_gist--1.9.sql` as the only base script, so `pg_available_extension_versions` lists only 1.9. `create extension ... version '1.8'` fails with `has no installation script nor update path`, and `update to '1.8'` from 1.9 fails with `no update path`. The target version comes from an older major, or from a `pg_upgrade`d 19 cluster still at 1.8. Every GiST index and exclusion constraint that needs btree_gist then fails as well.
- Each dump records the versions its server can install (`Dump::available_extensions`). The comparer reads the FROM dump, because the migration runs on that server:
  - a TO-only extension whose version is not installable is created without `VERSION`, at the server default, preceded by a comment;
  - an update is replaced by a comment only when the installed version is installable and the target is not. A `pg_upgrade`d version the server can no longer install still gets its update attempted.
  - Dumps without the list, and every installable version, produce the same SQL as before.
- The single-server CI cannot exercise this, because `a` and `b` get the same versions. It is covered by unit tests in `comparer/tests/core/schemas_extensions.rs`, and was verified live: from an empty PG19 database to a PG18 one, btree_gist was created at 1.9 and the exclusion constraint applied.
- **Not covered:** a downgrade between two installable versions (e.g. btree_gin 1.4 → 1.3 on PG19) still emits an `alter extension ... update to` that fails, as before.

#### standard_conforming_strings = off on a PostgreSQL 14–18 Server
- Every pgc connection runs `set standard_conforming_strings = on`, as pg_dump does, so literals are always dumped in standard form. A server, database or role set to `off` used to make pgc dump every backslash in defaults, CHECK constraints, index predicates and view bodies doubled.
- The migration is applied to the FROM server, where psql inherits that server's setting. A standard `'C:\temp'` would then be read as an escape string and store `C:`, a tab and `emp`. So each dump records its session default (`Dump::standard_conforming_strings_off`, from `pg_settings.reset_val`). When the FROM default is `off`, or unknown because the FROM dump was written by pgc 1.0.x, and the TO dump was written by pgc 1.1+, the migration begins with `set standard_conforming_strings = on;` before `begin;`, as a pg_dump script does. The SET is withdrawn from an otherwise empty migration.
- A fixture would need `ALTER DATABASE a SET standard_conforming_strings = off` on the shared CI databases. It is covered instead by:
  - a dedicated CI step on PG14–18 (skipped on 19, which cannot turn the setting off). It creates `pgc_scs_a` and `pgc_scs_b` set to `off`, compares, applies with psql, checks the stored default is `C:\temp` (7 characters), and requires an empty second compare;
  - the ignored live tests `dump_reads_standard_literals_from_a_database_with_scs_off` and `migration_keeps_backslash_literals_on_a_database_with_scs_off` in `app/tests/live_database.rs`.

#### Grantor Chains in Full Grants Mode (`GRANTED BY`)
A privilege granted by a role other than the object's owner (a delegate holding `WITH GRANT OPTION`) is recorded under that role. A plain `REVOKE` run by the owner or a superuser removes only the entries recorded under the owner, and does so silently. Revoking a grant option that other entries were granted under fails with `dependent privileges exist`. PostgreSQL 19 also records the role named by `GRANT ... GRANTED BY`, so such chains take a single statement to build. PostgreSQL 14–18 reject `GRANTED BY` naming anyone but the current user (`grantor must be current user`), so the fixture builds its every-version chains with `SET ROLE`. All objects live in `public` (which has no default privileges) and belong to `pgc_pg19_grants_owner`. The roles are `pgc_pg19_grants_delegate`, `pgc_pg19_grants_sub`, `pgc_pg19_grants_reader` and the mixed-case `"pgc_pg19_Grants_Mixed"`.
- **Delegated grants dropped** (every major):
  - `public.pg19_grants_chain`: in FROM, the delegate grants reader `SELECT, UPDATE`, PUBLIC `SELECT` and sub `SELECT (note)`. All are gone in TO, while the delegate keeps its grant options.
  - `public.pg19_grants_seq`: reader `USAGE` granted by `"pgc_pg19_Grants_Mixed"` (tests grantor quoting).
  - `public.pg19_grants_fn()`: reader `EXECUTE`, granted by the delegate.
  - Schema `public`: reader `USAGE`, granted by the delegate.
- **Grant option removed with dependents** (every major): `public.pg19_grants_opt`.
  - FROM chain: owner → delegate → sub → reader, each `WITH GRANT OPTION` except the last.
  - TO keeps delegate `SELECT` without the option, and reader `SELECT` granted by the owner.
  - Expected: reader's and sub's entries are revoked leaf first in their grantors' names, then `REVOKE GRANT OPTION FOR` from the delegate, then `GRANT SELECT` to reader.
- **Table revokes reach the columns** (every major). PostgreSQL applies a table-level `REVOKE` to every column, removing the column entries the same grantor gave the same grantee.
  - `public.pg19_grants_colkeep`: in FROM, reader has `SELECT` on the table and on `note` from the owner, and sub has both from the delegate. TO keeps only the two column grants. Expected: the table-level `REVOKE`s (plain for reader, in the delegate's name for sub), then `GRANT SELECT (note)` to both in the same run.
  - `public.pg19_grants_coldep`: in FROM, the delegate has `SELECT WITH GRANT OPTION` on the table and on `note`, and reader has `SELECT (note)` from the delegate. TO removes only the table-level option and gives reader its column grant from the owner. Expected, in order:
    1. reader's column entry revoked in the delegate's name (otherwise `dependent privileges exist`);
    2. `REVOKE GRANT OPTION FOR SELECT ON TABLE`;
    3. `GRANT SELECT (note)` to reader, and to the delegate `WITH GRANT OPTION`.
- **`GRANTED BY`** (PG19+): `public.pg19_grants_granted_by`. In FROM, reader `SELECT` and sub `SELECT (note)` are granted `GRANTED BY pgc_pg19_grants_delegate`; both are gone in TO. On 14–18 the statements fail on both sides and nothing differs.
- **Grantor-only change** (PG19+): `public.pg19_grants_same`. Reader `SELECT` is granted by the delegate in FROM and by the owner in TO. Expected: no diff, because the GRANT side is grantor-insensitive. On 14–18 FROM lacks the entry, and a plain `GRANT` is expected.
- **Expected form.**
  - When the migrated (FROM) server is PostgreSQL 19+: `REVOKE ... FROM <grantee> GRANTED BY <grantor>;`.
  - Otherwise: `SET ROLE <grantor>;` / `REVOKE ... FROM <grantee>;` / `RESET ROLE;`.
  - These statements all go at the top of the grants section (columns, then objects, then schemas), ahead of every owner-level statement.
  - With `OUTPUT_FOR_PRODUCTION=true`, each one is wrapped in a `DO $pgc$ ... $pgc$;` block that runs it only while `aclexplode` of the object's ACL still lists the entry, with `SET ROLE` inside the block before PG19. This keeps a replayed migration from failing with `permission denied`.

#### Owned Sequences and Serial Detection

- A TO-only sequence owned by a column (`OWNED BY`, `pg_depend` deptype `a`) is written as the column's `serial` / `bigserial` / `smallserial` pseudo-type only when `CREATE TABLE` with that pseudo-type recreates it exactly and nothing needs it earlier. All of the following must hold:
  - the owning table is new and is a plain table. A table that already exists is changed with `ADD COLUMN` / `SET DEFAULT`, which cannot use a pseudo-type, and `PARTITION OF` / `OF type` give their columns no type;
  - no column default of another new table that is created before the owning table (in schema and name order) names the sequence. The owner's own partitions and inheritance children, tables created later, and tables already in FROM are fine;
  - the column is `smallint` / `integer` / `bigint` and NOT NULL, and the sequence has the same type;
  - the sequence has the generated name (`<table>_<column>_seq`, truncated to 63 bytes the same way PostgreSQL's `makeObjectName` does it), and the default is plain `nextval('<sequence>'::regclass)`;
  - every sequence option is at its default and the sequence has no comment;
  - its persistence matches the table's (PG15+).
- Otherwise the sequence is created explicitly with its own options before the tables, the column keeps its real type and `nextval` default, and `ALTER SEQUENCE ... OWNED BY` is emitted in a "Sequences: Ownership section" after all tables.
- Only an identity column's own identity sequence (deptype `i`) is created and dropped together with the column. Any other sequence owned by an identity column is created and dropped like any other sequence.
- Genuine serials: `test_schema.pg19_serial_genuine` (`serial` PK, `bigserial`, `smallserial`) and `pg19_serial_gpart` (the `bigserial` of a partitioned table, whose partition `pg19_serial_gpart_p1` repeats the default) must still be emitted with the pseudo-types, byte for byte as before.
- `pg19_serial_num` (`numeric`) and `pg19_serial_oid8` (`oid8`, PG19+; on PG14-18 it stays bigint and is an ordinary bigserial). The old fallback arm turned any non-integer column into `serial`: it created an integer column and an integer sequence, and never converged (MAXVALUE out of range for type integer). PG19's `oid8` cannot be an identity column, so an owned sequence is the only way to auto-number it.
- `pg19_serial_mismatch`: an `integer` column fed by a `bigint` sequence.
- `pg19_serial_custom` (sequence `pg19_serial_custom_counter`) and `pg19_serial_renamed` (a renamed table whose sequence keeps the old name `pg19_serial_renamed_old_id_seq`). The pseudo-type would create a sequence with another name, and the default would keep pointing at a relation that does not exist.
- `pg19_serial_add` and `pg19_serial_alter` exist in FROM. In TO they gain a serial column, or their `id` becomes serial. The sequence used to be skipped as "created by the column", but `ADD COLUMN` / `SET DEFAULT` never creates it, so the migration failed on every run.
- `pg19_serial_nullable` (a serial with `DROP NOT NULL`), `pg19_serial_tuned` (`INCREMENT BY 10 CACHE 5` plus a sequence comment), and `pg19_serial_seq_unlogged` / `pg19_serial_seq_logged` (sequence persistence changed on its own; PG15+). The pseudo-type cannot reproduce these, and the second diff used to re-alter them.
- `pg19_serial_part_p1`: a partition of `pg19_serial_part` with its own serial-shaped owned sequence. `PARTITION OF` cannot declare a serial column, so the sequence used to be skipped and never created. pgc does not compare a partition's own column defaults (a separate issue), so the CI step does not compare that one default.
- `pg19_serial_shared` (a serial) and `pg19_serial_a_shared_ref`: the second table sorts first and defaults to `nextval` of the first table's sequence, so the sequence must exist before any table is created. The first diff used to fail on a sequence that did not exist yet.
- `pg19_serial_ident` (TO: an identity column that also owns `pg19_serial_ident_extra`) and `pg19_serial_ident_drop` (the same shape, with its extra sequence only in FROM). The extra sequence used to be skipped as "identity/serial": it was never created, and it was never dropped.
- `pg19_serial_plain`: a sequence owned by a column that has no `nextval` default. It used to be created without `OWNED BY`.
- The round-2 diff cannot see a lost ownership or a sequence that is never created or dropped, because the sequence hash ignores ownership and the skip repeats every run. The CI step `Assert owned sequences and serial columns` therefore compares every `pg19_serial_*` sequence (type, options, persistence, comment, owning column and deptype) and every column (type, NOT NULL, default) between `a` and `b` directly.

### 47. Text Search: Snowball Dutch Stemmer Rewrite (PG19+)
PostgreSQL 19 rewrote the Dutch Snowball stemmer and kept the old one as language `dutch_porter`. Moving a dictionary to `dutch_porter` to keep the pre-19 stems is therefore a natural upgrade step.
- **Modified (PG19+)**: `test_schema.pg19_misc_dict_nl` — FROM `language = 'dutch', stopwords = 'dutch'`, TO `language = 'dutch_porter', stopwords = 'dutch'`. Expected: exactly `ALTER TEXT SEARCH DICTIONARY test_schema.pg19_misc_dict_nl (language = 'dutch_porter', stopwords = 'dutch');`, with no `DROP`.
- **Unchanged dependents**: configuration `pg19_misc_tsconfig_nl` maps the dictionary. The STORED generated column `misc_pg19_ts_docs.body_tsv` and the GIN index `misc_pg19_ts_docs_fts` both use that configuration. `DROP TEXT SEARCH DICTIONARY ... CASCADE` would drop all three.
- **Modified**: `misc_pg19_ts_notes` gains the STORED generated column `note_tsv` over the same configuration, on every version. On PG19 the rows `a` already holds (`mogelijkheden`, `gebruikt`, `bedrijven`, which stem differently under `dutch` and `dutch_porter`) are computed when the column is added. So the dictionary ALTER must come first, and pgc emits in-place option changes in their own section before the tables. Round 2 cannot see stale stored values. The CI catalog assertion recomputes them instead, and on PG19 expects row 1 to hold `'mogelijk':1`.
- **Added (PG19+)**: `pg19_misc_dict_pl`, the new Polish stemmer, without STOPWORDS because no `polish.stop` ships. Also `pg19_misc_tsconfig_eo`, which is `COPY = pg_catalog.esperanto`.
- On PG14–18, `schema_b.sql` creates the dictionary with `language = 'dutch'` in a `DO` block, because `dutch_porter` does not exist there. The dictionary itself therefore produces no diff; only `note_tsv` is added. The Polish and Esperanto statements fail on `b` only.
- **Emission rule**: first the bare names of the options TO no longer has, then every TO option in TO order. PostgreSQL removes same-named options and appends in statement order, so `dictinitoption` ends up byte-equal to TO's.
  - The statement goes in the `Text Search Dictionary Options (before tables)` section, together with any owner or comment change of the same dictionary.
  - A `TEMPLATE` change is still drop + recreate, with the other dictionaries after the tables, because PostgreSQL cannot alter it. Owner-only and comment-only changes also stay there.
- The ALTER does not re-stem data already stored. With comments on, it is preceded by a note to rebuild the dependent indexes and stored tsvector columns, and pgc prints the same warning on stderr.

### 48. oid8 Data Type (PG19+)
- **Modified (PG19+)**: `test_schema.misc_pg19_oid8_refs` — `id` (primary key) and `big_ref` go from bigint to oid8, `legacy_ref` from oid to oid8, and `back_ref` from oid8 to bigint. Each is `ALTER COLUMN ... TYPE` with no `USING`.
- **Added (PG19+)**:
  - columns `ref_list oid8[] DEFAULT '{1,2}'`, `max_ref oid8 DEFAULT '18446744073709551615'::oid8`, `nonzero_ref` (a domain) and `ref_span` (a range);
  - CHECK `chk_misc_pg19_oid8_refs_id` and hash index `idx_misc_pg19_oid8_refs_max_ref`;
  - domain `pg19_misc_oid8_nonzero` and range type `pg19_misc_oid8_span`;
  - function `pg19_misc_oid8_or_default(x oid8 DEFAULT '7'::oid8)`;
  - view `pg19_misc_oid8_refs_summary`, using the oid8 `min`/`max` aggregates.
- PG14–18: every oid8 statement fails (`type "oid8" does not exist`), and the bigint/oid base table is identical on both sides.

### 49. Window Function Null Treatment: IGNORE NULLS / RESPECT NULLS (PG19+)
All over `test_schema.misc_pg19_readings`. `pg_get_viewdef` prints `IGNORE NULLS` and never prints `RESPECT NULLS`, which is the default.
- **Modified**: `pg19_misc_v_null_treatment` — plain `lag` → `lag ... IGNORE NULLS` (`CREATE OR REPLACE VIEW`)
- **Modified**: `pg19_misc_v_null_treatment_off` — `last_value ... IGNORE NULLS` → plain `last_value` (`CREATE OR REPLACE VIEW`)
- **Unchanged**: `pg19_misc_v_respect_nulls` — TO adds `RESPECT NULLS`, which deparses away, so there is no diff
- **Modified**: `pg19_misc_mv_null_treatment` — materialized view, plain `lead` → `lead ... IGNORE NULLS` (drop + create)
- **Added**: `pg19_misc_v_ignore_nulls_new` — `nth_value ... IGNORE NULLS OVER w` with a named `WINDOW`
- PG14–18: `IGNORE/RESPECT NULLS` is a syntax error, so such a view exists on one side only and is created or dropped as a plain view.

### 50. JSON_ARRAY(query) Deparse (PG19)
- **Modified (PG19)**: `pg19_misc_ja_view` — FROM is spelled in the pre-19 form: the `JSON_ARRAYAGG` scalar subquery that PostgreSQL 16–18 store, and that a pg_dump from them carries into 19. TO is spelled `JSON_ARRAY(SELECT ...)`. Expected: `CREATE OR REPLACE VIEW`. This is a real behaviour change on 19: an empty result is `NULL` in the old form and `'[]'` in the new one.
- **Modified (PG19)**: `pg19_misc_ja_mv` — the same for a materialized view with `RETURNING jsonb` (drop + create)
- **Unchanged**: `pg19_misc_ja_stable` — `JSON_ARRAY(SELECT ...)` on both sides
- PG16–18: both spellings deparse identically, so there is no diff. PG14–15: there is no `JSON_ARRAY`, and the views fail on both sides. pgc deliberately does not normalise one form into the other.

### 51. Heap Storage Parameters New in PG19
The options are set with separate `ALTER ... SET` statements. On PG14–18 only the ALTER fails (`unrecognized parameter`), and the objects still exist on both sides.
- **Modified**: `misc_pg19_reloptions_test` — `fillfactor=80` on every version. On PG19, FROM adds `autovacuum_parallel_workers=2, log_autovacuum_min_duration=500` and TO adds `autovacuum_parallel_workers=4, log_autoanalyze_min_duration=1000`. Expected: `reset (...)` + `set (...)`.
- **Added**: `misc_pg19_reloptions_new` — created `with (autovacuum_parallel_workers=0, log_autoanalyze_min_duration=250)`
- **Modified**: leaf partition `misc_pg19_reloptions_part_1` gains `autovacuum_parallel_workers=1`
- **Modified**: materialized view `pg19_misc_reloptions_mv` gains `log_autoanalyze_min_duration=10` (drop + create + `alter materialized view ... set`)

### 52. New Contrib Extension pg_stash_advice (PG19+)
- **Added (PG19+)**: `pg_stash_advice` in `public`, TO only. Expected: `create extension if not exists pg_stash_advice with schema public version '1.0';`. The version is installable on the migrated server, so it stays pinned. The extension needs no `shared_preload_libraries`, and its member objects are extension-owned and never dumped. On PG14–18 it fails harmlessly (`is not available`).

### 53. Aggregate Changes In Place, PARALLEL, SORTOP and Unbounded State
Applies on every version: PG14–18 accept and store negative state sizes too; PostgreSQL 19's planner reads a negative `SSPACE` as "unbounded".
- **Modified (option)**: `test_schema.pg19_routines_agg_initcond(bigint)`: `INITCOND = '0'` in FROM, `'1'` in TO. Expect `CREATE OR REPLACE AGGREGATE` and no DROP. The view `test_schema.pg19_routines_agg_initcond_v` uses the aggregate on both sides and must survive, because OR REPLACE keeps the OID.
- **Modified (PARALLEL)**: `test_schema.pg19_routines_agg_parallel(bigint)`: default `PARALLEL UNSAFE` in FROM, `PARALLEL = SAFE` in TO.
- **Modified (SSPACE)**: `test_schema.pg19_routines_agg_sspace(integer)` has the default state size in FROM and `SSPACE = -1` in TO. `test_schema.pg19_routines_agg_sspace_reset(integer)` goes the other way (`-1` → default).
- **Modified (MSSPACE)**: `test_schema.pg19_routines_agg_msspace(integer)`: the moving aggregate gains `MSSPACE = -1`.
- **Added (unbounded state)**: `test_schema.pg19_routines_agg_sspace_new(integer)`: TO-only, created with `SSPACE = -1`.
- **Modified (SORTOP)**: `test_schema.pg19_routines_agg_sortop(integer)`: no sort operator in FROM, `SORTOP = >` in TO. It must be emitted as `SORTOP = OPERATOR(pg_catalog.>)`. The stored `regoper` text `pg_catalog.>` is a syntax error there, because the grammar only takes a qualified operator inside `OPERATOR(...)`.
- **Added (SORTOP)**: `test_schema.pg19_routines_agg_sortop_new(integer)`: TO-only, with `SORTOP = <`.
- **Modified (aggregate kind)**: `test_schema.pg19_routines_agg_kind(VARIADIC "any" ORDER BY VARIADIC "any")`: ordered-set in FROM, hypothetical-set (`HYPOTHETICAL`) in TO, with identical identity arguments. `CREATE OR REPLACE` refuses a kind change, so expect `DROP AGGREGATE … CASCADE` and then the create.
- **Modified (routine kind)**: `test_schema.pg19_routines_fn_to_agg(bigint)`: an SQL function in FROM, an aggregate with the same signature in TO. Expect `DROP ROUTINE … CASCADE` (kind-agnostic, so a replay still works) and then `CREATE OR REPLACE AGGREGATE`.
- These cases cover four pgc 1.0.x bugs:
  - It emitted a plain `CREATE AGGREGATE` for every change, which failed with "already exists".
  - It never emitted `PARALLEL`, so those aggregates looped.
  - It wrote `SORTOP` in a form that does not parse.
  - It dropped negative `SSPACE`/`MSSPACE` from both the script and the hash, a silent loss the round-2 check cannot see.

### 54. Privileges of a Dropped-and-Recreated Routine
Applies on every version. CI runs `GRANTS_MODE=full`.
- **Modified (routine kind, granted)**: `test_schema.pg19_routines_grant_kind(bigint)`: a function in FROM, an aggregate in TO, with `GRANT EXECUTE … TO pgc_pg19_routines_role` on both sides. The `DROP ROUTINE … CASCADE` removes the grant, so expect `GRANT EXECUTE ON FUNCTION test_schema.pg19_routines_grant_kind(bigint) TO pgc_pg19_routines_role;` in the same migration.
- **Modified (return type, PUBLIC revoked)**: `test_schema.pg19_routines_grant_revoked(bigint)`: returns `bigint` in FROM and `integer` in TO, with `REVOKE EXECUTE … FROM PUBLIC` on both sides. The recreated function starts with PUBLIC's `EXECUTE` again, so expect `REVOKE EXECUTE ON FUNCTION test_schema.pg19_routines_grant_revoked(bigint) FROM PUBLIC;`.
- pgc 1.0.x compared these against the FROM routine's privileges, which the DROP had already removed. Explicit grants took a second run to come back. A PUBLIC revoke was lost silently: round 2 sees nothing, because both dumps agree on the text of the ACL.

### 55. Aggregate SUPPORT Function (PG19+)
Requires PostgreSQL 19 and a superuser. PG14–18 accept `SUPPORT = …` with `WARNING: aggregate attribute "support" not recognized` and create the aggregate without it, so on those versions only the FROM-only and TO-only aggregates differ. The support function is `int8inc_support`, the one `count()` uses.
- **Modified (added)**: `test_schema.pg19_routines_support_add(bigint)`: gains `SUPPORT = int8inc_support`.
- **Modified (removed)**: `test_schema.pg19_routines_support_drop(bigint)`: loses it.
- **Unchanged**: `test_schema.pg19_routines_support_same(*)`: the same `SUPPORT` on both sides.
- **Removed**: `test_schema.pg19_routines_support_old(bigint)`: FROM-only aggregate with `SUPPORT`.
- **Added**: `test_schema.pg19_routines_support_new(bigint)`: TO-only, with `SUPPORT` and `PARALLEL = SAFE`.
- `SUPPORT` can only change through `CREATE OR REPLACE AGGREGATE`: there is no ALTER form, and `ALTER FUNCTION` refuses aggregates. Omitting it resets `prosupport`.
- When FROM was dumped from PostgreSQL 14–18 and TO from 19, the `SUPPORT` clause is left out, with a comment and a stderr warning. The older server would accept the clause with a WARNING and ignore it, so the compare would never converge. An aggregate that differs only in its support function gets just the comment. A FROM dump without a version (pgc 1.0.x) gets the clause as before.

### 56. CHECK Constraint Enforceability Changed In Place (PG19+)
- **Modified**: `test_schema.wp19_tables_ck`:
  - `wp19_tables_ck_relax`: enforced in FROM, `NOT ENFORCED` in TO.
  - `wp19_tables_ck_tighten`: `NOT ENFORCED` in FROM, enforced (re-validated) in TO.
  - `wp19_tables_ck_notvalid`: `NOT VALID` in FROM, `NOT ENFORCED` in TO.
- **Modified**: `test_schema.wp19_tables_ck_parent` / `wp19_tables_ck_child`. The child has a merged local copy of `wp19_tables_ck_amount` (conislocal, coninhcount = 1) and sorts before its parent on purpose. TO sets the parent's copy `NOT ENFORCED`, and that recurses to the child. pgc emits the parent's statement first, because PostgreSQL 19 rejects `NOT ENFORCED` on a child while the parent's copy is still enforced.
- **Modified**: `test_schema.wp19_tables_ck_part` (partitioned, partition `wp19_tables_ck_part_1`). `wp19_tables_ck_part_amount` is `NOT ENFORCED` in FROM and enforced in TO. The parent manages the partition's inherited copy.
- **Modified**: `test_schema.wp19_tables_keep_parent` / `wp19_tables_keep_child`. TO sets the parent's `wp19_tables_keep_amount` `NOT ENFORCED` and keeps the child's merged copy enforced. `ALTER CONSTRAINT ... NOT ENFORCED` cannot say `ONLY` and would flip the child too, so pgc keeps the drop + re-add on the parent. The re-added constraint merges into the child's copy and leaves it enforced.
- **Modified**: `test_schema.wp19_tables_rc`. `g` goes from a plain column to `GENERATED ALWAYS AS (id * 2) STORED` on every version, and its CHECK `wp19_tables_rc_g_check` goes `NOT ENFORCED` on PG19. The DROP COLUMN takes the CHECK with it, so the flip stays a drop + re-add that runs before the column is dropped.
- On PG19 the other flips are `ALTER TABLE ... ALTER CONSTRAINT c [NOT] ENFORCED`, with no `ONLY`, so they recurse.
- An enforced-but-`NOT VALID` target still needs drop + `ADD ... NOT VALID`, because `ALTER ... ENFORCED` always validates. A `NO INHERIT` change is always drop + re-add: `ALTER CONSTRAINT ... [NO] INHERIT` applies only to NOT NULL constraints.
- Every NOT ENFORCED state here is set with the PG19-only `ALTER CONSTRAINT`. PG18 rejects it for CHECK ("cannot alter enforceability"), and PG14-17 lack the syntax, so on those versions FROM and TO stay identical for the flips. On PG14-18, and for dumps without a server version, pgc keeps the drop + re-add.

### 57. Generated Column Expression Changed with SET EXPRESSION (PG17+ / PG19+)
- **Modified**: `test_schema.wp19_tables_gen_stored`. The STORED column `total` goes from `(qty * 2)` to `(qty * 3)`. It has an index, and the view `wp19_tables_gen_stored_v` reads it.
  - PG17+: `ALTER TABLE ONLY ... ALTER COLUMN total SET EXPRESSION AS (qty * 3)`. The index and the view stay.
  - PG14-16: the view is pre-dropped, `DROP COLUMN` + `ADD COLUMN` run, the index is restored and the view recreated.
  - pgc used to emit `ALTER COLUMN ... ADD GENERATED ALWAYS AS (...) STORED`, which is invalid on every version.
- **Modified**: `test_schema.wp19_tables_gen_virtual` (PG18+). The VIRTUAL `doubled` has CHECKs and is read by `wp19_tables_gen_virtual_v`.
  - PG19: `SET EXPRESSION`, and the view is kept.
  - PG18, which rejects SET EXPRESSION in tables with check constraints: the view is pre-dropped, the column is dropped and re-added, and the CHECK is restored.
- **Modified**: `test_schema.wp19_tables_gen_virtual_nochk` (PG18+). VIRTUAL with no CHECK: `SET EXPRESSION` on 18 and 19.
- **Modified**: `test_schema.wp19_tables_gen_published` (PG18+). The table is an explicit member of publication `wp19_tables_pub`. PG18 and PG19 reject SET EXPRESSION for its virtual columns, so the column is dropped and re-added.
- **Modified**: `test_schema.wp19_tables_gen_retype`. The type and the expression change together (`integer (id * 2)` becomes `date ('2020-01-01'::date + id)`). That is `DROP COLUMN` + `ADD COLUMN` on every version. It is never `ALTER COLUMN ... TYPE` followed by SET EXPRESSION, because TYPE casts the old expression and int -> date cannot be cast.
- **Modified**: `test_schema.wp19_tables_gen_newcol`.
  - The new expression `(qty * 2) + (bonus * 3)` reads the new column `bonus`. The old one read `legacy`, which is dropped.
  - The generated change (SET EXPRESSION on PG17+, DROP/ADD COLUMN before) runs after `ADD COLUMN bonus` and before `DROP COLUMN legacy`.
  - PostgreSQL deparses the expression as `((qty * 2) + (bonus * 3))`, which checks that only the enclosing pair of parentheses is stripped.
- **Modified**: `test_schema.wp19_tables_gen_newcol_p`. plain -> generated, reading a new column: the DROP/ADD COLUMN runs after the ADD.
- **Added**: `test_schema.wp19_tables_gen_parens`. TO-only, created with `GENERATED ALWAYS AS ((a * 2) + (b * 3)) STORED`.
- **Modified**: `test_schema.wp19_tables_gen_vpart` (PG18+). A partitioned VIRTUAL `v` whose expression changes. TO adds partition `wp19_tables_gen_vpart_2` with a CHECK, and that partition exists before the parent is altered.
  - PG18: drop + add.
  - PG19: recursing SET EXPRESSION (no `ONLY`).
- **Modified**: `test_schema.wp19_tables_gen_part` / `wp19_tables_gen_part_1`. Partitioned STORED, and the partition's local CHECK on the column changes too.
  - PG17+: recursing SET EXPRESSION.
  - PG14-16: the parent drops and re-adds the column, and the partition's CHECK drop says `IF EXISTS`, because the parent's recursing DROP COLUMN already took it.
- **Modified**: `test_schema.wp19_tables_gen_inh`, a classical-inheritance family, with children:
  - `wp19_tables_gen_inh_follow`: inherited copy, an index, a view and a local CHECK that changes.
  - `wp19_tables_gen_inh_merged`: a merged copy that follows the parent (PG16+).
  - `wp19_tables_gen_inh_own`: its own expression, unchanged (PG16+).
  - On PG17+: `ALTER TABLE ONLY ... SET EXPRESSION` on the parent and on each follower, and nothing for `_own`.
  - On PG14-16: the parent drops the column, drops the followers' copies with `DROP COLUMN IF EXISTS`, and re-adds it once. The followers emit no drop/add of their own, and their index, view and CHECK are restored.
- **Modified**: `test_schema.wp19_tables_gen_vinh` / `wp19_tables_gen_vinh_c` (PG18+). VIRTUAL inheritance with no CHECK: `ONLY` SET EXPRESSION on 18 and 19 for the parent and the child.
- The decision is taken from the FROM dump's server version, because the migration runs there. A dump without a version gets the drop + add form.

#### Classical Inheritance Ordering
- **Removed**: `test_schema.wp19_tables_inh_drop_a_parent` with its child `wp19_tables_inh_drop_b_child`, a FROM-only family whose parent sorts first. The child must be dropped first.
- **Added**: `test_schema.wp19_tables_inh_z_parent` with its child `wp19_tables_inh_a_child`, a TO-only family whose child sorts first. The parent must be created first.
- pgc orders `INHERITS` parents before children for CREATE/ALTER and children before parents for DROP. Tables already in a valid order keep their position.

### 58. Table and Column Names (all versions)
- **Added**: `test_schema.pgx_like_escape` — TO-only. The tables query used to filter `tablename not like 'pg_%'`; `_` is a LIKE wildcard, so every table whose name starts with `pg` plus any one character (`pgbench_accounts`, `pgx_like_escape`) was left out of every dump and never migrated. The filter now matches a literal underscore (`'pg\_%'`). Round 2 cannot see a table missing from both dumps, so the CI catalog assertion checks that the table exists in `a` after the migration.
- **Added**: `test_schema."spaced Columns"` — TO-only, with the quoted columns `"Col X"` and `"a b c"`. `CREATE TABLE` removed each column's name by cutting its definition at the first space, so `"Col X" integer` came out as `"Col X" X" integer`: an unterminated quoted identifier that made psql read the rest of the migration into it. The CI catalog assertion checks the table's columns in `a`.

### 59. Extended Statistics on Expressions (PG14+) and Virtual Generated Columns (PG19+)
The tables are named `t_pg19_stat_*` and the statistics objects `pg19_stat_*`.
- **Modified**: `test_schema.pg19_stat_expr_kinds`: `(ndistinct)` in FROM, `(mcv)` in TO, on `qty, (lower(code))`. Drop + recreate; the kind list must not name `expressions`.
- **Modified**: `test_schema.pg19_stat_expr_onlist`: `ON qty, (extract(year from created_on))` in FROM, `shipped_on` in TO. The old comma split of the ON list saw both sides as unchanged.
- **Modified**: `test_schema.pg19_stat_expr_target`: statistics target 100 in FROM, 250 in TO (`ALTER STATISTICS ... SET STATISTICS`). pgc lost explicit targets on PG17+, where `stxstattarget` is a nullable smallint.
- **Modified**: `test_schema.pg19_stat_expr_reset`: statistics target 300 in FROM, the default in TO (`SET STATISTICS -1`).
- **Unchanged**: `test_schema.pg19_stat_expr_same`: `(mcv) ON qty, (code || ',')`, a comma inside a literal. Must stay silent.
- **Removed**: `test_schema.pg19_stat_expr_from_only`: univariate `ON (qty + 1)`.
- **Added**: `test_schema.pg19_stat_expr_single`: univariate `ON (lower(code))`. The CREATE must carry no kind list.
- **Added**: `test_schema.pg19_stat_expr_tricky`: `ON qty, (coalesce(code, ',')), (extract(year from created_on))`. The ON list comes from `pg_get_statisticsobjdef_columns`, not a comma split.
- **Restored**: `test_schema.pg19_stat_conv_st` (target 200). `t_pg19_stat_conv.dbl` becomes a STORED generated column, which pgc emits as DROP COLUMN + ADD COLUMN. PostgreSQL drops the statistics with the column, and pgc recreates them from the `pg_depend` column graph.
- **Unchanged**: `test_schema.pg19_stat_conv_setexpr` (target 250). `t_pg19_stat_conv.tripled` changes its STORED expression (`qty * 3` to `qty * 4`).
  - Before PG17 this is a DROP COLUMN + ADD COLUMN; the statistics go with it and are restored.
  - From PG17 on it is `SET EXPRESSION`, which keeps them but rebuilds them without their statistics target. pgc sets the target again, and must not recreate them.
- **Restored**: `test_schema.pg19_stat_part_st` (target 400). `t_pg19_stat_part` changes its partition key and is dropped and recreated; its unchanged statistics are created again.
- **Restored**: `test_schema.pg19_stat_fn_items_st` (target 600). `pg19_stat_fn_compute` changes its return type. `DROP FUNCTION ... CASCADE` drops the generated column `gen_total` and the statistics on it, and Phase 7 restores both.
- **Added**: `test_schema.pg19_stat_fn_items_new`: `(mcv) ON value, gen_total`, TO only. Created before Phase 7's CASCADE drops it, then restored from the TO dump's column graph.
- **Restored**: `test_schema.pg19_stat_fn_part1_st` (target 700), on the partition `t_pg19_stat_fn_part1` of the partitioned `t_pg19_stat_fn_part`, whose `gen_total` also calls `pg19_stat_fn_compute`. Phase 7 re-adds the column on the parent only. Extended statistics are not inherited, so the partition's own statistics must be restored too.
- `t_pg19_stat_pc` (partitioned) changes `v` from integer to bigint. The parent's ALTER COLUMN ... TYPE recurses to the partition `t_pg19_stat_pc1`, which is not dropped. The ALTER rebuilds the statistics on `v` and resets their targets:
  - **Unchanged**: `test_schema.pg19_stat_pc_keep` (target 450) on the partition. It must not be created again ("already exists"), and its target is set again.
  - **Unchanged**: `test_schema.pg19_stat_pc_parent` (target 350) on the parent. Its target is set again.
  - **Modified**: `test_schema.pg19_stat_pc_kind`: `(ndistinct)` in FROM, `(mcv)` in TO, on the partition. An ordinary drop + create.
- **Unchanged**: `test_schema.pg19_stat_mv_rebuilt` (target 800) on `mv_pg19_stat_rebuilt`, whose definition changes. pgc drops the view before the tables and creates it after them, and the statistics are created again after the view.
- **Added**: `test_schema.pg19_stat_mv_new` on the TO-only materialized view `mv_pg19_stat_new`. It must be created after the view.
- PG19 only. PG14-17 reject the VIRTUAL table; PG18 creates it but rejects statistics on virtual columns.
  - **Unchanged**: `test_schema.pg19_stat_vcol_rewrite` (target 500) and `test_schema.pg19_stat_vcol_expr` (univariate `ON (doubled + 1)`). `doubled` changes its expression (`qty * 2` to `qty * 3`), which pgc applies with `SET EXPRESSION`. The statistics stay (no restore), and the rebuilt target is set again.
  - **Restored**: `test_schema.pg19_stat_vcol_readd` (target 550) and `public.pg19_stat_vcol_xschema` (`(mcv) ON price, readded`, in a different schema from its table). `readded` changes type and expression (`integer (qty + 1)` to `bigint (qty + 2)`), which SET EXPRESSION cannot do, so it is dropped and re-added.
  - **Modified**: `test_schema.pg19_stat_vcol_kinds`: `(ndistinct)` in FROM, default kinds in TO, on `qty, total`. Emitted as `(ndistinct, dependencies, mcv) on qty, (total)`.
  - **Removed**: `test_schema.pg19_stat_vcol_from_only`: univariate `ON total`.
  - **Added**: `test_schema.pg19_stat_vcol_to_only`: univariate `ON total`, emitted as `on (total)` with no kind list.

### 60. SQL-Standard Routine Bodies: BEGIN ATOMIC and RETURN (PG14+), ON CONFLICT DO SELECT (PG19+)
Applies on every version (PG14+).

PostgreSQL parses a SQL-standard body when the routine is created. It stores the body as a query tree in `pg_proc.prosqlbody` and leaves `prosrc` empty. pgc 1.0.x read only `prosrc`, so:
- it dumped every such body as `''`
- it replayed the body as `as $$$$`, which fails for a function and makes a procedure an empty no-op
- it never detected a change to the body

pgc now reads the body with `pg_get_function_sqlbody(oid)` under an empty `search_path`, as `pg_dump` does, so every name outside `pg_catalog` stays schema-qualified. It emits the body verbatim after every option, with no `AS`.

Unlike a string body, such a body is a hard dependent (`pg_depend`) of every relation, column, routine and operator it uses, and PostgreSQL resolves it at CREATE time even with `check_function_bodies` off.
- pgc records those dependencies (`sql_body_dependencies`).
- It drops such a routine before a change it would block and re-creates it afterwards.
- It creates such a routine after the operators and casts it uses.

- **Unchanged**: `test_schema.pg19_sqlbody_unchanged(integer)` has the same `BEGIN ATOMIC SELECT p + 1; END` on both sides. No diff is expected.
- **Modified (RETURN)**: `test_schema.pg19_sqlbody_return(integer)` has `RETURN p + 1` in FROM and `RETURN p + 2` in TO. Expect `CREATE OR REPLACE FUNCTION ... RETURN (p + 2);` and no DROP.
- **Modified (procedure)**: `test_schema.pg19_sqlbody_proc_changed(integer)` has a one-statement `BEGIN ATOMIC` body (`ON CONFLICT DO NOTHING`) in FROM, and two statements in TO: `ON CONFLICT DO UPDATE`, then a `DELETE`.
- **Modified (ON CONFLICT DO SELECT, PG19+)**: `test_schema.pg19_sqlbody_upsert(integer)` uses `DO NOTHING` in FROM and `DO SELECT FOR UPDATE WHERE ...` in TO. DO SELECT is a syntax error before 19, so on PG14–18 the TO function is never created and the diff drops it.
- **Modified (string ↔ SQL-standard)**: `test_schema.pg19_sqlbody_to_atomic(integer)` switches from `AS $$ ... $$` to `BEGIN ATOMIC`, and `test_schema.pg19_sqlbody_to_string(integer)` switches back. `CREATE OR REPLACE` converts either form, and the hash tells them apart.
- **Removed**: `test_schema.pg19_sqlbody_proc_from_only(integer)`.
- **Added**:
  - `test_schema.pg19_sqlbody_options(integer)`: every option comes before the body.
  - `test_schema.pg19_sqlbody_proc_to_only(integer)`: `SECURITY DEFINER`, with a comment.
  - `test_schema.pg19_sqlbody_literals()`: a CR LF and a `$$` inside literals, kept byte for byte.
- **Dependency order**: `test_schema.pg19_sqlbody_a_caller` calls `test_schema.pg19_sqlbody_z_callee`, and `public.pg19_sqlbody_a_pub_caller` calls `public.pg19_sqlbody_z_pub_callee`.
  - The callee sorts last by name but must be created first.
  - The public pair relies on the body being deparsed with an empty `search_path`. Under the default path the callee printed as a bare `pg19_sqlbody_z_pub_callee(p)`, which the dependency scan cannot see.
  - `public.pg19_sqlbody_pub_view_count()` reads the TO-only view `public.pg19_sqlbody_pub_view`. Routines sort before views, so the recorded dependency is what puts the view first.
- **Hard dependents, pre-dropped and re-created**:
  - `test_schema.pg19_sqlbody_legacy_count()` / `_legacy_next()` read the FROM-only table `pg19_sqlbody_legacy` and sequence `pg19_sqlbody_legacy_seq`. The readers are dropped first, since `drop table` / `drop sequence` fail under them.
  - `pg19_sqlbody_cols.old_col` is dropped under the changed reader `pg19_sqlbody_cols_old()`.
  - `pg19_sqlbody_cols.v` is retyped integer → bigint under the unchanged reader `pg19_sqlbody_cols_max()`. That reader is called by the view `pg19_sqlbody_cols_view` and by the CHECK constraint `pg19_sqlbody_limits_chk`, and granted to `pgc_pg19_sqlbody_reader` (PUBLIC revoked). All of them come back.
  - `pg19_sqlbody_shape_view` loses a middle column (DROP + CREATE) under the unchanged reader `pg19_sqlbody_shape_count()`.
  - `pg19_sqlbody_rt_callee` changes its return type (DROP ... CASCADE) under the unchanged SQL-standard caller `pg19_sqlbody_rt_caller`.
  - `pg19_sqlbody_parted` changes its partition key (table rebuilt) under the unchanged reader `pg19_sqlbody_parted_count()`.
  - The operator `test_schema.===` changes its function (DROP OPERATOR ... CASCADE + CREATE) under the unchanged user `pg19_sqlbody_uses_op`, which the column default `pg19_sqlbody_op_default.ok` calls.
- **After operators and casts**: the TO-only `pg19_sqlbody_uses_new_op` (operator `test_schema.<=>`, called by the view `pg19_sqlbody_new_op_view`) and `pg19_sqlbody_uses_cast` (cast integer → `test_schema.pg19_sqlbody_pair`) are created after the operator and cast phases.
- Round 2 cannot see a loss that happens on both sides alike (pgc 1.0.x emptied these bodies in both dumps). The CI catalog assertion therefore:
  - compares `pg_get_functiondef` of every non-C, non-aggregate routine of the compared schemas between `a` and `b`
  - compares the views, constraints, defaults, routine ACLs, operators and casts around the fixture
  - fails on any `as $$$$` in the migration
  - spot-checks the fixture values

Clear Command Test addition (`clear_test.sql`, section 10b): the SQL-standard functions `clear_app.top_customer_count()` (reads a view), `clear_app.order_total_atomic(integer)` (reads a table) and `clear_app.order_total_doubled(integer)` (calls the former). The clear script drops them together with the views, before the relations they read, and the caller before its callee.

### 61. Domain CHECK Constraints and Enum Labels Changed Alone (all versions)
Each object below differs between the two schemas only in its CHECK constraints or labels, not in owner, comment or default, so the type hash alone has to notice. pgc 1.0.x hashed a type before its domain constraints and enum labels were loaded and migrated none of these. Its round 2 was still empty, because the change was lost on both sides.

The fixture also holds data that makes statement order matter:
- a table and a column dropped in TO that hold values only the old CHECKs allow;
- a CHECK that calls a routine only TO has;
- a column default that uses a newly added enum label.

The CI step compares, after round 1, every domain's `pg_constraint` rows (name, `convalidated`, definition) and every enum's label list between `a` and `b`, and checks the statement order in the round-1 SQL.

- **Modified (CHECK body)**: `test_schema.pg19_domains_body`: `pg19_domains_body_range` goes from `VALUE > 0` to `VALUE > 5`.
  - The `drop constraint` runs with the types. ALTER DOMAIN cannot change an expression in place.
  - The `add constraint "pg19_domains_body_range" CHECK ((VALUE > 5));` runs in the "Domain constraints: Add section". That section comes after the tables, routines, operators and casts.
  - It has to wait: `test_schema.pg19_domains_legacy` (dropped in TO) holds 3 and `pg19_domains_usage.old_qty` (dropped in TO) holds 2, and the new CHECK rejects both.
- **Modified (CHECK added)**: `test_schema.pg19_domains_gain` gains `"pg19_domains Gain ""q"""`, a name that has to be quoted.
- **Modified (CHECK removed)**: `test_schema.pg19_domains_lose` loses `pg19_domains_lose_len`.
- **Modified (validated)**: `test_schema.pg19_domains_validate`: `pg19_domains_validate_nonneg` is `NOT VALID` in FROM and validated in TO.
  - Expected: `alter domain ... validate constraint "pg19_domains_validate_nonneg";`, with no drop, emitted even with `--use-drop false`.
  - It runs after `pg19_domains_legacy`, which holds -3, is dropped.
- **Modified (made NOT VALID)**: `test_schema.pg19_domains_invalidate` is validated in FROM and `NOT VALID` in TO. No ALTER goes that way, so the expected output is drop + `add constraint ... CHECK ((VALUE >= 0)) NOT VALID;`.
- **Modified (CHECK calls a new routine)**: `test_schema.pg19_domains_sku`: `pg19_domains_sku_fmt` calls FROM-only `pg19_domains_old_sku` in FROM and TO-only `pg19_domains_is_sku` in TO.
  - The old CHECK is dropped before `pg19_domains_old_sku` is dropped.
  - The new one is added after `pg19_domains_is_sku` is created.
- **Unchanged**: `test_schema.pg19_domains_same` has two constraints, declared in opposite orders in the two schemas. It must not appear in the diff.
- **Modified (enum label)**: `test_schema.pg19_domains_level` gets `'medium'` inserted between `'low'` and `'high'`, and `pg19_domains_usage.level` defaults to `'medium'` in TO.
  - Expected: `alter type test_schema.pg19_domains_level add value if not exists 'medium' before 'high';`.
  - With `--use-single-transaction` it is emitted before `begin;`, because a value added inside the transaction cannot be used until it commits.
- **Modified (PG19+)**: `test_schema.pg19_domains_json_doc` is a domain over the jsonb domain `pg19_domains_jsonb`. Its CHECK `pg19_domains_json_doc_shape` goes from `VALUE IS JSON ARRAY` to `VALUE IS JSON OBJECT`, as a drop + re-add.
  - PG16–18 reject IS JSON on a domain over a jsonb domain, and PG14–15 have no IS JSON.
  - On those versions the `ALTER DOMAIN` fails on both sides and the round trip still closes.
- Dumps written by pgc 1.0.x keep their old type hashes. `Dump::read_from_file` rehashes every type, so comparing them with each other, or a FROM dump written by 1.0.x with a new TO dump, gives the same type SQL as comparing fresh dumps. A new FROM dump against a 1.0.x TO dump is refused (section 67).

### 62. Rule Changes, Ordering and Dependencies (all versions; ON CONFLICT DO SELECT on PG19+)

All objects live in `test_schema` and are named `pg19_rules_*` (or the quoted `"Pg19 Rules Mixed"` table). Each case below was reproduced live on PG14 and PG19 before the fix.

- **Changed**: `pg19_rules_chg` on `pg19_rules_src` goes from a one-action `DO ALSO` to a two-action `DO INSTEAD ( ...; ... )`. `pg_get_ruledef` always prints `CREATE RULE ... ;`. pgc used to emit that text, with another `;` appended, for a changed rule. Applied to a rule that exists, it fails with `rule "..." for relation "..." already exists`. Expected output is `CREATE OR REPLACE RULE pg19_rules_chg AS ...;`, ending in one semicolon; the semicolons inside the parentheses belong to the actions.
- **Changed, quoted names**: `"Pg19 Rules Where"` on `"Pg19 Rules Mixed"` changes its event (`ON UPDATE` → `ON DELETE`), its qualification and its comment. `CREATE OR REPLACE RULE` can change all of these and keeps the rule's OID (verified). The `COMMENT ON RULE` must not quote the already-`quote_ident`-ed names a second time.
- **Removed, quoted name**: `"Pg19 Rules Gone"`. Before the fix the drop read `drop rule if exists """Pg19 Rules Gone""" on test_schema."""Pg19 Rules Mixed"""`. That is another name: the drop was a silent `NOTICE ... skipping`, and the same drop came back on every run.
- **Removed together with the table it writes to**: `pg19_rules_to_old_audit` inserts into `pg19_rules_old_audit`, which only FROM has. `DROP TABLE` runs without `CASCADE` and is refused while the rule exists, so removed rules are dropped in a `Rules DROP` block ahead of the `Views DROP` section.
- **Unchanged rule on a retyped column**: `pg19_rules_qty` reads `pg19_rules_src.qty`, which goes `integer` → `bigint`. PostgreSQL refuses `ALTER COLUMN ... TYPE` while a rule depends on the column. The rule is dropped in the `Rules DROP` block and recreated with `CREATE OR REPLACE RULE` at the end. The dependency comes from `Rule::depends_on`, a `pg_depend` snapshot of the rule's relations, columns and routines. A dump written by pgc 1.0.x has no snapshot and keeps the old behaviour.
- **Foreign tables a rule reads**: `pg19_rules_ft_read` reads the FROM-only foreign table `pg19_rules_ft` (TO keeps the rule, using `NEW`). `pg19_rules_ft2_read` reads `pg19_rules_ft2.v`, which goes `integer` → `bigint`. `DROP FOREIGN TABLE` runs without `CASCADE`, and `ALTER FOREIGN TABLE ... ALTER COLUMN ... TYPE` is refused while a rule reads the column. Both rules are dropped first and recreated. A server change, which drops and recreates the foreign table, is handled the same way. Both foreign tables use the existing `test_foreign_server`.
- **Unchanged rule on a rebuilt view**: `pg19_rules_view` loses a column, which `CREATE OR REPLACE VIEW` cannot do, so the view is dropped and recreated. `pg19_rules_view_ins` goes with it and must be recreated after the view.
- **Unchanged rule on a recreated table**: `pg19_rules_part` changes its partition key (`RANGE` → `LIST`). `pg19_rules_part_del` must be recreated after the table.
- **Table turned into a view of the same name**: `pg19_rules_tv` is a table in FROM and a view in TO, with the same `DO ALSO NOTIFY` rule `pg19_rules_tv_notify`. `DROP TABLE` takes the rule. A `NOTIFY`-only rule has no `pg_depend` dependencies at all, so this is decided from the rule's own table, which also works for 1.0.x dumps.
- **Unchanged rules taken by a routine's CASCADE**: `pg19_rules_tag(integer)` changes its return type (`integer` → `bigint`), which is `DROP FUNCTION ... CASCADE` + `CREATE`. The CASCADE takes:
  - `pg19_rules_tagged`, which calls the function;
  - the view `pg19_rules_tag_view`, which only filters on the function, together with its rule `pg19_rules_tag_view_del`;
  - `pg19_rules_reads_tag_view` on `pg19_rules_reader`, which only reads that view.

  The routine-dependents pass (issue #179/#189) recreates the view, and all three rules must come back.
- **Comment removed on a rule the CASCADE did not take**: `pg19_rules_tag_txt_view` calls the unchanged `pg19_rules_tag(text)` overload. The routine-dependents pass matches it by name and re-creates it with `CREATE OR REPLACE VIEW`, so its rule `pg19_rules_tag_txt_del` is re-emitted with `CREATE OR REPLACE RULE`, although nothing dropped it. TO removes the rule's comment, so the recreate must also clear it with `comment on rule ... is null`.
- **Added**: `pg19_rules_new_view_ins`, on the TO-only view `pg19_rules_new_view`, calls the TO-only function `pg19_rules_fn`. Rules are therefore created after Routines & Views, Operators and Casts, just before Event Triggers.
- **PG19+ `ON CONFLICT DO SELECT`**: `pg19_rules_upsert` changes from `ON CONFLICT (id) DO NOTHING` to `ON CONFLICT (id) DO SELECT FOR UPDATE RETURNING ...` and is replaced in place. Before 19 the TO statement is a syntax error under `ON_ERROR_STOP=0`, so the rule is absent from TO and the diff drops it.
- **The round-2 diff does not catch all of this.** A rule lost the same way on both sides — say, by a table recreate that no later dump can see — converges. The guard is the `Assert rules match after round 1` CI step, which checks that:
  - `pg_get_ruledef` and the comment of every non-`_RETURN` rule are the same in `a` and `b`;
  - every fixture rule above is present, and the removed ones are gone;
  - `pg19_rules_old_audit` and `pg19_rules_ft` are dropped, `qty` and `pg19_rules_ft2.v` are retyped, and `pg19_rules_tv` is a view;
  - no statement touches a `_RETURN` rule;
  - no rule statement ends in `;;`.
- In production mode new rules are emitted as `CREATE OR REPLACE RULE`, so the migration can be replayed.

### 63. REPLICA IDENTITY USING INDEX
All tables are in `test_schema` and apply on PG14+. The only exception is the `NOT VALID` not-null of `pg19_replident_notvalid`, which is PG18+ syntax and fails harmlessly on PG14–17. Before 1.1.0, pgc dumped only `relreplident`, so none of these cases could be represented. The dump now records the index from `pg_index.indisreplident`, and its columns from `pg_index.indkey`.
- **Modified**: `pg19_replident_enter`: DEFAULT in FROM; USING INDEX `pg19_replident_enter_code_uk` (a plain unique index) in TO. pgc 1.0.x emitted `replica identity default` here on every run.
- **Modified**: `pg19_replident_switch`: USING INDEX `pg19_replident_switch_a_uk` in FROM; USING INDEX `pg19_replident_switch_b_uk` in TO, with both indexes kept. It takes one `USING INDEX` statement, with no DEFAULT in between. pgc 1.0.x did not detect this, because the hash held only `i`.
- **Modified**: `pg19_replident_leave`: USING INDEX in FROM; DEFAULT in TO, while the key column `code` loses NOT NULL. `replica identity default` must come before `alter column code drop not null`, which PostgreSQL refuses on a replica-identity key column.
- **Modified**: `pg19_replident_full`: USING INDEX in FROM; FULL in TO, and the index is dropped.
- **Modified**: `pg19_replident_rebuild`: the index keeps its name, but its definition changes from `(a)` to `(a, b)`. The table leaves the index before the drop and returns to it after the re-create, because a rebuilt index does not inherit `indisreplident`.
- **Modified**: `pg19_replident_ukey`: DEFAULT → USING INDEX of a UNIQUE constraint's index.
- **Modified**: `pg19_replident_ukey_rebuild`: USING INDEX of a UNIQUE constraint that is dropped and re-added as `UNIQUE (a, b)`.
- **Modified**: `pg19_replident_pkey`: DEFAULT → USING INDEX of the primary key's index.
- **Modified**: `pg19_replident_orphan`:
  - In FROM, the replica-identity index was dropped and created again. That leaves `relreplident = 'i'` with no flagged index, which the server treats as NOTHING and pgc now dumps as NOTHING.
  - TO has USING INDEX.
  - pgc 1.0.x hashed both sides as `i` and left the table orphaned.
- **Modified**: `pg19_replident_notvalid` (PG18+): `NOT NULL code NOT VALID` in FROM; a validated NOT NULL plus USING INDEX in TO. PostgreSQL 19 (and 18.7+) refuse USING INDEX while a key column's NOT NULL is `NOT VALID`, so USING INDEX follows the constraint change. On PG14–17 the FROM `ALTER` fails, so `code` stays nullable and the migration sets it NOT NULL first.
- **Modified**: `pg19_replident_part`: partitioned parent with one partition, DEFAULT → USING INDEX of a new unique index. Under `--output-for-production`, USING INDEX runs post-commit, after the per-partition `CREATE INDEX CONCURRENTLY` + `ATTACH PARTITION`.
- **Modified**: `pg19_replident_genrewrite`: the key column becomes `GENERATED ALWAYS AS (id * 2) STORED`, which pgc does as `DROP COLUMN` + `ADD COLUMN`. The drop takes the index with it, so USING INDEX follows the index re-created by the column-dependent recreation (the Issue #188 machinery).
- **Modified**: `pg19_replident_unrel`: another column, `g`, becomes `GENERATED ALWAYS AS (code * 2) STORED` (a drop + re-add). The index is on `code` only, so the table stays on it and no replica identity statement is emitted. Under `--output-for-production`, leaving in-transaction and returning after `COMMIT` would leave a table without a primary key with no replica identity in between.
- **Modified**: `pg19_replident_pp` / `pg19_replident_pkpart`:
  - The partitioned parents gain a unique index on `pg19_replident_ppk`, or a PRIMARY KEY.
  - The existing partitions `pg19_replident_pp_p1` / `pg19_replident_pkpart_k1` switch to USING INDEX of the partition index PostgreSQL creates for them from it.
  - So do the new partitions `pg19_replident_pp_p2` / `pg19_replident_pkpart_k2`.
  - A new partition is created before its parent is altered, so pgc runs these statements once the whole table section is done (post-commit under `--output-for-production`). Each is wrapped in a `DO` block that raises a WARNING instead of failing when the partition has no index of that name.
  - The index column is named so that PostgreSQL's name for each partition index (`<partition>_<column>_idx`) matches the one the production build uses (`<partition>_<parent index>`). That keeps the fixture convergent in both modes.
- **Added**:
  - `pg19_replident_new`: USING INDEX on a plain unique index. Under `--output-for-production` it runs post-commit, after the concurrent build.
  - `pg19_replident_new_ukey`: USING INDEX on a UNIQUE constraint's index, created in-transaction.
- **CI assertion**: after round 1, the CI catalog assertion compares `relreplident` and the `indisreplident` index name of every non-default table between `a` and `b`. It also checks the order of the round-1 statements.

### 64. Names Carrying Comment Delimiters (all versions)
pgc names every object it touches in a block-comment header (`/* Table: s.t*/`, `/* View: ...*/`, `/* Routine: ...*/`). Before 1.1.0 it wrote the name there verbatim. PostgreSQL allows any character in an identifier, so a `*/` in a name closed the header early and the rest of the name ran as SQL, and a `/*` opened a nested comment that swallowed the rest of the migration. The comment stripper behind `USE_COMMENTS=false` nests the same way, so the script CI applies was affected too. All objects are named `pg19_comments_*` and apply on PG14+.
- **Unchanged**: `public.pg19_comments_victim` has one row on both sides and must keep it.
- **Modified**: `test_schema."pg19_comments_t*/drop table public.pg19_comments_victim;/*"` drops column `gone` and adds `added`. Written verbatim, its `/* Table: ...*/` header ran `drop table public.pg19_comments_victim;`.
- **Modified**: the view `test_schema."pg19_comments_v/*"` loses column `b`, so it is dropped and recreated.
- **Modified**: the function `test_schema."pg19_comments_f*/"(integer)` changes its return type from `integer` to `bigint`, so it is dropped and created again.
- **Removed**: the table `test_schema."pg19_comments_d*/"` and the sequence `test_schema."pg19_comments_s/*"`.
- **Added**: the table `test_schema."pg19_comments_n/*"`, whose identity sequence `"pg19_comments_n/*_id_seq"` appears in a `Skipping sequence ...` header; and the enum `test_schema."pg19_comments_e*/"`.
- Expected: every header prints these names with the delimiters split (`"pg19_comments_t* /drop table public.pg19_comments_victim;/ *"`), and every statement above runs. Before the fix, round 2 on PG19 had 215 lines, because most of the script after the first nested header was lost.
- Names with a line break, which ends a `--` comment, are not in the fixture. With `USE_DROP=true`, every `--` note that names an object reports a change pgc cannot make (an enum label removed, an extension owner changed), so round 2 could never be empty.
  - `comparer/tests/core/fix_comments.rs` covers them instead, for every object kind whose name reaches a comment: LF, bare CR, CRLF, `*/` and `/*`.
  - A live check on PG14 and PG19 confirmed that none of these ran their payload:
    - a view named `E'v\ndrop table ...; --'`;
    - its bare-CR twin;
    - a PG14 extension-owner role with a newline.

Names in statements, not only in comments (same block):
- **Modified**: the materialized view `test_schema."pg19_comments_Mv*/"`, which has an index `"pg19_comments_Mv_a"`, gains column `b` with a column comment, so it is dropped and recreated with its index and comment.
- **Added**: the materialized view `test_schema."pg19_comments_MvNew"`. Before 1.1.0 pgc read materialized-view schema, name and owner without `quote_ident`:
  - the first view came out as `test_schema.pg19_comments_Mv*/`, which does not parse;
  - the second was created as `pg19_comments_mvnew` and never converged.
- **Modified**: the partitioned table `test_schema.pg19_comments_rp` gains the unique index `pg19_comments_rpk_idx`.
- **Added**: the partition `test_schema."pg19_comments_rp$pgc$1"`, whose `REPLICA IDENTITY USING INDEX` names the index it inherits from the parent. pgc sets it after the table section in a guarded `DO` block. The block used the fixed tag `$pgc$`, which the partition name ends early; it now uses a tag the body does not contain (`$pgc1$`). The parent index is named `<column>_idx`, so the partition index gets the same name with and without `--output-for-production`.
- **Modified**: the foreign table `test_schema.pg19_comments_ft` changes `table_name` from `'a'` to `'b'` and gains `updatable 'false'`. Expected: `options (set table_name 'b', updatable 'false')`. Before, the whole option list was sent as ADD, which fails with `option "table_name" provided more than once`.

**CI assertion**: after round 1, the CI step checks that:
- the victim still has its row;
- the relations, columns, view and materialized-view definitions, matview indexes and column comments, routines, enum labels, replica identities and foreign-table options of every `pg19_comments_*` object match between `a` and `b`, and that `b` really holds the fixture's shape;
- the round-1 script names the victim only inside the quoted fixture name.

### 65. What a Routine's DROP ... CASCADE Takes Along (PG14+ for the SQL-standard half, all versions otherwise)
All objects live in `test_schema` and are named `rv_cascade_*`. They are in the `fix:sqlbody` block of both schema files.

pgc drops a routine with `CASCADE` in two places:
- ahead of the table DDL, when the routine has a SQL-standard body that reads a column being retyped (section 60);
- in the routine phase, when its return type, arguments or kind change, or when it goes away.

PostgreSQL removes everything that depends on the routine. Before this fix, pgc put back only table CHECK/EXCLUDE constraints, indexes, column defaults, generated columns, policies, views and rules. v1.1.0 applied the migration cleanly but lost the rest, where v1.0.27 had stopped on `cannot alter type of a column used by a function or procedure`.

- **SQL-standard readers of a retyped column.** `rv_cascade_codes.code` goes `text` → `varchar(10)`. It is read by `rv_cascade_is_valid(text)`, `rv_cascade_norm(text)` and `rv_cascade_first_code()`, which are dropped with CASCADE before the retype and re-created after it. The CASCADE takes:
  - the domain CHECK `rv_cascade_code_d_check`, re-added with `drop constraint if exists` + `add constraint`, ahead of the "Domain constraints: Add section";
  - the trigger `rv_cascade_orders_chk` (its `WHEN` clause calls `rv_cascade_is_valid`), re-created right after the routines;
  - the expression statistics `rv_cascade_orders_st` on `rv_cascade_norm(code)`, restored after the routines;
  - `rv_cascade_flag(boolean)`, whose argument default calls `rv_cascade_is_valid`. It is dropped ahead of the table DDL too and re-created after its callee, with its grant to `pgc_pg19_sqlbody_reader`;
  - the domain `rv_cascade_code_def`, whose `DEFAULT` calls `rv_cascade_first_code()`. PostgreSQL makes a domain depend on its default, so the CASCADE would drop the domain and the column `rv_cascade_orders.fallback` with its data. The migration runs `alter domain ... drop default;` right before dropping the routine and sets the default again afterwards;
  - the index `rv_cascade_orders_mv_norm_idx` on the materialized view `rv_cascade_orders_mv`, which itself calls nothing. It is re-created with `CREATE INDEX IF NOT EXISTS`;
  - the default of the foreign-table column `rv_cascade_ft.code`, set again with `alter foreign table`;
  - the partition clones of the WHEN trigger `rv_cascade_parted_chk`. The parent's trigger is re-created, then the clone on `rv_cascade_parted_1` is disabled again and the one on `rv_cascade_parted_2` gets its comment back. PG15+ lists clones; on PG14 they are internal.
- **What the table changes would create calling such a routine.** They cannot create it: the routine is gone until the routine phase. They leave it out, and it is created once the routine is back:
  - the new CHECK `rv_cascade_orders_code_chk`;
  - the new index `rv_cascade_orders_norm_idx`;
  - the policy `rv_cascade_orders_pol`;
  - the default TO gives `rv_cascade_orders.code`;
  - the new table `rv_cascade_fresh`: its CHECK and default;
  - the new foreign-table column `rv_cascade_ft.checked`: its default.

  The FROM CHECK `rv_cascade_orders_id_chk` calls the routine and TO rewrites it without the call. The CASCADE has already taken it, so the table changes add TO's version without dropping anything first.
- **A plpgsql routine whose return type changes.** `rv_cascade_score(integer)` goes `integer` → `bigint`, so the routine phase drops and re-creates it. The same kinds come back:
  - the domain CHECK `rv_cascade_score_d_check`;
  - the domain default of `rv_cascade_score_def`, whose column `rv_cascade_scores.preset` keeps `42`;
  - the trigger `rv_cascade_scores_guard`;
  - the statistics `rv_cascade_scores_st`;
  - the index `rv_cascade_scores_mv_idx` on `rv_cascade_scores_mv`;
  - the foreign-table default `rv_cascade_ft.preset`.

  Some routines are taken along with it although TO keeps them:
  - `rv_cascade_score_or(bigint)`, through its argument default;
  - `rv_cascade_score_reader()`, whose SQL-standard body calls `rv_cascade_score_or`;
  - the view `rv_cascade_score_view` over it.

  These are not dropped ahead of the tables, because the table changes use them: TO adds `rv_cascade_scores.bonus DEFAULT rv_cascade_score_or()`, which fills the existing row with `1`. They are re-created right after `rv_cascade_score`, and `bonus` gets its default back.
- **Created by the table changes, then taken again.** TO adds to `rv_cascade_scores` the CHECK `rv_cascade_scores_qty_chk`, the index `rv_cascade_scores_score_idx` and the generated column `doubled` (with the index `rv_cascade_scores_doubled_idx`), all calling `rv_cascade_score`. They are put back after the routine phase drops the routine.
- **Not dropped with CASCADE at all.** The fixture does not cover these cases, because the migration stops on them; unit tests and live scenarios do.
  - A routine is not dropped ahead of a change when either:
    - its CASCADE would take something pgc cannot re-create: a partitioned table (through its partition key), a range or base type (and every column of it), a foreign-data wrapper, or an operator or cast;
    - or the table changes need it: a new or changed generated column calling it, a new column on an existing table whose default calls it, or a new column of a domain whose default calls it.

    The same applies to every routine it depends on. The migration carries a `-- Routine ... is in the way of this migration ...` note, pgc prints a warning, and the change fails on PostgreSQL's dependency check, as with pgc 1.0.x.
  - The routine phase drops a routine without CASCADE, with a note and a warning, when its CASCADE would take one of those things. The migration then stops instead of losing the partitioned table and its rows.
- **CI step.** Round 2 cannot see a loss that happens on both sides alike. The `Assert reg-cascade objects match after round 1` step therefore:
  - compares, between `a` and `b`, the `rv_cascade_*` domain CHECKs and defaults, triggers (state and comment included), statistics, columns, views, policies, column defaults (foreign tables included), constraints, indexes (materialized views included), and routine definitions and ACLs;
  - checks that the round-1 SQL detaches `rv_cascade_code_def`'s default before dropping `rv_cascade_first_code()` and sets it after;
  - spot-checks `a` for:
    - the kept data;
    - working domain defaults, CHECKs and WHEN triggers;
    - the restored statistics and matview indexes;
    - the foreign-table defaults;
    - the objects created after the predropped routines;
    - `bonus` = 1, `rv_cascade_score_view`;
    - on PG15+, the clone states;
    - the argument-default users and their privileges.

### 66. Table DDL That Takes Away Publication Rows, CHECK Enforcement and Privileges (all versions; in-place CHECK flips on PG19+)
Three cases where statements from the table section change what later sections of the migration find. The objects are named `rv_regmisc_*`.

#### A partition retyped through its parent stays a publication member (`rv_regmisc_pub_*`, pg19:publications)
`test_schema.rv_regmisc_pubtbl_pt` is partitioned, with the partition `rv_regmisc_pubtbl_pt1`. Its `v` goes from `integer` to `bigint`. Only the parent is altered: the partition is never dropped, so it keeps its `pg_publication_rel` rows.
- **Modified**: `rv_regmisc_pub_list_to_empty` (`pt1`, `rv_regmisc_pubtbl_other` → empty). Expected: `DROP TABLE ONLY` for both members, the partition included (every version).
- **Modified**: `rv_regmisc_pub_list_to_all` (`pt1` → `FOR ALL TABLES`). Expected: drop+recreate before PG19. On PG19: `DROP TABLE ONLY ... rv_regmisc_pubtbl_pt1`, then `SET ALL TABLES`. PostgreSQL refuses `SET ALL` while any member is left.
- **Modified**: `rv_regmisc_pub_list_to_except` (`pt1` → `FOR ALL TABLES EXCEPT (TABLE rv_regmisc_pubtbl_other)`, PG19+). Same order as above. Before PG19 TO lacks it, so it is dropped.
- **Unchanged**: `rv_regmisc_pub_unchanged` lists `pt1` and `rv_regmisc_pubtbl_other`. Expected: no statement.

#### A CHECK enforceability flip next to a rewrite of the column it reads (`rv_regmisc_ck_*`, pg19:tables)
`ALTER COLUMN ... TYPE` and `SET EXPRESSION` rewrite the table. The rewrite checks every row against the CHECK constraints that are enforced at that moment. The FROM rows are chosen so that the statement order matters.
- **Relaxed** (enforced → `NOT ENFORCED`): the rewritten rows violate the CHECK.
  - `rv_regmisc_ck_expr`: STORED `g` goes from `c * 2` to `c * 3`, and the row becomes −6 against `g > -5`.
  - `rv_regmisc_ck_type`: `numeric(6,2)` → `numeric(6,0)` rounds 0.6 to 1 against `c <> 1`.
  - `rv_regmisc_ck_virt`: VIRTUAL, PG18+.
  - Expected on PG19: `ALTER CONSTRAINT ... NOT ENFORCED` before the rewrite.
- **Tightened** (`NOT ENFORCED` → enforced): the FROM rows violate the CHECK and the rewritten ones do not.
  - `rv_regmisc_ck_tight_expr`: `c * 2` → `c + 1`, row −6 → −2.
  - `rv_regmisc_ck_tight_type`: 0.6 → 1 against `c <> 0.6`.
  - Expected on PG19: `ALTER CONSTRAINT ... ENFORCED` after the rewrite. That statement validates the rows.
- PG18 drops the CHECK first and re-adds it last.
- PG14–17 cannot parse `NOT ENFORCED`. There the relaxed tables lose their CHECK and the tightened ones gain it, and every rewrite still applies.

#### A partition's own CHECK under its parent's rewrite (`rv_regmisc_ckfam_*`, pg19:tables)
`test_schema.rv_regmisc_ckfam_p` is partitioned. Its `c` goes from `numeric(6,2)` to `numeric(6,0)`, and each partition holds a row with 0.6, which becomes 1. The partitions leave the type change to the parent. The parent's `ALTER COLUMN ... TYPE` recurses, runs before the partitions' own alters, and checks their rows against their own CHECKs.
- **Modified**: `rv_regmisc_ckfam_p1` has `CHECK (c <> 1)`.
  - PG18+: it goes `NOT ENFORCED`.
  - PG14–17: TO cannot parse `NOT ENFORCED`, so the CHECK is removed.
  - Expected: `ALTER CONSTRAINT ... NOT ENFORCED` on PG19, `DROP CONSTRAINT` before that. It is emitted in the block that runs before every table alter ("CHECK constraints let go before a parent's alter rewrites their table"), ahead of the parent's `ALTER COLUMN ... TYPE`. The PG18 re-add comes after it.
- **Modified**: `rv_regmisc_ckfam_p2` has `CHECK (c <> 0.6)`, going from `NOT ENFORCED` (PG18+; missing on PG14–17) to enforced. Expected: `ALTER CONSTRAINT ... ENFORCED` (PG19) or `ADD CONSTRAINT` after the parent's rewrite, which validates the rounded row.
- The same rule applies to an `INHERITS` child's own (not inherited) CHECK under a parent's `ALTER COLUMN ... TYPE`. A unit test covers it rather than this fixture. The child's own `ALTER COLUMN ... TYPE` of an inherited column still fails with "cannot alter inherited column" on every version (pre-existing).

#### Privileges the table DDL takes away (`rv_regmisc_grants_*`, fix:grants)
Owned by `pgc_pg19_grants_owner`, in `public`, using the fix:grants roles.
- **Modified**: `rv_regmisc_grants_readd`. `g` goes from plain to `GENERATED ALWAYS AS (qty * 2) STORED`, which is a drop + add on every version. Reader and the delegate keep `SELECT (g)`. Sub's `SELECT (g)` from the delegate goes.
  - Expected: `GRANT SELECT (g)` to reader and to the delegate `WITH GRANT OPTION` after the re-add.
  - No `REVOKE`: sub's entry went with the old column. A `REVOKE ... GRANTED BY` or a `SET ROLE` revoke would fail with `permission denied for column`.
- **Modified**: `rv_regmisc_grants_rebuilt`. The partition key changes from `RANGE (id)` to `RANGE (k)`, so the table is dropped and recreated. Its column privileges start empty.
  - Expected: `GRANT SELECT (code)` to reader and to the delegate, and the table-level `GRANT ... WITH GRANT OPTION`.
  - No revoke of sub's column entry.
  - The CI round trip runs `GRANTS_MODE=full`. In `addonly` mode the recreated table is planned from an empty ACL too, so its table-level grants are issued again (unit test).
- **Modified**: `rv_regmisc_grants_pt1`, a partition of `rv_regmisc_grants_pt`, whose `v` becomes `bigint`. The partition is not dropped and keeps its privileges.
  - Expected: reader's `SELECT` is revoked plainly.
  - Sub's `SELECT` (granted by the delegate) is revoked in the delegate's name: `GRANTED BY` on PG19+, `SET ROLE` before.

### 67. Dumps Written by Different pgc Versions (all versions)

No fixture: which pgc version wrote a dump is a property of the pair of dump files, not of the schema. The CI round trip always writes both dumps with the binary under test, so no `schema_a.sql`/`schema_b.sql` object can reach this case. It is covered by:
- a dedicated CI step;
- unit tests in `comparer/tests/core/mixed_versions.rs`;
- integration tests in `app/tests/compare_pipeline.rs` (`// ==== reg:mixed tests ====`) that run the `pgc` binary;
- live runs with real pgc 1.0.27 and 1.1 dumps of the same databases on PG14, 16, 18 and 19.

Every dump written by pgc 1.1 or later records `server_version_num`. No dump written by pgc 1.0.x does, and that is how pgc tells them apart.

**Refused**: FROM written by pgc 1.1+, TO written by pgc 1.0.x.
- A 1.0.x dump:
  - leaves out tables whose names start with `pg` but not `pg_` (its `tablename not like 'pg_%'` treated `_` as a wildcard);
  - stores SQL-standard routine bodies empty;
  - has no publication row filters, column lists, `TABLES IN SCHEMA` members or options;
  - holds the subscriptions of the cluster's other databases.
- So two dumps of the same database used to produce `drop table if exists m.pgbench_accounts;`, `create or replace procedure ... as $$$$;`, `ALTER PUBLICATION pub_m SET TABLE m.t;` (filter and column list lost) and `CREATE SUBSCRIPTION` of another database's subscription.
- `pgc --command compare` and the `--config` chain now stop with an error telling the user to re-dump TO with pgc 1.1 or later. pgc exits non-zero and writes no script (`Comparer::check_dump_versions`, called by `main.rs` on the dumps it read).

**Compared, with a warning**: FROM written by pgc 1.0.x, TO written by pgc 1.1+.
- stderr carries a warning, a line naming the TO-only tables pgc 1.0.x could not see, and a line naming the TO subscriptions the old dump lists under the same name.
- The script opens, after its header, with a fixed comment naming the risk.
- The migration starts with `set standard_conforming_strings = on;`.
  - A 1.0.x FROM dump does not record the server's default. For an `off` server it holds every backslash literal doubled, so each one is rewritten from the TO dump's standard form.
  - The SET is withdrawn from an otherwise empty migration.
- Tables whose names start with `pg` but not `pg_` are created. Where FROM already has one, this fails with `relation already exists`.
- Under `--output-for-production`, `create table if not exists` skips only that `CREATE TABLE`:
  - constraints added to the table separately (a named CHECK, a foreign key) still fail with `already exists`;
  - any other difference in that table is silently not applied.
- Every TO subscription is created, because a 1.0.x dump lists the subscriptions of every database in the cluster and cannot say which are FROM's.
  - Matched by name, a subscription only a sibling database had was silently never created.
  - Now one FROM really has fails with `subscription already exists`, which PostgreSQL checks before contacting the publisher.
  - FROM-only names are dropped once each, with `DROP SUBSCRIPTION IF EXISTS`.
- SQL-standard routines are re-created, and publications and replica identities are set again.

**Unchanged**: two dumps written by pgc 1.0.x, and two dumps written by pgc 1.1+, give byte-identical output to before.

**CI assertion**: a standalone step on PG14–19.
- It creates `pgc_mixed_a` and `pgc_mixed_b`, both set to `standard_conforming_strings = off` on PG14–18, and dumps them.
- It derives "1.0.x-shaped" dumps by removing `server_version_num`, `standard_conforming_strings_off`, `available_extensions` and the tables 1.0.x could not see.
- It checks that:
  - the new-FROM/old-TO compare exits non-zero, writes nothing and says to re-dump TO;
  - the old-FROM/new-TO compare warns on stderr (naming `public.pgmixed_hidden`), carries the comment and starts with the guard;
  - applied with psql, that migration stores the default `C:\temp` (7 characters);
  - two old-shaped dumps get neither the guard nor the warning;
  - a fresh second compare is empty.
- Finally it adds a `connect = false` subscription to `pgc_mixed_b`, copies it into the old-shaped FROM dump as another database's, and checks that the compare still emits `CREATE SUBSCRIPTION pgc_mixed_sub` once and names it on stderr.

### 68. Tables Dropped and Recreated with Everything That Went with Them (all versions)
All in the `pg19:tables` block, in `test_schema` except one table in `public`; every version from PG14 runs all of it. A partition-key change (or a type change on a partition-key column, or a plain table turned into a partitioned one) makes pgc drop the table and create it again from TO. Its rows are lost by design, but everything its `DROP TABLE` took along must come back in the same migration.
- **Modified**: `rv_recreate_orders`: `PARTITION BY RANGE (id)` → `RANGE (region_id)`. `id` is `serial`, and the table references `rv_recreate_regions` (`rv_recreate_orders_region_fk`). There are rows in every partition.
  - `rv_recreate_orders_p1` (`FROM (0) TO (100)`) and `rv_recreate_orders_def` (`DEFAULT`) keep their bound, so their own hash does not change; the parent's `DROP TABLE` still takes them. pgc used to leave them out of round 1: the parent came back with no partitions, every insert failed, and a second run was needed.
  - `rv_recreate_orders_p2` changes its bound (`TO (200)` → `TO (300)`) and keeps its own `LIST (kind)`. Its unchanged partition `rv_recreate_orders_p2a` is created again too, and the TO-only `rv_recreate_orders_p2b` waits until `_p2` is back.
  - The unchanged `_p1` carries an index, a comment, a grant, extended statistics (`rv_recreate_orders_p1_st`), a trigger of its own and a view (`rv_recreate_orders_p1_v`). The view is dropped before the table section and recreated after it; the rest is restored once `_p1` is created again.
  - The parent's row trigger `rv_recreate_orders_touch` is cloned onto every partition (PG15+ lists the clone on each). The clones are not created a second time.
  - `rv_recreate_orders_audit` is a statement trigger on the parent and, under the same name, a row trigger of `_p1`'s own. PostgreSQL clones only row triggers, so the partition's is created again with `_p1`. pgc used to take it for a clone by its name alone and leave it out.
  - The serial sequence `rv_recreate_orders_id_seq` (value 500, a `USAGE` grant) is detached with `ALTER SEQUENCE ... OWNED BY NONE` before the `DROP TABLE`, and owned by `id` again after the tables, so it keeps its value and grant. Before, the `DROP TABLE` dropped it and the `CREATE TABLE ... DEFAULT nextval(...)` failed, leaving neither the table nor the sequence.
  - The table's own foreign key `rv_recreate_orders_region_fk`, which `CREATE TABLE` leaves out, is added back after the tables. Under `--output-for-production` on PG14–17 it is added in one statement: PostgreSQL before 18 cannot add a `NOT VALID` foreign key to a partitioned table.
- **Modified**: the foreign keys held against the recreated table are dropped before the table section and added back after the tables (`NOT VALID` + `VALIDATE` under `--output-for-production`). Before, `DROP TABLE` failed with "cannot drop table ... because other objects depend on it" and the migration never converged. The referencing rows leave the key `NULL`, so adding the keys back succeeds; a row still pointing at a lost row would make the add, or its validation, fail. The keys are:
  - `rv_recreate_order_lines.rv_recreate_order_lines_order_fk`: unchanged.
  - `rv_recreate_order_refunds.rv_recreate_order_refunds_order_fk`: TO adds `ON DELETE CASCADE`; it is dropped once and added from TO.
  - `rv_recreate_p1_notes.rv_recreate_p1_notes_order_fk`: references the unchanged partition `_p1`.
- **Modified**: `rv_recreate_events`: a plain table with a `bigserial` key becomes `PARTITION BY RANGE (created_on)`, with the TO-only partition `rv_recreate_events_2024`. It is referenced by `rv_recreate_event_tags.rv_recreate_event_tags_event_fk`, and gets the same sequence and foreign-key handling.
- **Modified**: `rv_recreate_aa_event_archive`: a plain table with a row that TO attaches to `rv_recreate_events` (`FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')`). It sorts before `rv_recreate_events`, so taken in FROM order its `ATTACH` ran before the `DROP TABLE`, which then took it along, row and all, without an error. It is now attached once `rv_recreate_events` is back, and keeps its row.
- **Modified**: `public.rv_recreate_pub_orders`: a plain table TO partitions (`RANGE (k)`, with `public.rv_recreate_pub_orders_p1`), referenced by `test_schema.rv_recreate_pub_lines.rv_recreate_pub_lines_order_fk`. The dump's `search_path` reaches `public`, so the foreign key's definition names the table without its schema. It is now dropped first like the others; before, pgc looked for the table in `test_schema`, found none, and the `DROP TABLE` failed.
- **Modified**: `rv_recreate_tree_c`: a partition of `rv_recreate_tree` that changes its own key (`LIST (id)` → `LIST (k)`), so it is dropped and recreated with its partition `rv_recreate_tree_c1` while `rv_recreate_tree` stays. `rv_recreate_tree_refs.rv_recreate_tree_refs_fk` references `rv_recreate_tree`; PostgreSQL copies that key onto every partition of it, so it blocks `DROP TABLE rv_recreate_tree_c` and is dropped first too.
- **Modified**: `"rv_recreate_Cased"` (mixed-case names, `RANGE ("Id")` → `RANGE ("K")`). It has the row trigger `"rv_recreate_Cased_Trg"` cloned onto its partitions, the unchanged `"rv_recreate_Cased_P1"`, the TO-only `"rv_recreate_Cased_P2"`, and `"rv_recreate_CasedRefs"."rv_recreate_Cased_FK"` referencing it.
  - The foreign key is added back with its quoted identifiers as they are. pgc lowercased every identifier of an `ADD CONSTRAINT`, so the key was lost after its drop.
  - The new partition waits until its quoted parent is back, and the parent's trigger is not created a second time on the partitions.
- With `use_drop` off, the `DROP TABLE` is only commented out, so the partitions, sequences and foreign keys are left alone.
- **Not covered**: a foreign-table partition of a recreated table. The dump records no partition membership for a foreign table, so the `DROP TABLE` takes it along unseen, and a second run creates it again, as the standalone foreign table pgc knows it as.
- **CI assertion** (`.github/scripts/assert-pre-recreate.sh`): after round 1, `a` must match `b` for every `rv_recreate_*` relation in `test_schema` and `public` in:
  - the partition tree (kind, parent, bound, partition key);
  - the sequences (options, `OWNED BY`, privileges) and column defaults;
  - the foreign keys (definition, validated);
  - indexes, triggers, statistics, privileges and comments.

  `b` must have 12 partitions and 8 foreign keys, so the check is not vacuous. `a` must also keep the sequence values (500 and 2) and the row of `rv_recreate_aa_event_archive`. The round-1 SQL must:
  - detach both sequences;
  - create the unchanged partitions;
  - drop the six referencing foreign keys first.

### 69. What a Routine's DROP ... CASCADE Takes Through a View, and the Privileges of What the Migration Creates (PG14+)
All objects are named `rv_pcascade_*` and sit in the `fix:sqlbody` block of both schema files:
- the routines in `data`;
- the relations in `test_schema`;
- the overload's view and materialized view in `public`.

CI runs `GRANTS_MODE=full`. The fixture needs PG14+ (a SQL-standard body).

- **Modified (return type, default privileges)**: `data.rv_pcascade_norm(text)` returns `text` in FROM and `varchar` in TO, so the routine phase drops it with `DROP FUNCTION ... CASCADE`.
  - Both sides run `ALTER DEFAULT PRIVILEGES IN SCHEMA data GRANT EXECUTE ON FUNCTIONS TO pgc_pg19_pcascade_exec` and `ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_pcascade_owner IN SCHEMA data GRANT EXECUTE ON FUNCTIONS TO pgc_pg19_pcascade_reader`.
  - The routine revokes the first default's grant and grants `EXECUTE` to the reader.
  - The re-created routine starts with the default privileges of whichever role creates it, so expect `REVOKE EXECUTE ON FUNCTION data.rv_pcascade_norm(t text) FROM pgc_pg19_pcascade_exec;` and `GRANT EXECUTE ON FUNCTION data.rv_pcascade_norm(t text) TO pgc_pg19_pcascade_reader;` in the same migration.
- **Unchanged, read through a view that calls the routine**: the CASCADE takes all of the following:
  - `test_schema.rv_pcascade_items_v` calls the routine. It has a grant and a comment, and SELECT is revoked from `pgc_grant_reader`, which `test_schema`'s default privileges give every new view;
  - `rv_pcascade_items_v2` reads it. It is owned by `pgc_pg19_pcascade_owner` and has a column comment;
  - the materialized view `rv_pcascade_items_mv` reads it. It has a commented unique index and `ndistinct` statistics;
  - `data.rv_pcascade_count()` reads `rv_pcascade_items_v2` in a `BEGIN ATOMIC` body. PUBLIC and the default's grantee are revoked;
  - the rule `rv_pcascade_items_v2_del` on `rv_pcascade_items_v2` reads `rv_pcascade_items_v`.

  pgc tells which views call the routine from the routines each view's `_RETURN` rule depends on in `pg_depend`. The dump now records them per view (`routine_dependencies`). Expect, in order:
  1. The rule is dropped first.
  2. In the Views DROP section, `data.rv_pcascade_count()`, the view over the view and the materialized view are dropped before `drop view if exists test_schema.rv_pcascade_items_v;`.
  3. All of them are re-created after the routine, each after what it reads, with their owners, comments, index, statistics, rule and grants.

  pgc 1.0.x and 1.1.0 re-created only `rv_pcascade_items_v`. The rest, the materialized view's data included, was missing until the next compare, and 1.1.0 failed to re-create the rule.
- **Unchanged, nothing reads it**: `test_schema.rv_pcascade_solo_v` calls the routine, and nothing reads it.
  - It is re-created right after the routine (Phase 7 of `compare_routines_and_views`), as before.
  - It now gets its privileges back in the same migration, planned from the schema's default privileges like a dropped view: `GRANT SELECT ON TABLE test_schema.rv_pcascade_solo_v TO pgc_pg19_pcascade_reader;` and `REVOKE SELECT ON TABLE test_schema.rv_pcascade_solo_v FROM pgc_grant_reader;`.
- **Unchanged, calling an unchanged overload**: `data.rv_pcascade_norm(integer)` does not change.
  - `public.rv_pcascade_int_v` calls it. The materialized view `public.rv_pcascade_int_mv` (with a unique index) reads it, and it has an `INSTEAD OF INSERT` trigger, which pgc does not model.
  - Its definition names `data.rv_pcascade_norm`, but the CASCADE does not take it. Expect neither view dropped, only Phase 7's harmless `CREATE OR REPLACE VIEW public.rv_pcascade_int_v`, and the trigger kept.
  - In FROM, the delegate `pgc_pg19_pcascade_owner`, which holds the grant option, granted SELECT to `pgc_pg19_pcascade_reader`. TO lacks that grant. The view keeps its privileges, so expect `REVOKE SELECT ON TABLE public.rv_pcascade_int_v FROM pgc_pg19_pcascade_reader GRANTED BY pgc_pg19_pcascade_owner;` on PG19. Before PG19 it is `SET ROLE pgc_pg19_pcascade_owner;` followed by the `REVOKE`.
- **NULL ACL under default privileges**: `data.rv_pcascade_plain_ret(integer)` (return type `integer` → `bigint`) and the TO-only `data.rv_pcascade_plain_new()` are created before the default privileges, so their ACL is NULL on both sides.
  - The migration creates them under those defaults. Expect `REVOKE EXECUTE ... FROM pgc_pg19_pcascade_exec;` and `REVOKE EXECUTE ... FROM pgc_pg19_pcascade_reader;` for each.
  - That leaves `{=X/postgres,postgres=X/postgres}`, which the next compare reads as the NULL ACL it spells out. Round 2 has no `REVOKE EXECUTE ... FROM PUBLIC`.
- **Added**: `data.rv_pcascade_new()`, TO only, revokes the first default's grant. Expect `REVOKE EXECUTE ON FUNCTION data.rv_pcascade_new() FROM pgc_pg19_pcascade_exec;` and, for the other role's default, `... FROM pgc_pg19_pcascade_reader;`.
- **NULL TO ACL against an explicit FROM ACL (all versions)**: `test_schema.update_timestamp()` (section 4) grants `EXECUTE` to `pgc_grant_reader` in FROM and has a NULL ACL in TO.
  - Expect only `REVOKE EXECUTE ON FUNCTION test_schema.update_timestamp() FROM pgc_grant_reader;`.
  - pgc 1.0.x and 1.1.0 also emitted `REVOKE EXECUTE ... FROM PUBLIC`, although TO's NULL ACL grants PUBLIC `EXECUTE`.
- **Not in the fixture**: unit tests and live scenarios cover these, because the round trip would not converge or needs an older dump. Such a view is left to the routine's CASCADE as before, and its readers come back on the next compare:
  - a view whose plain `DROP VIEW` would fail: a routine with its row type in its result or arguments, a policy reading it, or a table column of its row type, on the view itself or on a view reading it;
  - any view in a FROM dump written by pgc 1.0.x, which records no routine dependencies.
- **CI step** (`.github/scripts/assert-pre-cascade.sh`). Round 2 cannot see a loss that happens on both sides alike, so the step:
  - compares between `a` and `b`:
    - every `rv_pcascade` relation in `test_schema` and `public` (kind, owner, sorted ACL, comment, definition), with its columns, indexes, triggers and statistics;
    - the rule;
    - the routines with their ACLs (a NULL ACL read as `acldefault`);
    - the default privileges of `data`;
    - the ACL of `test_schema.update_timestamp()`;
  - checks the round-1 order: drop of the view over the view, then drop of the view, then drop of the function, then re-create of the view;
  - checks that `rv_pcascade_solo_v`, `rv_pcascade_int_v` and `rv_pcascade_int_mv` are not dropped;
  - checks that no `REVOKE ... FROM PUBLIC` hits the NULL-ACL routines;
  - spot-checks that in `a`:
    - the views, the materialized views, the routine and the rule work;
    - the trigger is still there;
    - PUBLIC keeps `EXECUTE` where TO has it.

### 70. Privileges, Subscription Publication Lists and Publication Rows That Block Table Changes (all versions; column lists / row filters PG15+, generated column lists PG18+, EXCEPT PG19+)
Objects are named `rv_pgrants_*`. The roles, created idempotently, are `"pgc_pg19_Pgrants_Reader"` (mixed case), `"pgc_pg19_pgrants reader"` (with a space), `"pgc_pg19_Pgrants_Deleg"` (mixed case) and `pgc_pg19_pgrants_app`.

#### Grantees That Need Quotes (fix:grants block, `public`)
aclitem text quotes a role name only when it holds a character other than an ASCII letter, digit or `_`. So `pgc_pg19_Pgrants_Reader` arrives bare, and written as-is it names `pgc_pg19_pgrants_reader`. A reserved word such as `user` does not parse at all. Every grantee is now spelled the way the grantor path already spelled it.
- **Modified**: `rv_pgrants_tbl`. It has no grants in FROM. TO adds `SELECT` and `UPDATE (note)` for the mixed-case role and `INSERT` for the spaced one. Expected: `GRANT ... TO "pgc_pg19_Pgrants_Reader";` / `TO "pgc_pg19_pgrants reader";`.
- **Modified**: `rv_pgrants_tbl_revoke`. FROM grants `SELECT` to the mixed-case role; TO grants nothing. Expected: `REVOKE SELECT ... FROM "pgc_pg19_Pgrants_Reader";`.
- **Modified**: `rv_pgrants_chain`. In FROM, the mixed-case delegate holds `SELECT WITH GRANT OPTION` and granted `SELECT` to the mixed-case reader; TO has no grants. Expected: the grantor-named revoke (`GRANTED BY "pgc_pg19_Pgrants_Deleg"` on PG19, `SET ROLE "pgc_pg19_Pgrants_Deleg"` before). The owner-level `REVOKE GRANT OPTION FOR` / `REVOKE` then name `"pgc_pg19_Pgrants_Deleg"` the same way.
- **Added**: the default privilege `FOR ROLE pgc_pg19_grants_owner IN SCHEMA public GRANT SELECT ON TABLES TO "pgc_pg19_Pgrants_Reader"`, TO only. No fixture object is created by that role, so nothing else picks it up.

#### NULL ACLs of Routines and Types (fix:grants block, `public`)
A NULL `proacl` / `typacl` is the default privileges, `acldefault`, which give PUBLIC `EXECUTE` or `USAGE`. It is now compared as that default. Before, it was compared as "no privileges", so it looked equal to `{owner=X/owner}` and a `REVOKE ... FROM PUBLIC` was never migrated, even though round 2 was empty.
- **Modified**:
  - `rv_pgrants_secret()`: a `SECURITY DEFINER` function whose ACL is NULL in FROM, with EXECUTE revoked from PUBLIC in TO. Expected: `REVOKE EXECUTE ... FROM PUBLIC;`.
  - `rv_pgrants_dom` (domain) and `rv_pgrants_enum` (enum): NULL in FROM, USAGE revoked from PUBLIC in TO. Expected: `REVOKE USAGE ON TYPE ... FROM PUBLIC;`.
  - `rv_pgrants_granted()`: NULL in FROM. TO grants EXECUTE to the mixed-case role and revokes it from PUBLIC. Both statements are expected in round 1; before, the REVOKE only came in round 2.
- **Modified** (the reverse direction):
  - `rv_pgrants_reopen()`: PUBLIC revoked in FROM, NULL in TO. Expected: `GRANT EXECUTE ... TO PUBLIC;`.
  - `rv_pgrants_kept()`: EXECUTE granted to the spaced role in FROM, NULL in TO. Expected: only that role's EXECUTE is revoked. Before, PUBLIC's was revoked as well.
  - `rv_pgrants_emptied()`: everything revoked in FROM, the owner's own EXECUTE included, so `proacl` is `{}` rather than NULL; NULL in TO. Expected: `GRANT EXECUTE ... TO PUBLIC;`. The dump now tells `{}` from NULL through `acl_explicitly_empty`. The owner's own entry is not compared, as for every object.
- **Added**: `rv_pgrants_new_fn()` and `rv_pgrants_new_dom`, created with PUBLIC revoked. Expected: the REVOKE in round 1.

#### Objects the Migration Creates Under Another Role's Default Privileges (fix:grants block)
PostgreSQL applies only the creating role's `pg_default_acl` rows. CI runs the migration as a superuser, while the FROM rows belong to `pgc_pg19_pgrants_app`, and are the same on both sides:
- database-wide: `REVOKE USAGE ON TYPES FROM PUBLIC`;
- in `test_order`: `GRANT EXECUTE ON FUNCTIONS TO "pgc_pg19_Pgrants_Reader"`.

A new or recreated routine or type is now compared against what any creator could give it. What the target lacks is revoked and what it holds is granted, each a no-op for some creators. Taking the rows for granted skipped the REVOKE, or the GRANT, that the superuser's object needed.
- **Modified**: `test_order.rv_pgrants_app_fr()`. It is `SECURITY DEFINER`, owned by the app role, with PUBLIC revoked and the mixed-case role granted. Its return type changes from `integer` to `bigint`, which is a drop plus a create. Expected after the create: `REVOKE EXECUTE ... FROM PUBLIC;` and `GRANT EXECUTE ... TO "pgc_pg19_Pgrants_Reader";`.
- **Added**: `test_order.rv_pgrants_app_new()`, with the same ACL, and `public.rv_pgrants_app_dom`, with PUBLIC revoked, both owned by the app role. Expected: the same two statements for the function, and `REVOKE USAGE ON TYPE public.rv_pgrants_app_dom FROM PUBLIC;`.
- **Modified**: `public.rv_pgrants_range`, a range type with USAGE revoked from PUBLIC and granted to the mixed-case role on both sides. It loses its `subtype_diff`, so it is dropped and recreated. Expected: the REVOKE and the GRANT again after the create. Before, the recreated type kept PUBLIC's USAGE and lost the grant until round 2.
- Every other new type with a NULL ACL gets a no-op `GRANT USAGE ... TO PUBLIC`, because the database-wide row may have taken PUBLIC's USAGE away. That leaves an explicit ACL equivalent to NULL.
- A type whose ACL alone differs is no longer altered. A range type used to be dropped with `CASCADE` and recreated, taking its columns with it.
- Multirange types get no GRANT or REVOKE at all, since PostgreSQL refuses them.
- A database-wide `REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC` is left out of the fixture. The no-op `GRANT EXECUTE ... TO PUBLIC` it causes on the new ordered-set aggregate `pg19_routines_agg_kind` fails, because GRANT spells that aggregate's arguments with `ORDER BY` (a separate bug). Unit tests cover the case.

#### Subscriptions (pg19:publications block; every version)
Both subscriptions are created `WITH (connect = false, slot_name = NONE)`, so they stay disabled and exist on both sides.
- **Modified**: `rv_pgrants_sub_setpub`. Its publications change from `rv_pgrants_remote_p1` to `rv_pgrants_remote_p1, rv_pgrants_remote_p2` while it is disabled. Expected: `SET PUBLICATION ... WITH (refresh = false);`. PostgreSQL refuses the default refresh on a disabled subscription.
- **Modified**: `rv_pgrants_sub_quoted`. TO subscribes to `"RvPgrants_Remote", "rv pgrants remote", "select"`. Expected: the names quoted as `quote_ident` would. The dump keeps the names raw, so subscription hashes are unchanged.
- Covered by unit tests only:
  - A subscription that is enabled by the migration gets `REFRESH PUBLICATION` right after its `ENABLE`.
  - With `--use-single-transaction`, `SET PUBLICATION` is always `WITH (refresh = false)`, and `REFRESH PUBLICATION` goes into a "Subscriptions post-commit" section after `commit;`. A refresh cannot run inside a transaction block.

#### Publication Rows That Block a Table Change (pg19:publications block, `test_schema`)
PostgreSQL refuses to drop or retype a column that a member's column list or row filter uses ("cannot alter type of a column used by a publication WHERE clause", column lists included). It also refuses `SET UNLOGGED` of a member, and on PG19 `SET UNLOGGED` or `ATTACH PARTITION` of an EXCEPT entry. These rows are now released in a section ahead of the tables, "Publications: release members that block table changes". The publications section then puts back what TO keeps.
- **Modified**: `rv_pgrants_pub_collist` (PG15+). Its column list changes from `(id, code, qty)` to `(id, code)`, while TO drops `qty` and retypes `code` to `varchar(20)`.
- **Unchanged, released and restored**:
  - `rv_pgrants_pub_collist_same` (PG15+): `(id, code)` on the same table.
  - `rv_pgrants_pub_filter` (PG15+): `WHERE (qty > 0)`, while `qty` becomes `bigint`. The publication hashes are equal, so before this fix nothing ever unblocked the ALTER.
- **Modified**: `rv_pgrants_pub_unlogged` (every version). TO makes `rv_pgrants_publog` UNLOGGED and leaves it out, because unlogged tables cannot be published.
- **Unchanged, released and restored**: `rv_pgrants_pub_gen` (PG18+). Its column list includes a stored generated column whose type and expression change, which is done as a drop plus add.
- **Modified**: `rv_pgrants_pub_except` (PG19+). `FOR ALL TABLES EXCEPT` lists a table that TO makes UNLOGGED and one that TO attaches as a partition; TO is plain `FOR ALL TABLES`. On PG14–18 the FROM side fails and TO's publication is created.

#### Publications a `USE_DROP=false` Migration Keeps (CI assertion, step B)
`assert-pre-grants.sh` runs its own migration with `USE_DROP=false`, between the databases `pgc_pgrants_a` and `pgc_pgrants_b`. It covers:
- a FROM-only publication with a row filter on a column that TO retypes (PG15+);
- a FROM-only publication whose only member TO makes UNLOGGED (every version);
- on PG15–18, a `FOR TABLE t (id, qty)` publication that TO makes `FOR ALL TABLES`. That is a drop plus recreate, which stays commented out without `use_drop`.

Nothing alters these publications, so the released rows are set back after the tables with `ALTER PUBLICATION ... SET TABLE <the FROM list, with its column lists and row filters>`. It is a `SET`, so a replayed migration does not fail on it. The UNLOGGED member cannot come back: the script says so in a comment and pgc prints a warning. Before, the rows were lost without a word, and round 2 was empty.

**CI assertion**: `.github/scripts/assert-pre-grants.sh`. After round 1 it compares `a` with `b`:
- PUBLIC's EXECUTE / USAGE on every routine and type both databases have, reading a NULL ACL as `acldefault`;
- the `rv_pgrants_*` privileges by grantee name, including `test_order.rv_pgrants_app_*` and `rv_pgrants_range`;
- the default privileges of `pgc_pg19_grants_owner` and `pgc_pg19_pgrants_app`;
- the `rv_pgrants_*` subscription publication lists;
- the publication rows (column lists, row filters and EXCEPT entries), and the columns, persistence and partition status of their tables.

It also checks that the round-1 SQL:
- quotes these names;
- skips the refresh;
- releases each blocking row before the table statement it blocks;
- re-applies the ACLs of created and recreated objects.

Step B then runs the `USE_DROP=false` migration described above.

### 71. Retyped Columns under Generated Columns, What Blocks a Column Drop + Add, Statistics Column Order, CHECK Families and Partition Trigger Clones (all versions)
All objects are in `test_schema`, prefixed `rv_pcols_`, in the `pg19:tables` block. The exception is the partitioned parent `rv_pcols_outside.rv_pcols_out`: its schema is outside the dumped ones.

VIRTUAL columns (`rv_pcols_gen_virt`) need PG18+. The CHECK tables created with `NOT ENFORCED` (`rv_pcols_ckr_*` in TO, `rv_pcols_ckt_*` and `rv_pcols_ckc_*`) need PG18+. On older majors those tables fail harmlessly on the side that uses the syntax.

**Retype under a generated column**
- **Modified**: `rv_pcols_gen`: `a` changes from `integer` to `bigint`.
  - The unchanged STORED generated column `g` reads `a` and has an index, a CHECK, extended statistics (`rv_pcols_gen_st` on `g, flag`) and a view (`rv_pcols_gen_v`).
  - PostgreSQL refuses `ALTER COLUMN a TYPE` while `g` reads it, on every version. So `g` is dropped before the retype and re-added from TO after it.
  - The view is dropped first and re-created; the index, CHECK and statistics are restored.
- **Modified**: `rv_pcols_gen_expr`: the same retype, with the expression changing from `a * 2` to `a * 3` and `g` becoming `bigint`. It is a drop + add even where `SET EXPRESSION` exists.
- **Modified**: `rv_pcols_gen_plain`: `g` becomes a plain column and keeps its values. Its `DROP EXPRESSION` runs before the retype.
- **Modified**: `rv_pcols_gen_gone`: `g` goes away. Its drop runs before the retype.
- **Modified**: `rv_pcols_gen_both`: both `a` and the generated `g` that reads it go away. `g` is dropped first: PG15+ refuses to drop `a` while `g` depends on it, and PG14 drops `g` along with `a`.
- **Modified**: `rv_pcols_gen_part` / `rv_pcols_gen_part_1`: the partitioned parent's drop, retype and add recurse to the partition. The partitioned index is restored.
- **Modified**: `rv_pcols_gen_virt` (PG18+): the same retype under a VIRTUAL column.

**INHERITS retype**
- **Modified**: `rv_pcols_ihp` (`a` integer → bigint) with children `rv_pcols_ihc` (generated `g` = `a * 2`, indexed), `rv_pcols_ihgc` (a child of `rv_pcols_ihc`, with its own generated `h` = `a + 1`) and `rv_pcols_ihc2` (plain).
  - Only the parent retypes `a`; its `ALTER COLUMN ... TYPE` recurses. A child's own retype of the inherited column failed ("cannot alter inherited column").
  - `g` and `h` are dropped in a block ahead of every table alter ("Generated columns let go before a parent's alter retypes a column they read"). `g`'s drop also removes the grandchild's inherited copy.
  - They are re-added after the parent's retype. The grandchild gets `g` back through the child's recursing `ADD COLUMN`.
  - The rows are recomputed and the index restored.

**What blocks a column drop + add**
- **Modified**: `rv_pcols_blk`: `a` and the generated `"G x"` become `bigint`. `"G x"` has:
  - a named NOT NULL `rv_pcols_blk_gx_nn`;
  - a mixed-case unique key `"rv_pcols_blk_G_key"`, which `rv_pcols_blk_ref.gx` references;
  - a policy `rv_pcols_blk_pol`;
  - an `UPDATE OF` trigger `rv_pcols_blk_upd`;
  - a disabled `WHEN` trigger `rv_pcols_blk_when`.
- PostgreSQL refuses `DROP COLUMN` while the policy, the triggers or the other table's foreign key depend on the column ("other objects depend on it"). So these are dropped in a block ahead of every table alter ("Dependents dropped ahead of a column drop + add").
- They are put back after the column's re-add: the policy and the foreign key with the other dependents, the triggers with the other triggers, in their state.
- The unique key is restored with its column's case (before, it was lowercased to `"g x"`).
- On PG18+ the re-add names its NOT NULL: `add column ... constraint rv_pcols_blk_gx_nn not null`. Before, an auto-named one was created first and the named re-add was refused.
- **Modified**: `rv_pcols_blkp` / `_1` / `_2`: a partitioned table with an `UPDATE OF g` row trigger, and a partition's own `UPDATE OF g` trigger.
  - Both triggers are dropped ahead, and re-created after the recursing drop + add.
  - The re-created trigger clones itself afresh, so `_2`'s disabled clone and `_1`'s clone comment are set again.

**Extended statistics column order**
- **Modified**: `rv_pcols_st`: in FROM, `b` was dropped and re-added, so it sits after `c`.
  - PostgreSQL prints plain statistics columns in attnum order. So `rv_pcols_st_same` (`ON b, c`) prints `ON c, b` in FROM, and `rv_pcols_st_expr` puts its plain columns in a different order.
  - Neither is re-created: the plain columns are compared as a set, and expressions keep their order.
  - `rv_pcols_st_changed` (`b, c` → `a, b`) is still dropped and re-created.

**CHECK constraints in inheritance families**
- **Modified**: `rv_pcols_ckx_par` / `rv_pcols_ckx_ch` / `rv_pcols_ckx_gch` / `rv_pcols_ckx_ch2`: a CHECK expression change in an INHERITS family, with merged copies on a child and a grandchild. `rv_pcols_ckx_ch2` only inherits the CHECK.
  - Every copy is dropped, parents first, ahead of the table alters, then re-added.
  - The re-added child copies merge again (`conislocal`, `coninhcount = 1`).
  - Before, the parent's recursing re-add collided with the child's copy ("already exists").
- **Modified**: `rv_pcols_cku_par` / `rv_pcols_cku_ch`: the child's merged copy becomes inherited only. The parent drops and re-adds its unchanged copy around the child's drop; the child could not drop an inherited copy on its own.
- **Modified**: `rv_pcols_ckr_*` (relaxed on both), `rv_pcols_ckt_*` (tightened on both) and `rv_pcols_ckc_*` (only the child's copy relaxed, under an unchanged NOT ENFORCED parent).
  - On PG18 these are a drop + re-add, laid out as above.
  - On PG19 they use `ALTER CONSTRAINT ... [NOT] ENFORCED` in place.

**Partition trigger clones**
- **Modified**: `rv_pcols_trg_ex` and its partitions `rv_pcols_trg_ex_1` (partitioned), `rv_pcols_trg_ex_1a` and `rv_pcols_trg_ex_2`:
  - `rv_pcols_trg_changed` changes definition. Re-creating it clones it again, so `_2`'s disabled clone is disabled again afterwards.
  - `rv_pcols_trg_kept` is unchanged. Its clone on `_1` becomes `ENABLE REPLICA`; the one on `_1a` stays `ENABLE` (set again after the parent's recursing statement); the one on `_2` loses its comment.
  - `rv_pcols_trg_added` is new; its clone on `_1a` gets a comment.
  - A clone is never created or dropped on its own.
- **Added**: `rv_pcols_trg_new` with partitions `_1` and `_2`, a row and a statement trigger, and `_2`'s clone disabled. Only the parent's `CREATE TRIGGER` is emitted; per-partition ones failed with "already exists".
- **Removed**: `rv_pcols_trg_zgone` with its partition `rv_pcols_trg_agone` (which sorts first) and a row trigger. Only the parent's trigger is dropped; dropping the partition's clone failed with "cannot drop trigger ... requires it".
- **Modified**: the partitions of `rv_pcols_outside.rv_pcols_out`, whose schema is not dumped. The parent has row triggers `rv_pcols_out_ins` and `rv_pcols_out_upd`. The dump records every clone of such a partition, since the parent's state is not there to compare with.
  - `rv_pcols_out_1` (partitioned) goes from a disabled `rv_pcols_out_ins` to enabled, while its partition `rv_pcols_out_1a` becomes `ENABLE ALWAYS`.
  - `rv_pcols_out_2`'s `rv_pcols_out_upd` loses its comment and is disabled.
- **Added**: `rv_pcols_out_3`, a new partition of that parent. Its clones get their TO state set (the parent's is unknown): `rv_pcols_out_ins` disabled, a comment on `rv_pcols_out_upd`. It gets no `CREATE TRIGGER`.
- **Removed**: `rv_pcols_out_gone`.
- **Modified**: `rv_pcols_att_2` has an own trigger `rv_pcols_att_trg` in FROM. In TO it is a partition of `rv_pcols_att`, whose row trigger has that name.
  - The own trigger is dropped before `ATTACH PARTITION`, which clones the parent's; before, "trigger ... already exists".
  - The clone is then disabled.

**CI assertion**: `.github/scripts/assert-pre-columns.sh` compares a and b after round 1:
- column types and generation expressions, indexes (a partition's copy of a parent index without its name), and views;
- statistics (kinds, plain columns as a set, expressions in order);
- CHECK constraints (enforceability on PG18+, validation, `conislocal`, `coninhcount`);
- policies, and the other constraints (an auto-named NOT NULL without its name);
- triggers, clones included, with their definition, enabled state and comment;
- the recomputed generated values;
- the attach and the undumped parent's partitions;
- the order of the round-1 statements (`TEXT_CHECKS=0` skips this for production output).

### 72. Clear Command: Dependency Order and Cycles (all versions)
Both schemas contain the same objects in the `fix:sqlbody` block, so a compare leaves them alone. `pgc --command clear` on a database loaded from either file has to drop them without an error and without `CASCADE`:
- `test_schema.rv_preclear_cycle`: its CHECK `rv_preclear_cycle_chk` calls `rv_preclear_cycle_max()`, whose SQL-standard body reads the table. Neither can be dropped first, so the clear script drops the constraint ahead of both (`alter table if exists ... drop constraint if exists ...`, under a `Break dependency cycle` comment).
- `test_schema.rv_preclear_rule_src`: its rule `rv_preclear_rule_log_count` reads the view `rv_preclear_rule_count`, which reads the table. The clear script drops the rule first.
- `test_schema.rv_preclear_snap`: its default calls `rv_preclear_schema_count()`, whose SQL-standard body reads only `information_schema.schemata`. The table is dropped before the function.
- `test_schema.rv_preclear_agg(integer)` is an aggregate over the plpgsql state function `rv_preclear_agg_sfunc`, created in the usual order. The aggregate is dropped first.
- `test_schema.rv_preclear_gen`: its stored generated column `g` calls `rv_preclear_gen_bump()`, whose SQL-standard body reads the table, and the view `rv_preclear_gen_view` reads `g` (itself read by `rv_preclear_gen_view_count()`).
  - PostgreSQL 15+: this is a cycle. The clear script breaks it with `alter table if exists ... alter column g drop expression if exists`, which keeps the column. Dropping the column would be blocked by the view.
  - PostgreSQL 14: the column's use of the function is an automatic dependency (dropping the function drops the column). The view and its reader are dropped before the function.
- `rv_preclear_ddl_audit`: an event trigger that logs every `DROP TABLE` into `test_schema.rv_preclear_ddl_log`. The clear script drops event triggers before anything else. Otherwise the trigger would fail the drop of its own log table.
- The rest of the fixture covers the other orderings the clear command reads from `pg_depend`:
  - domain CHECKs calling functions (`pg19_domains_*`) and domains over domains;
  - `INHERITS` children listed after their parent (`wp19_tables_gen_inh_*`);
  - rules reading views (`pg19_rules_reads_tag_view`);
  - casts, operators and event triggers on fixture functions;
  - on PG19, the foreign-data wrappers, servers, user mappings and `SERVER` subscriptions that depend on the `postgres_fdw` in `public`.
- The CI step `assert-pre-clear` checks all of this:
  - it loads each file into a scratch database of its own;
  - a clear of `test_schema` alone must not drop any foreign-data wrapper, server, user mapping or subscription, since they all hang off the unselected `public`;
  - it then runs `pgc --command clear` over the six fixture schemas with `--use-single-transaction` and applies the script with `ON_ERROR_STOP=1`;
  - it fails on any error, on a cycle pgc cannot break, on `CASCADE`, or on a schema left behind.

### 73. What Goes with a Generated Column a Routine's DROP ... CASCADE Takes, Publication Rows and Partition Objects around a Retype, PostgreSQL 14 Generated Columns, and What Blocks an In-Place Retype (all versions; row filters PG15+, generated column lists PG18+)
All objects are in `test_schema`, prefixed `rv_r3g_`, at the end of the `pg19:tables` block. The publications (`rv_r3g_pub_*`) need row filters (PG15+), and `rv_r3g_pub_cols` / `rv_r3g_pub_rt_cols` need `publish_generated_columns` (PG18+). On older majors those publications fail harmlessly on both sides. The role `pgc_pg19_r3g_reader` is created idempotently.

**A routine's DROP ... CASCADE takes a stored generated column**
- **Modified**: `rv_r3g_fn(x integer)` returns `integer` in FROM and `bigint` in TO. `rv_r3g_items.g`, `rv_r3g_newdep.g` and the partitioned `rv_r3g_part.g` call it and become `bigint`.
  - The routine phase drops the function with `CASCADE`, which takes the columns, and re-adds them.
  - The table phase no longer runs `ALTER COLUMN g TYPE` first. That was redundant everywhere, and PostgreSQL 14 refused it ("found unexpected dependency for column").
- The CASCADE also takes the following, and the migration now puts them back:
  - the views `rv_r3g_items_v`, `rv_r3g_items_vv` (over the first) and `rv_r3g_part_1_v`, dropped ahead and re-created after the re-add;
  - the materialized view `rv_r3g_items_mv`, with its statistics `rv_r3g_items_mv_st`;
  - the SQL-standard `rv_r3g_items_sum()` and `rv_r3g_items_max()`, and the string-body `rv_r3g_items_top()`, whose return type changes, all reading `g`;
  - the column privileges of `pgc_pg19_r3g_reader`;
  - the `UPDATE OF g` trigger `rv_r3g_items_upd` and the `WHEN` trigger `rv_r3g_items_when`;
  - the publication rows of `rv_r3g_pub_filter` (row filter), `rv_r3g_pub_cols` (column list, PG18+) and `rv_r3g_pub_part`;
  - on the partition `rv_r3g_part_1`: its own index, policy, trigger and column privileges.
- **What calls a routine that waits for the re-add**: `rv_r3g_use` has a default and a CHECK calling `rv_r3g_items_max()`, and the policy `rv_r3g_use_pol` calling it. `rv_r3g_use2` has a CHECK calling `rv_r3g_items_top()`.
  - These come back only after those routines are re-created, in the block "Recreate dependents dropped by CASCADE that call the routines created above".
  - Before, the migration failed with "function does not exist", or `rv_r3g_items_top`'s own `DROP ... CASCADE` silently took the CHECK again.
- **What only TO has on the re-added column**: on `rv_r3g_newdep.g`, TO adds an index, a CHECK, a UNIQUE key that the new `rv_r3g_newdep_ref` references, and a RESTRICTIVE policy.
  - The table phase creates them, the CASCADE takes them again, and they now come back with the re-add, found through the TO dump's column edges.
- **Held-back routines**: TO adds the operator `test_schema.#@@#` and the SQL-standard `rv_r3g_uses_op()` that uses it. This holds the routines, and with them the re-created `rv_r3g_items_mv`, until after the operators.
  - The materialized-view statistics now follow the view there. Before, they were created before the view existed.
- **Modified**: `rv_r3g_ren_fn` renames its parameter, so its column is dropped and re-added with the same type. Its view, privileges and trigger come back.
- **Modified**: `rv_r3g_cfg.n` changes from `integer` to `bigint`. The SQL-standard `rv_r3g_k` reads it, so `drop_views` drops it ahead of the tables, and that CASCADE takes `rv_r3g_kt.g`. The column's view, privileges, trigger and publication row come back.
- **Modified**: `rv_r3g_c16` retypes `a` in the same migration as its routine's return type change. `g` is re-added twice, and the view is re-created after both.

**Publication rows around a retype**
- **Modified**: in `rv_r3g_pubrt` and the partition `rv_r3g_pubrtp_1`, the generated column is dropped and re-added because TO retypes `a`, which it reads.
  - The members of `rv_r3g_pub_rt_filter`, `rv_r3g_pub_rt_cols` (PG18+) and `rv_r3g_pub_rtp` are released before the drop and restored after the re-add.
  - Before, the migration failed on every run.

**A partition's own objects around a retype**
- **Modified**: `rv_r3g_pdep` (`a` → `bigint`) re-creates `g` around the retype. The partition `rv_r3g_pdep_1` has its own index, CHECK, UNIQUE (referenced by `rv_r3g_pdep_ref`), statistics and RESTRICTIVE policy on `g`. These are restored after the alter.

**PostgreSQL 14: generated columns over a function or operator**
- **Modified**: `rv_r3g_pg14` retypes `g` (calls `rv_r3g_keep_fn`), `o` (uses `#@#`) and `b` (`abs(v)`) to `bigint`.
  - PG14 refuses `ALTER COLUMN ... TYPE` on the first two, so they are dropped and re-added (flag `generation_blocks_retype` in the PG14 dump).
  - `b` is retyped in place. PG15+ retypes all three in place.
- **Modified**: `rv_r3g_plain` makes `g` (calls `rv_r3g_keep_fn`) a plain `bigint`, and `h` (calls `rv_r3g_gone_fn`, which TO drops) a plain `integer`. The partitioned `rv_r3g_plainp` makes its `g` a plain `bigint`.
  - PG14 keeps such a column depending on the function after `DROP EXPRESSION`, so a type change still failed, and the function's `DROP ... CASCADE` took `h` with its data.
  - On PG14 these columns are now rewritten, keeping their values: a `"pgc_rewrite_<col>"` column is filled with `ALTER COLUMN ... TYPE ... USING`, the old column is dropped, and the new one renamed.
  - Their index, view, privileges and comment come back.
  - PG15+ uses `ALTER COLUMN ... TYPE` / `DROP EXPRESSION`.

**Policies and triggers in the way of an in-place retype**
- **Modified**: these columns are retyped in place, and each is used by a policy or trigger:
  - `rv_r3g_rt_docs.tenant_id`, used by the policy `rv_r3g_rt_docs_pol`;
  - `rv_r3g_rt_orders.status`, used by an `UPDATE OF` trigger;
  - `rv_r3g_rt_ev.kind`, used by a `WHEN` trigger;
  - the partitioned `rv_r3g_rt_part.tenant_id`, used by policies and triggers on the parent and on its partition.
  They are dropped ahead of the table alters and put back afterwards.
- **Modified / Added**: `rv_r3g_rt_a_reader` changes its policy, which reads `rv_r3g_rt_docs.tenant_id` through a subquery. The new `rv_r3g_rt_a_new` has such a policy.
  - Both tables are processed before `rv_r3g_rt_docs`, so their policies used to be created before the retype, which then failed.
  - They now wait for the retype.

**CI assertion**: `.github/scripts/assert-r3-gencol.sh` compares a with b after round 1:
- column types and generation expressions, defaults, views and their privileges, routines, column privileges, row security, policies, triggers (clones included, with enabled state), indexes, constraints and statistics;
- publication rows by column name and row filter (PG15+);
- counts of the objects present on both sides;
- the recomputed and rewritten values: `rv_r3g_plain` must keep `1:5:15:25`, with no `pg_depend` row left on a routine;
- in default mode, the order of the round-1 statements (`TEXT_CHECKS=0` skips this for production output).

### 74. Catalog Names in Emitted DDL: Collations, Event Triggers, Primary-Key Columns, Foreign Keys Next to a Dropped Table, New Collations and Retypes (all versions)
All objects are named `rv_r3d_*` and sit in the `pg19:misc` block of both schema files. They apply on PG14+.

pgc writes catalog names into DDL. Before 1.1.0 several places wrote them wrongly:
- A column's `COLLATE` clause took the raw collation name, without its schema and without doubling a `"`. A name holding a `"` ran the rest of itself as SQL when the migration was applied.
- Event trigger names were quoted a second time.
- The inline `PRIMARY KEY` of `CREATE TABLE` was cut from the constraint definition at the first `)` and split at every `,`.
- The foreign keys dropped ahead of a `DROP TABLE` were picked by substring.

Collations (the same on both sides):
- `test_schema.rv_r3d_ci` (POSIX).
- `public.rv_r3d_ci` (C): the same name with another locale. This is what an unqualified `collate "rv_r3d_ci"` bound to.
- `test_schema."rv_r3d_ci""; create table public.rv_r3d_pwned(x int); --"` (C).

Collation changes:
- **Modified**: `test_schema.rv_r3d_coll_alter`.
  - `c_retag` changes from `"C"` to `test_schema.rv_r3d_ci`. This is a collation-only change, which never reached the table hash and was never migrated.
  - `c_reschema` changes from `public.rv_r3d_ci` to `test_schema.rv_r3d_ci`; only the schema differs.
  - `c_kept` stays `"POSIX"`.
  - TO adds `c_added`, collated with the hostile collation.
  - Expected:
    - `alter column c_retag type text collate test_schema.rv_r3d_ci`;
    - the same for `c_reschema`;
    - `add column c_added text collate test_schema."rv_r3d_ci""; create table public.rv_r3d_pwned(x int); --"`.
- **Added**: `test_schema.rv_r3d_collated`. Its `CREATE TABLE` carries the hostile, the schema-qualified and the `"C"` clauses. A `pg_catalog` collation keeps its old spelling (`collate "C"`).
- **Added** (TO only): the collation `test_schema.rv_r3d_new_ci`, used by:
  - `rv_r3d_to_new_coll.c`, which moves to it from `"C"`;
  - the new table `rv_r3d_new_coll_tbl`;
  - the new domain `rv_r3d_new_dom`.
  - Expected: `CREATE COLLATION test_schema.rv_r3d_new_ci` in the "New Collations (before types and tables)" section, ahead of the domain, the table and `alter column c type text collate test_schema.rv_r3d_new_ci`.
  - Before, it was created after the tables and every one of those statements failed with "does not exist".

Event triggers, on `test_schema.rv_r3d_evt_fn()`:
- **Modified**: `"rv_r3d_Keep Trg"` is disabled and commented in TO.
  - Expected: `alter event trigger "rv_r3d_Keep Trg" disable;` and `comment on event trigger "rv_r3d_Keep Trg" is 'r3-ddl kept';`.
  - Before, the name came out as `"""rv_r3d_Keep Trg"""` and both statements failed.
- **Removed**: `"rv_r3d_From ""Only"""`.
- **Added**: `"rv_r3d_To ""Only"""` (`ddl_command_end`, `WHEN TAG IN ('CREATE TABLE')`).

Primary keys (TO only):
- **Added**: `test_schema.rv_r3d_pk_hostile`, keyed on `"Code (x)", "a,b", "select", "say ""hi"")"`.
  - Expected: `constraint rv_r3d_pk_hostile_pkey primary key ("Code (x)", "a,b", "select", "say ""hi"")")`.
  - Before, it was `primary key ("Code (x")` and the table was never created.
- **Added**: `test_schema.rv_r3d_pk_include`, whose key has `INCLUDE (x) DEFERRABLE INITIALLY DEFERRED`.
  - Both now follow the key list in `CREATE TABLE`.
  - Before, they were left out and round 2 dropped and re-added the key.

Foreign keys next to a dropped table:
- **Removed**: `rv_r3d_account`, whose name is a prefix of `rv_r3d_accounts`. The unchanged `rv_r3d_payments_account_fk` references `rv_r3d_accounts`.
  - Expected: that foreign key is not touched.
  - Before, it was dropped ahead of the `DROP TABLE` and never added back.
- **Removed**: `rv_r3d_log`, whose name appears in the column `rv_r3d_log_catalog_id` of `rv_r3d_products_catalog_fk`. TO changes that key (it loses `ON DELETE CASCADE`).
  - Expected: one drop, in the table alter, and one add.
  - Before, it was dropped twice and the second drop failed with "does not exist".
- **Removed**: `rv_r3d_legacy_ref`, together with `rv_r3d_orders_legacy_fk`, which references it and which TO drops. Expected: dropped once, ahead of the table.
- **Removed**: the partition `rv_r3d_parted_2` of `rv_r3d_parted`, which `rv_r3d_parted_ref_fk` references.
  - PostgreSQL copies that key onto every partition, so it is dropped ahead of the partition and added back in the Foreign Keys section.
  - Before, it was dropped and lost.

A retype in the way of policies and triggers:
- **Modified**: `test_schema.rv_r3d_retype`.
  - `owner` and `status` change collation only (`"C"` to `"POSIX"`); `code` changes from `varchar(10)` to `varchar(20)`.
  - Its policies read the columns: `rv_r3d_retype_owner` is kept; `rv_r3d_retype_code` changes; `rv_r3d_retype_gone` is removed.
  - So do its triggers: `rv_r3d_retype_when` (`WHEN (old.status IS DISTINCT FROM new.status)`) and `rv_r3d_retype_upd_of` (`UPDATE OF owner`).
  - So does the policy `rv_r3d_retype_ref_pol` of `rv_r3d_retype_ref`, whose subquery reads `rv_r3d_retype.code`.
  - PostgreSQL refuses `ALTER COLUMN ... TYPE` while any of them is there ("cannot alter type of a column used in a policy definition" / "... in a trigger definition").
  - Expected:
    - they are dropped in the "Dependents dropped ahead of a column retype" section ahead of every table alter;
    - the columns are retyped;
    - the TO policies are created in "Recreate policies dropped ahead of a column retype", and the triggers with the other triggers.
    - `rv_r3d_retype_gone` is dropped once and not recreated.
  - Before, the ALTERs failed on every run.
- **Modified**: `test_schema.rv_r3d_pkey_coll`, partitioned by `LIST (c)`, whose partition-key column `c` changes collation only.
  - PostgreSQL retypes no partition-key column, so the table is dropped and recreated with its partition `rv_r3d_pkey_coll_a`, as for a type change there.
  - The note says "collation change requires table recreation".

**CI assertion** (`assert-r3-ddl.sh`): after round 1 the step checks that:
- `public.rv_r3d_pwned` does not exist in `a`;
- the schema-qualified collation of every column of every `rv_r3d_*` table matches `b`, as do the column types and the collation of the `rv_r3d_*` domains;
- the name, event, state, tags and comment of every `rv_r3d` event trigger match `b`;
- every primary-key and foreign-key definition, policy and trigger of the `rv_r3d_*` tables matches `b`;
- in the round-1 script:
  - `rv_r3d_payments_account_fk` is never dropped;
  - `rv_r3d_products_catalog_fk` is dropped exactly once;
  - no event trigger name is written `"""`;
  - `test_schema.rv_r3d_new_ci` is created before `rv_r3d_to_new_coll.c` moves to it;
  - `rv_r3d_retype_when` is dropped before `rv_r3d_retype.status` is retyped.

---

### 75. Composite Type Attributes, Typed Tables, Sequence Data Types and Owners, Multirange Schemas (all versions)
All objects are named `rv_r3t_*` and sit in `test_schema`, in the `pg19:misc` block of both schema files. Every version from PG14 runs all of it.
- **Modified**: `shared_schema.address_type` (adds `region`) and `test_schema.user_profile` (adds `phone`), the composite cases of section 3.
  - They reached only an `-- Alter script not implemented for type ... (c -> c)` comment, which `USE_COMMENTS=false` strips. Round 1 was empty, round 2 looked converged, and `a` never got the attribute.
  - They are now `alter type ... add attribute ...`.
- **Modified**: `rv_r3t_comp` goes from `(a integer, b integer, c text, gone text)` to `(a bigint, bb integer, c text, added text)`.
  - `a` is retyped with `alter attribute "a" set data type bigint`, `gone` is dropped and `added` is added.
  - Nothing tells the rename `b` → `bb` apart from a drop and an add. `ADD ATTRIBUTE` appends, so that alone would leave `(a, c, bb, added)`; `c` is dropped and re-added after `bb` to reach b's order.
  - That happens only because nothing stores the type (no typed table, no column). Otherwise the order is left alone, with a note and a stderr warning.
- **Modified**: `rv_r3t_used` gets `z` while `rv_r3t_used_tbl.v` stores a value of it. `ADD ATTRIBUTE` works then, and the row keeps its value.
  - A retype would not work. PostgreSQL refuses `ALTER ATTRIBUTE ... TYPE` while a table or materialized-view column stores the type, directly or through an array, a domain or another composite type.
  - The dump records those columns (`composite_stored_in`). Such a retype is left out, commented, with a note and a stderr warning.
- **Modified**: `rv_r3t_typed_t`, the type of the typed table `rv_r3t_typed`, which holds one row: `id` becomes `bigint`, `dropme` is dropped and `extra` is added, each `... cascade`.
  - PostgreSQL refuses all three statements on a type with typed tables unless they say `CASCADE`.
  - The table diff used to emit `alter table ... add column` / `drop column` / `alter column ... type` for the typed table, which PostgreSQL refuses ("cannot add column to typed table").
  - It now sets only what the table adds itself: `extra`'s `DEFAULT 7`, then `update ... set extra = default where extra is null`, then `NOT NULL`. `ADD ATTRIBUTE` leaves the new column NULL in every row, so without the fill the `SET NOT NULL` failed on every run ("contains null values"). An ordinary table's `ADD COLUMN ... DEFAULT` fills the rows the same way.
  - Typed tables are recorded in the dump (`typed_tables`). For a pgc 1.0.x dump they are found among its tables.
  - The `DROP ATTRIBUTE ... CASCADE` gets a note and a stderr warning, because CASCADE also drops every object depending on the dropped column, including ones outside the compared schemas.
- **Modified**: `rv_r3t_tv_t` goes from `(id integer, name text)` to `(id bigint, name text)`. Its typed table `rv_r3t_tv` holds a row and is read by the view `rv_r3t_tv_v`.
  - The composite alterations used to run in the types section, before "Views DROP", so PostgreSQL refused the retype ("cannot alter type of a column used by a view or rule"). The view was then re-created on the old type, every run.
  - They now run in a section of their own, after the view drops and publication releases and before the tables, as an ordinary table's `ALTER COLUMN ... TYPE` does.
- **Modified**: sequence data types.
  - The cases: `rv_r3t_ser` (`serial` → `bigserial`, with a row), `rv_r3t_bigser` (`bigserial` → `serial`), `rv_r3t_seq` (`AS integer` → `AS bigint`) and `rv_r3t_ident` (identity `integer` → `bigint`).
  - `ALTER SEQUENCE` had no `AS` clause. The widening failed on every run (`MAXVALUE ... is out of range for sequence data type integer`), and the narrowing silently kept `bigint`.
  - It now starts with `as <type>`, before the bounds, and runs before the column's retype. The sequence keeps its position.
- **Modified**: `rv_r3t_own_ser` (serial) and `rv_r3t_own_ident` (identity) are owned by `pgc_pg19_r3t_owner` in b.
  - Before: `alter sequence ... owner to` came before the table's `alter table ... owner to`, and PostgreSQL refuses a direct owner change of a sequence linked to a table ("cannot change owner of sequence ... linked to table"). With `--use-single-transaction` or `ON_ERROR_STOP=1` the migration rolled back on every run.
  - Now the sequence's owner statement is left out, and the table's carries it.
- **Modified**: `rv_r3t_mv_seq` is `OWNED BY rv_r3t_mv_a.id` in a, and in b is owned by `pgc_pg19_r3t_owner`, `OWNED BY rv_r3t_mv_b.id`, with `rv_r3t_mv_b` owned by that role.
  - Neither statement works directly: a direct `OWNER TO` is refused while the sequence is linked to `rv_r3t_mv_a`, and `OWNED BY rv_r3t_mv_b` is refused while the owners differ.
  - It is unlinked first (`... owned by none`), takes its owner, and is linked to `rv_r3t_mv_b.id` after the tables. A sequence that becomes `OWNED BY` a column while its owner changes is linked after the tables the same way.
- **Modified**: `rv_r3t_mis`, a range whose multirange `a` has in `public` (`multirange_type_name = public.rv_r3t_mis_multirange`). That is where pgc before 1.1.0 created every multirange. It is moved with `alter type public.rv_r3t_mis_multirange set schema test_schema;`, without recreating the range (which would drop every column of it).
- **Added**: `rv_r3t_new_rng`, and `rv_r3t_mr_use` with a column of `test_schema.rv_r3t_new_rng_multirange`.
  - `CREATE TYPE ... AS RANGE` wrote `multirange_type_name = <name>` unqualified. PostgreSQL resolves that in the creation schema of `search_path`, not the range's schema, so the multirange landed in `public` and the table failed ("type ... does not exist") on every run.
  - It is now written schema-qualified, as the target has it. `test_schema.int_range` and `float_range` now get their multiranges in `test_schema` too.
- **Not covered by the round trip** (covered by unit tests):
  - `SET <list GUC> TO NULL` in a PostgreSQL 19 TO against a pre-19 FROM. CI loads both schemas on one major. Such a server stores the `SET ... = ''` fallback as `""`, so the routine is compared in that form, with a note and a stderr warning, instead of being re-created on every compare. Also checked live with a PG18 → PG19 run.
  - A typed table whose type lies outside the compared schemas: note plus stderr warning.
  - A domain collation change: note plus stderr warning.
- **CI assertion** (`.github/scripts/assert-r3-types.sh`): after round 1, `a` must match `b` in:
  - the attributes (name, type, order) of every composite type;
  - the columns and the row of `rv_r3t_typed`;
  - the column types of `rv_r3t_tv` and its view;
  - every sequence's type, bounds, owner and `OWNED BY` column, and `rv_r3t_ser`'s position;
  - the owners of the `rv_r3t_own_*` tables and their sequences;
  - the schema of every range's multirange.

  The round-1 SQL must move `public.rv_r3t_mis_multirange` and must not change a linked sequence's owner directly. `b` must hold the expected attribute lists, sequence types, typed-table row and sequence link, so the check is not vacuous.

### 76. CHECK Copies a Table Only Inherits, ATTACH / DETACH PARTITION and INHERIT / NO INHERIT with the Parents' CHECKs, NOT NULL Name Drift, and Wrapper / Dictionary Template Changes (all versions; NOT NULL constraints PG18+, CHECK enforceability PG18+ / in place PG19+)
All objects are in `test_schema` and prefixed `rv_r3c_`. The wrappers and the server are named `rv_r3c_*`, and the extra role is `pgc_pg19_r3c_reader`. The tables are in the `pg19:tables` block, the server and its foreign tables in `pg19:subscriptions-fdw`, and the dictionary in `pg19:misc`. The NOT ENFORCED CHECKs are added by separate `ALTER TABLE ... ADD CONSTRAINT` statements, and the enforced copies are set by `ALTER TABLE ONLY ... ENFORCED`. On PG14–17 only those statements fail, on both sides, and on PG18 only the `ONLY ... ENFORCED` ones do.

**CHECK copies a table only inherits** (PG19)
- **Modified**: `rv_r3c_ie_p` goes from enforced to NOT ENFORCED. Its `INHERITS` child `rv_r3c_ie_c` keeps an ENFORCED copy in TO. The parent's in-place `ALTER CONSTRAINT ... NOT ENFORCED` recurses, and `alter table test_schema.rv_r3c_ie_c alter constraint rv_r3c_ie_p_ck enforced` follows it in the "Enforceability of inherited CHECK constraint copies" section, after every table alter.
- **Modified**: `rv_r3c_ie_pt` is NOT ENFORCED on both sides. Partition `_1` gets an ENFORCED copy in TO. `_2` has one in FROM only and gets `... not enforced`. `_3` is TO-only, and is created and then made ENFORCED.
- **Modified**: `rv_r3c_ie3_c1` keeps an ENFORCED copy of `rv_r3c_ie3_p`'s NOT ENFORCED CHECK in FROM, and follows the parent in TO. Its `... not enforced` recurses into `rv_r3c_ie3_c2`'s merged copy, which TO keeps ENFORCED, so `alter table test_schema.rv_r3c_ie3_c2 alter constraint rv_r3c_ie3_ck enforced` follows it.
- On PG18 a copy that has to change is a comment and a stderr warning, because PG18 cannot alter a CHECK's enforceability.

**ATTACH / DETACH PARTITION** (all versions)
- **Modified**: `rv_r3c_at_same`, `rv_r3c_at_miss`, `rv_r3c_at_diff` and `rv_r3c_at_like` become partitions of `rv_r3c_at`. They have, in order:
  - the parent's CHECK;
  - no copy;
  - a copy with another definition;
  - a copy from `LIKE ... INCLUDING CONSTRAINTS`, plus on PG18+ the parent-named NOT NULL.
  The missing CHECK is added and the different one is dropped and re-added before the attach. A merged CHECK or NOT NULL is never dropped.
- **Modified**: `rv_r3c_atn_x` (PG18+) gets `ALTER CONSTRAINT ... NOT ENFORCED` (PG19), or a drop and re-add (PG18), before it is attached under `rv_r3c_atn`'s NOT ENFORCED CHECK.
- **Modified**: `rv_r3c_atc_x` lacks the NOT NULL of the parent's key column. `rv_r3c_atc_y` has an extra column and lacks one of the parent's. The partitioned `rv_r3c_atc_sub` lacks it too. The `SET NOT NULL`, `ADD COLUMN` and `DROP COLUMN` all run before each `ATTACH PARTITION`. `rv_r3c_atc_sub_1`'s TO index and CHECK on the new column are created after its parent's alter.
- **Modified**: `rv_r3c_mv_x` moves from `rv_r3c_mv_op` to `rv_r3c_mv_np`. The detach leaves the old parent's CHECKs behind as local constraints: `rv_r3c_mv_ck` merges into the new parent's, and `rv_r3c_mv_old` is dropped before the attach. `rv_r3c_mv_dt` is detached for good, and its leftover copies are dropped in the same run.

**INHERIT / NO INHERIT** (all versions)
The parent is `rv_r3c_inh_p` (CHECK `rv_r3c_inh_p_ck`), under `rv_r3c_inh_g` (CHECK `rv_r3c_inh_g_ck`, NOT NULL `id`). `rv_r3c_inh_s` only inherits both CHECKs.
- **Modified**: `rv_r3c_inh_merge`, standalone with both CHECKs, joins `rv_r3c_inh_p` and keeps them as merged copies. The result is `alter table ... inherit ...`, with no drop.
- **Modified**: `rv_r3c_inh_join`, standalone without the NOT NULL, the column `b` or the CHECKs, joins the parent and only inherits them. The migration:
  1. runs `SET NOT NULL` and `ADD COLUMN b`;
  2. has the holders `rv_r3c_inh_g` and `rv_r3c_inh_p` drop their CHECKs, parents first;
  3. runs `INHERIT`;
  4. re-adds the CHECKs, children first, so the table's copies are inherited only, as in TO.
- **Modified**: `rv_r3c_inh_keep` and `rv_r3c_inh_drop` leave the parent (`NO INHERIT`). The copies left behind are kept by `_keep` and dropped by `_drop`, along with its NOT NULL.
- **Modified**: `rv_r3c_inh_leave` has a merged copy of `rv_r3c_inh_p_ck`. That copy is dropped on its own after the `NO INHERIT`, with no drop and re-add across the family.
- **Modified**: `rv_r3c_inh_r` goes from `INHERITS (q1, q2)` to `(q2, q1)`. Both parents are left and joined again in TO order.

**NOT NULL name drift** (PG18+)
- **Modified**: `rv_r3c_nn_col` (column renamed), `rv_r3c_nn_tab` (table renamed, primary key), `rv_r3c_nn_ident` (identity column renamed) and `rv_r3c_nn_custom` stay NOT NULL under PostgreSQL's own names in TO. Nothing is emitted: NOT NULL constraints are paired by column.
- **Modified**: `rv_r3c_nn_named` is renamed to the name TO chose.
- **Modified**: `rv_r3c_nn_move` moves the name `rv_r3c_nn_move_x` from column `a` to `b`. `rv_r3c_nn_swap` swaps `_p` and `_q` between its columns. The names are renamed away first, and the swap goes through a temporary `pgc_nn_<hash>` name. Every rename is a `DO` block guarded on the constraint's current name and column, and none is dropped.

**Foreign server that changes its wrapper**
- **Modified**: `rv_r3c_srv` moves from `rv_r3c_fdw_a` to `rv_r3c_fdw_b`. It has:
  - a comment, a PUBLIC user mapping, and `GRANT USAGE` for `pgc_pg19_r3c_reader`;
  - the foreign table `rv_r3c_ft`, with a comment, `SELECT` and a column `UPDATE (label)` grant, and the view `rv_r3c_ft_view` over it;
  - the foreign table `rv_r3c_fp_remote`, a partition of the local `rv_r3c_fp`.
  The views and foreign tables are dropped first, then the user mapping, and then the server without `CASCADE`. The server comes back with its user mapping and foreign tables (owner, comment, and `ATTACH PARTITION` for the partition), then the view, and then the table, column and server grants.

**Text search dictionary that changes its template**
- **Modified**: `rv_r3c_dict` moves from `simple` to `snowball` and gets a new comment. Before the tables, one `DO` block:
  1. creates `pgc_new_<hash>`;
  2. runs `ALTER TEXT SEARCH CONFIGURATION rv_r3c_tscfg ALTER MAPPING REPLACE`;
  3. drops the old dictionary without `CASCADE`;
  4. renames the new one, then sets owner and comment.
  The generated column `rv_r3c_ts_docs.body_tsv` and the index `rv_r3c_ts_docs_fts` stay. A failing drop rolls the whole block back.

**CI assertion**: `.github/scripts/assert-r3-constraints.sh` compares a and b after round 1:
- the CHECK constraints of every `rv_r3c_` relation, inherited copies included, with `conislocal`, `coninhcount`, enforceability (PG18+), validation and definition;
- the partitions and `INHERITS` parents with `inhseqno` and bounds, foreign partitions included;
- the attached and inheriting rows;
- the `rv_r3c_atc` columns and `rv_r3c_atc_sub_1`'s indexes;
- which columns are NOT NULL, the NOT NULL constraint of each column (PG18+), and the names TO chose, moved or swapped;
- that `rv_r3c_ie3_c2` stays enforced (PG19);
- the server's wrapper, options, comment and `srvacl`, its user mapping, the foreign tables' options, comments, ACLs, partition flag and column ACLs, and the view;
- the dictionary, with no `pgc_new_` left behind, the configuration's mappings, the generated column, the index and the stored tsvector;
- (skipped with `TEXT_CHECKS=0`) that round 1 has no `CASCADE` on the server or the dictionary, and no `DROP CONSTRAINT` of a merged CHECK or a drifted, moved or swapped NOT NULL.

### 77. Empty Dumps and retain_dead_tuples on Enabled Subscriptions (all versions; the subscription half PG19+)

No fixture covers this section, for two reasons:
- An empty dump comes from the `--scheme` pattern, not from the schema. The CI round trip always dumps the fixture schemas, so no `schema_a.sql`/`schema_b.sql` object can make either side empty.
- Changing `retain_dead_tuples` on an enabled subscription needs a running apply worker and a live slot. Section 45 keeps those out of the fixtures on purpose, and a slot would also keep `a` and `b` from being dropped between runs.

Both cases are covered by:
- a dedicated CI step;
- unit tests in `dump/tests/core.rs`, `dump/tests/publication.rs` and `comparer/tests/core/subscriptions_fdw.rs` (`// ==== r3-compat tests ====`);
- integration tests in `app/tests/compare_pipeline.rs` and `app/tests/dump_file_format.rs`;
- `#[ignore]`d live tests in `app/tests/live_database.rs`.

**Empty dumps**: a dump whose pattern matches no accessible schema. This happens when the pattern matches nothing (the FROM of a first deployment into a fresh database, or a TO the schema is gone from) or when every matched schema lacks `USAGE`.
- It now records the server's `server_version_num`, its installable extensions and its `standard_conforming_strings` default.
- It also holds the database-wide objects that a dump of any schema of the same database holds: event triggers, publications, subscriptions, foreign-data wrappers, servers, user mappings, extensions installed in `public`, and default privileges not bound to a schema.
- It used to hold none of these, so it read as a dump written by pgc 1.0.x:
  - as TO, the compare was refused (non-zero exit, no script), even inside the `--config` chain. pgc 1.0.x did compare it, and dropped every database-wide object FROM held, the ones TO held too included. A subscription's replication slot on the publisher went with it.
  - as FROM, it got the 1.0.x warning, `set standard_conforming_strings = on;` and the pre-PostgreSQL 19 fallbacks. On PG19, `SET search_path TO NULL` was written `= ''`, which stores `""`, and the next compare re-created the routine. The migration also re-created the database-wide objects the target already held, and failed on the first one.
- Now an empty TO drops only what the schema held, and an empty FROM gives a plain create migration that converges in one round on every version.
- The clear command still reads nothing for such a pattern, so its script drops nothing.

**retain_dead_tuples on an enabled subscription (PG19+)**: PostgreSQL accepts the change only once every worker of the subscription has exited. `SET PUBLICATION`, a `CONNECTION` or `SERVER` change and `OWNER TO` each restart a running worker, and the launcher starts the new one at once. So a `DISABLE` + `SET` right after one of them met the restarted worker ("logical replication worker is still running") in almost every run, and left the subscription disabled under `ON_ERROR_STOP=1`. The migration now does this:
- `ALTER SUBSCRIPTION ... DISABLE;` comes first.
- A `DO` block waits, for at most 60 s, until `pg_stat_subscription` shows no worker of that subscription in the current database.
- Every other change runs while the subscription is disabled:
  - turning it off still precedes the source change, and turning it on still follows it;
  - `SET PUBLICATION` is written `WITH (refresh = false)`.
- Then comes `ENABLE`, then `REFRESH PUBLICATION`.
- A target that ends disabled with a changed publication list is enabled for the refresh and disabled again.
- With `--use-single-transaction` the wait is left out (the `DISABLE` cannot take effect before `commit;`), and pgc warns on stderr as before.

**CI assertion**: a standalone step.
- On PG14–19 it builds two databases that hold the same database-wide objects: an event trigger, a publication, a subscription with `connect = false`, an FDW, a server, a user mapping and `pgcrypto` in `public`. Only one of them also has the dumped schema. It then checks:
  - `pgc --command dump` of the other one records the server's version and extensions, and the same count of each database-wide kind as a dump of the schema;
  - a compare against it as TO succeeds and drops the table, and nothing database-wide;
  - a compare from it as FROM has no 1.0.x warning, no guard and no DDL on database-wide objects, and on PG19 keeps `SET search_path TO NULL`;
  - the migration applies with `ON_ERROR_STOP=1`, `proconfig` matches TO on PG19, and a second compare is empty.
- On PG19 it creates a publisher database and pre-created slots in the same cluster. Then, three times for each case (publication change with rdt on, connection change with rdt on, rdt off with a publication change and a disabled target), it checks:
  - the migration waits for the worker and applies with `ON_ERROR_STOP=1`;
  - `pg_subscription` and `pg_subscription_rel` of `a` then match `b`;
  - a second compare is empty.

### 78. Typed Tables: What a Composite Type's Retype Reaches, Attribute Collations, and CREATE TABLE ... OF (all versions)
All objects are named `rv_r4t_*`, in `test_schema` except the one type in `rv_r4t_outside`. They sit in the `pg19:misc` block of both schema files, and every version from PG14 runs all of it; the publication `rv_r4t_xpub` needs PG15+.
- **Modified**: `rv_r4t_pt` (`amount integer` → `bigint`), the type of the partitioned typed table `rv_r4t_tp`, whose partition `rv_r4t_tp1` has the policy `rv_r4t_tp1_pol` and the `BEFORE UPDATE OF amount` trigger `rv_r4t_tp1_trg`.
  - `ALTER TYPE ... ALTER ATTRIBUTE ... TYPE ... CASCADE` retypes a typed table's partitions too.
  - PostgreSQL used to refuse it ("cannot alter type of a column used in a policy definition"): only the typed table's own blockers were dropped ahead of it.
  - They are now dropped right before the composite section, and created from TO once, after every table alter.
- **Modified**: `rv_r4t_it` (`amount` → `bigint`), the type of `rv_r4t_tt`, whose `INHERITS` child `rv_r4t_ti` has:
  - the policy `rv_r4t_ti_pol` on `amount`;
  - a stored generated column `g` (`amount * 2`, `integer` → `bigint`), with the policy `rv_r4t_ti_gpol` on it.

  `rv_r4t_other_pol` reads `rv_r4t_tt.amount` through a subquery.
  - The retype recurses to the child. `rv_r4t_ti_gpol`, the other retype blockers and `alter table ... rv_r4t_ti drop column g` now run before the `ALTER TYPE`.
  - The child's alter adds `g` back.
- **Modified**: `rv_r4t_qt` (`"order"` and `"Amt"`: `integer` → `bigint`), the type of `rv_r4t_qtt`, which has the policy `rv_r4t_qtt_pol` on `"order"`. Its `INHERITS` child `rv_r4t_qti` has a generated column `g` over `"Amt"`.
  - An attribute is named as the catalog spells it, but a table column in `quote_ident` form, so an attribute that needs quoting used to reach no typed table.
  - Its policy was then not dropped ahead of the retype, nor its descendant's generated column, and the `ALTER TYPE` failed.
- **Dropped**: the typed table `rv_r4t_gone`, with a policy and a trigger on `amount`, while its type `rv_r4t_dt` retypes `amount`.
  - The type's `CASCADE` still reaches it, since the table phase only drops it later.
  - Its blockers go first, and nothing is put back on it.
- **Dropped**: `rv_r4t_3c1` (a generated `g` over `amount`, `INHERITS` the typed `rv_r4t_3tt`) and `rv_r4t_3c2` (`INHERITS rv_r4t_3c1`), while `rv_r4t_3t` retypes `amount`.
  - `rv_r4t_3c1` loses `g` ahead of the retype, and its `DROP COLUMN` takes `rv_r4t_3c2`'s inherited copy along.
  - `rv_r4t_3c2`'s own drop says `IF EXISTS`. It used to fail: "column g of relation rv_r4t_3c2 does not exist".
- **Modified / dropped**: `rv_r4t_5c1` keeps `g` as a plain `bigint` column, and `rv_r4t_5c2`, its child, is dropped.
  - `rv_r4t_5c1`'s `DROP EXPRESSION` reaches `rv_r4t_5c2`'s copy, so nothing is emitted for that copy.
  - It used to be dropped on its own, which fails: "cannot drop inherited column".
- **Modified**: `rv_r4t_lti` leaves the typed table `rv_r4t_ltt`'s family (`NO INHERIT`) and retypes `amount` itself (`integer` → `bigint`), under its policy `rv_r4t_lti_pol`. The type does not change.
  - Its own `ALTER COLUMN ... TYPE` has the policy dropped ahead again, and created after.
  - Only the columns a typed table's family keeps on both sides are left to the type.
- **Modified**: the partitioned typed table `rv_r4t_rp` goes from `LIST (id)` to `RANGE (id)`, so it is dropped and recreated, while its type `rv_r4t_rt` retypes `amount`. `rv_r4t_reader_pol` reads `rv_r4t_rp.amount`.
  - The reader's policy is dropped ahead of the retype and created again after the tables.
  - The table is recreated as `create table ... rv_r4t_rp of test_schema.rv_r4t_rt (id with options not null) partition by RANGE (id)`.
- **Modified**: `rv_r4t_ct` goes from `(id, note text, memo text COLLATE "C")` to `(id, note text COLLATE "C", memo text)`, under the typed table `rv_r4t_coll`'s policy and `UPDATE OF note` trigger.
  - The dump now records an attribute's own collation. It is hashed only when set, and a pgc 1.0.x FROM dump takes TO's.
  - The change is migrated with `alter type ... alter attribute "note" set data type text collate pg_catalog."C" cascade` and `... "memo" set data type text cascade`.
  - The table phase never counts a typed table's column as its own retype.
- **Not migrated, by design**: `rv_r4t_outside.rv_r4t_xt` (`amount integer` → `bigint`) is outside the compared schemas. Its typed table `rv_r4t_xtt` has the view `rv_r4t_xv` and the publication member `rv_r4t_xpub` (a column list and row filter on `amount`).
  - Nothing retypes the column, so neither is dropped, released or recreated any more. Both used to be, on every run.
  - The existing warning still says the type change needs a manual `ALTER TYPE`.
- **Modified**: `rv_r4t_o_pol` reads `rv_r4t_a_t.g`, which is dropped and re-added, and `rv_r4t_z.n`, which a later alter retypes in place. It is created once, after every table alter.
- **Added**:
  - `rv_r4t_newc` (`b text COLLATE "C"`), created with its collation;
  - three typed tables of `rv_r4t_it`, each created with only what it adds: `rv_r4t_new` (`id` primary key, `note` `DEFAULT 'n/a' NOT NULL`, a CHECK), `rv_r4t_bare` (no element list) and `rv_r4t_nn` (`amount with options constraint rv_r4t_nn_amount not null`).
- **Not covered by the round trip** (covered by unit tests):
  - a typed table whose retype a storing column blocks;
  - a pgc 1.0.x FROM dump against a current TO dump with attribute collations.
- **Known limitations**:
  - A partition or `INHERITS` child that TO takes out of the family while keeping the old column type gets the new type from the ancestor's recursing retype. This applies to ordinary tables as much as typed ones, and the next run retypes it back.
  - An `INHERITS` descendant's generated column over a typed column that no statement retypes is dropped and re-added on every run.
- **CI assertion** (`.github/scripts/assert-r4-typed.sh`). After round 1, `a` must match `b`, for every `rv_r4t_*` object, in:
  - the attributes and collations of the composite types;
  - which tables exist, typed by what, partitioned how, and their bounds;
  - every column's type, collation, NOT NULL, default and generation (`rv_r4t_xtt` excepted);
  - the policies, triggers, constraints and rows.

  The round-1 SQL must:
  - retype `rv_r4t_ct.note` through `ALTER TYPE`;
  - drop `rv_r4t_ti_pol`, `rv_r4t_ti.g`, `rv_r4t_qtt_pol` and `rv_r4t_qti.g` before their type's retype;
  - never drop `rv_r4t_3c2.g` or `rv_r4t_5c2.g` on its own;
  - never touch `rv_r4t_xv` or `rv_r4t_xpub`.

  `b` must hold the expected collations, typed tables, policy count (10) and retyped child columns, so the check is not vacuous.

### 79. Subscription Replication Slots and Subscriptions Only TO Has (all versions; the `SERVER` case PG19+)

The fixtures are the `rv_r4s_*` subscriptions at the end of the pg19:subscriptions-fdw block. Every one is disabled and points at an unreachable publisher (`host=127.0.0.1 port=1`). Nothing the migration runs contacts that publisher.

**The problem.** pgc did not read `pg_subscription.subslotname`, and it wrote a TO-only disabled subscription `WITH (enabled = false)`. PostgreSQL then connected to the publisher:
- when the publisher was unreachable, the migration failed;
- when it was reachable, the migration created a replication slot that the disabled subscription never consumed, and that slot kept retaining WAL there.

A slot that differed between FROM and TO could not be seen at all, by round 2 either.

**What happens now.** The dump records the slot. The key `slot_name` is absent for a slot named after the subscription, holds `"no_slot"` for `slot_name = NONE`, and holds `{"named": ...}` otherwise.
- **Created without contacting the publisher:** a subscription that is disabled, or has no slot, is created `WITH (connect = false[, slot_name = NONE | '...'], enabled = false, ...)`, as pg_dump writes it. A `--` note says when the recorded slot is not created on the publisher.
- **Created connected:** an enabled subscription is created as before (it creates its slot and copies the tables). It gets `slot_name = '...'` when the slot is not named after it, plus a note to add `create_slot = false` if that slot already exists.
- **Slot changes:** a slot change is `ALTER SUBSCRIPTION ... SET (slot_name = ...)`. It rides in the same `SET (...)` as the other options and always comes before any `ENABLE`.
  - PostgreSQL takes `slot_name = NONE` only on a disabled subscription, so an enabled one is disabled first.
  - The statement neither drops the old slot nor creates the new one on the publisher; a `--` note says so.
- **Hashes:** a subscription on a slot named after it keeps its pgc 1.0.x hash, and a pgc 1.0.x dump, which lacks the slot, is read as one.

**Slot names PostgreSQL refuses.** `CREATE SUBSCRIPTION ... WITH (connect = false)` records the subscription's own name as its slot without checking it, and `RENAME` keeps that name. `slot_name = '...'` only takes 1 to 63 lower-case letters, digits and underscores, and has two more exceptions:
- it reads `'none'` as `NONE`;
- PostgreSQL 19 also reserves `pg_conflict_detection`.

pgc never writes such a slot:
- **Altered subscription:** the `slot_name` is left out of the `SET`, so the subscription keeps its slot. If it has no slot, it also stays disabled.
- **Created subscription:** the subscription gets the slot named after it. If that name is not a valid slot name, it is created `connect = false` even when enabled, because the publisher would refuse to create that slot.

A `--` note in the script and a warning on stderr name each such subscription, and the migration still applies. The slot difference remains, since no DDL can close it. With `USE_COMMENTS=false`, round 2 is empty.

#### Subscriptions (pg19:subscriptions-fdw block)
- **Modified**: `rv_r4s_sub_own_to_none`: its own slot → `NONE` (`SET (slot_name = NONE)`)
- **Modified**: `rv_r4s_sub_none_to_named`: `NONE` → `rv_r4s_named_slot`
- **Modified**: `rv_r4s_sub_named_to_own`: `rv_r4s_old_slot` → its own slot (`SET (slot_name = 'rv_r4s_sub_named_to_own')`)
- **Unchanged**: `rv_r4s_sub_same_named`: `rv_r4s_kept_slot` on both sides
- **Modified, slot left out**: `"rv_r4s_Sub_Upper"`: `rv_r4s_upper_slot` → its own slot `rv_r4s_Sub_Upper` (upper case). No statement; `a` keeps `rv_r4s_upper_slot`.
- **Modified, slot left out**: `rv_r4s_sub_none_alias`: `NONE` → a slot named `none` (TO creates it as `"none"` and renames it). No statement; `SET (slot_name = 'none')` would set NONE.
- **Added**: `rv_r4s_sub_to_only_none`: no slot → `WITH (connect = false, slot_name = NONE, enabled = false)`
- **Added**: `rv_r4s_sub_to_only_own`: its own slot → `WITH (connect = false, enabled = false)`
- **Added**: `rv_r4s_sub_to_only_named`: `rv_r4s_other_slot`, with a comment → `WITH (connect = false, slot_name = 'rv_r4s_other_slot', enabled = false)` and `COMMENT ON SUBSCRIPTION`
- **Added**: `"rv_r4s_Sub_To_Only_Upper"`: its own slot, not a valid slot name → `WITH (connect = false, enabled = false)`, recording that slot
- **Added (PG19+)**: `rv_r4s_sub_to_only_server`: `SERVER pg19_subfdw_srv_a`, no slot, `retain_dead_tuples = true`, `max_retention_duration = 1000`, `wal_receiver_timeout = '30s'`. It is a syntax error on PG14–18, where TO does not have it.

**CI assertion** (`.github/scripts/assert-r4-sub.sh`), run after round 1:
- the slot, enabled state and comment of every subscription of `a` match `b`, except the two left-out ones;
- every `rv_r4s` case above is in `a` (on PG19 also the `SERVER` one, with its options);
- every `CREATE SUBSCRIPTION` in the round-1 SQL carries `connect = false` (every fixture subscription is disabled);
- 4 `rv_r4s` subscriptions are created (5 on PG19), and the three `SET (slot_name = ...)` are present;
- no `rv_r4s_*` replication slot exists;
- `"rv_r4s_Sub_Upper"` and `rv_r4s_sub_none_alias` keep FROM's slot in `a` and are never altered;
- every `slot_name = '...'` the SQL writes is `[a-z0-9_]{1,63}` and not `none`.

**Change to the section 77 step.** It now drops its subscriptions without their slots (`DISABLE`, `SET (slot_name = NONE)`, `DROP`) and removes the slots afterwards. The reason: the migration moves `a` onto the slot `b` streams from, and a plain `DROP SUBSCRIPTION` would wait forever (`DROP_REPLICATION_SLOT ... WAIT`) for a slot another subscription holds. It also compares `subslotname`.

**Other edits to existing sections**

Section 44: replace the note "A fixture subscription must still exist on both sides: pgc's own `CREATE SUBSCRIPTION` connects to the publisher." with:

> **Note**: Subscriptions need no publisher and no replication slot when they are created `WITH (connect = false, slot_name = NONE)`; section 45 uses that. pgc creates a disabled subscription the same way (`connect = false`), so a fixture subscription may exist only in TO (section 79).

Section 45 (line 503): replace "Every subscription is created `WITH (connect = false, slot_name = NONE)`, so it is disabled, owns no slot and can be dropped without a publisher." with:

> The `pg19_subfdw_*` and `rv_pgrants_*` subscriptions are created `WITH (connect = false, slot_name = NONE)`, so they are disabled, own no slot and can be dropped without a publisher. The `rv_r4s_*` subscriptions at the end of the same block (section 79) are disabled too, but most record a replication slot. A plain `DROP SUBSCRIPTION` of one of those contacts the publisher to drop the slot and fails, so detach it first: `ALTER SUBSCRIPTION ... DISABLE`, then `SET (slot_name = NONE)`.

---

### 80. Inherited CHECK Constraints Around SET EXPRESSION and Parent Retypes (all versions; the in-place forms PG17+, enforceability PG19+)

The fixtures are the `inhck_*` tables of the fix:inhcheck block at the end of both schema files. Every CHECK reads the generated column `v` (or the retyped column), except `inhck_i_b`.

**The problem (PG17+).** `ALTER TABLE ONLY ... SET EXPRESSION` rebuilds the CHECK constraints that read the column and gets inheritance wrong; this is a server bug, verified on 17, 18 and 19rc1, STORED and VIRTUAL alike:
- on a child, a copy the table only inherits is dropped and never added back;
- on a child, a merged copy (local and inherited) loses its inheritance;
- on a parent, the re-add recurses into children whose copies were never dropped, so each counts its parent twice (`coninhcount` 2).

The migrated child then accepted rows its CHECK refuses. Round 2 was empty, because a dump holds only local constraints.

**What happens now.** For each table whose alter writes `ALTER TABLE ONLY ... SET EXPRESSION`, every inheritable CHECK that reads one of those columns — its own when it has `INHERITS` children, and its kept ancestors' — is dropped ahead of the table alters, parents first (`InheritedRecreates::family_checks`). It is added back in the section `CHECK constraints added after the table alters`, once every table alter has run, together with any CHECK of the same name that TO adds to those tables (`InheritedRecreates::checks_after_alters`). A partitioned table's statement recurses (no `ONLY`) and is not affected. Without `USE_DROP` nothing can be dropped, so the expression change is left out with a warning.

**The problem (PG19).** A copy kept ENFORCED under a NOT ENFORCED parent is rebuilt from the parent's CHECK by a recursing `ALTER COLUMN ... TYPE` (a collation change included) or a partitioned parent's `SET EXPRESSION`, and comes out NOT ENFORCED. Round 2 caught it, so the migration needed a second run. Such a copy now gets `ALTER CONSTRAINT ... ENFORCED` in the `Enforceability of inherited CHECK constraint copies` section.

#### Tables (fix:inhcheck block)
- **Modified (every version)**: `inhck_a` / `inhck_a_child`: both expressions `(a * 2)` → `(a * 3)`, the CHECK only inherited by the child.
- **Modified (every version)**: `inhck_e` / `inhck_e_child` / `inhck_e_grandchild`: the same over three levels.
- **Modified (every version)**: `inhck_h` / `inhck_h_child`: the CHECK changes too (`v > -1000` → `v > -2000`).
- **Modified (every version)**: `inhck_i` / `inhck_i_child`: `inhck_i_b` reads `b`, not `v`, and is never dropped.
- **Modified (PG17+, created by a `DO` block)**: `inhck_b` / `inhck_b_child`: the child keeps its own `(a * 5)`; the parent's changes.
- **Modified (PG17+)**: `inhck_c` / `inhck_c_child`: only the child's own expression changes, `(a * 5)` → `(a * 7)`.
- **Modified (PG17+)**: `inhck_d` / `inhck_d_child`: the child holds a merged copy.
- **Modified (PG17+)**: `inhck_f_x` / `inhck_f_y` / `inhck_f_m`: two parents with the same CHECK.
- **Modified (PG17+)**: `inhck_g` / `inhck_g_child`: TO adds `inhck_g_v` to the parent; the child's own expression changes.
- **Modified (PG19+)**: `inhck_va` / `inhck_va_child` and `inhck_vc` / `inhck_vc_child`: shapes A and C with VIRTUAL columns.
- **Modified (every version; enforceability PG19+)**: `inhck_en` / `inhck_en_child` (`integer` → `bigint`), `inhck_enc` / `inhck_enc_child` (`COLLATE "C"`), `inhck_enp` / `inhck_enp_1` (a partitioned parent, a column that is not its key): the parent's CHECK NOT ENFORCED, the child's copy ENFORCED.

The `DO`-gated cases need a drop and re-add of an inherited column on PostgreSQL 14 to 16 (and for VIRTUAL on 18), which pgc cannot make yet (see the CHANGELOG's known limitations).

**CI assertion** (`.github/scripts/assert-inhcheck.sh`), run after round 1:
- every CHECK of the `inhck_*` tables, inherited copies included, matches `b`: enforced, validated, local, `coninhcount`, definition; so do the columns and the inheritance;
- a row each child's CHECK must refuse is refused in `a`;
- on PG17+ the CHECK is dropped before the family's first `SET EXPRESSION` and added after its last, `inhck_g_v` is added after the child's `SET EXPRESSION`, and `inhck_i_b` is never dropped;
- on PG19 `ALTER CONSTRAINT ... ENFORCED` follows each parent's retype for `inhck_en_child`, `inhck_enc_child` and `inhck_enp_1`.

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

The generated clear script groups the drops in this order:

1. **Event triggers** that depend on something dropped here (none in this file), ahead of every drop that would fire them.
2. **Views**, topologically sorted by `table_relation`. Tie-break: materialized before regular, then alphabetical by `schema.name`. The SQL-standard routines that read a relation (section 10b) are dropped here too.
   - `v_top_customers` depends on `v_customer_summary`, so it is dropped first.
   - Materialized views with no view-dependencies appear before regular views at the same level.
3. **Foreign key constraints** (all FKs across all tables), then **tables** (`customers`, `categories`, `orders`, `order_items`, `audit_log`, `employees`).
4. **Foreign tables**, then the **subscriptions, user mappings, foreign servers and foreign-data wrappers** that depend on something dropped inside the selected schemas and that nothing outside them depends on (none in this file).
5. **Extended statistics**, then **casts** and **operators** (none in this file).
6. **Routines** (`update_timestamp`, `get_customer_order_total`, `format_audit_entry`, `active_customer_count`, `cleanup_old_orders`).
7. **Sequences** (`customer_id_seq`, `audit_id_seq`).
8. **Types** (`order_status`, `full_name`, `positive_int`).
9. **Text search configurations and dictionaries, collations** (none in this file).
10. **Extensions** (`uuid-ossp`, `pg_trgm`).
11. **Schemas** (`clear_app`, `clear_shared`).

The command then moves a drop only where a `pg_depend` dependency requires it. No dependency in this file crosses the groups, so the output follows the group order. Within a group, objects come in catalog order, which differs between majors; `clear_expected.sql` holds the PostgreSQL 19 output.

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
- [ ] Every dependency `pg_depend` records between dropped objects is respected (a domain before the function its CHECK calls, an `INHERITS` child before its parent, an aggregate before its state function, a table before the view its rule reads)
- [ ] A dependency cycle through a table's constraint, default, stored generated expression, trigger, rule, policy or index is broken by dropping that part first
- [ ] Event triggers are dropped before anything else
- [ ] Database-wide objects (foreign-data wrappers, servers, user mappings, subscriptions, event triggers) are dropped only when they depend on something dropped inside the selected schemas and nothing outside them depends on them; an extension in an unselected `public` does not count

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
- Creating extended statistics on expressions with a valid kind list: no `expressions` kind, and none at all for univariate statistics (PG14+)
- Rebuilding the ON list of extended statistics from `pg_get_statisticsobjdef_columns`, so literals containing commas and `EXTRACT(... FROM ...)` survive, and detecting changes inside such expressions (PG14+)
- Detecting extended statistics target changes (`ALTER STATISTICS ... SET STATISTICS`), including on PG17+
- Setting a statistics target again after `ALTER COLUMN ... TYPE` or `SET EXPRESSION` makes PostgreSQL rebuild the statistics object, which resets the target
- Recreating extended statistics that PostgreSQL drops along with something pgc changes: a column pgc drops and re-adds, a generated column removed by `DROP FUNCTION ... CASCADE` (including statistics on its table's partitions and statistics new in TO), a table recreated for a partition-key change, or a materialized view that is rebuilt
- Creating extended statistics on a new materialized view after the view, and leaving a partition's statistics alone when only its parent's column type changes
- Creating, dropping, and altering extended statistics on virtual generated columns (PG19+)
- Detecting NOT ENFORCED constraint flag changes (PG18+)
- Handling virtual/stored generated column transitions (PG18+)
- Detecting UNLOGGED ↔ LOGGED table persistence changes, ordering `SET UNLOGGED` / `SET LOGGED` topologically over the FK graph (leaves-first / roots-first respectively) and suppressing redundant `ALTER SEQUENCE ... SET UNLOGGED|LOGGED` on owned sequences whose owning table is flipping in the same migration — PostgreSQL propagates the table change to owned sequences automatically (Issue #180)
- Detecting storage parameters (reloptions/WITH clause) changes (fillfactor, autovacuum settings, etc.)
- Detecting REPLICA IDENTITY changes (DEFAULT, NOTHING, FULL and USING INDEX), including a switch between two indexes and an index rebuilt under the same name. A table leaves USING INDEX first only when the migration drops or rebuilds that index, or drops, re-creates or clears the NOT NULL of one of its columns. It sets USING INDEX only after the index and validated NOT NULLs exist: post-commit under `--output-for-production` when the index is built there, and after the whole table section for a partition's index created from its parent. A table left on USING INDEX after its index was dropped is treated as NOTHING.
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
- Recreating dependents silently dropped by `DROP FUNCTION ... CASCADE` when a routine is fully dropped or its return type / argument list / argument defaults change (functional indexes, CHECK constraints, generated columns, column DEFAULT expressions, RLS policies — Issue #179)
- Recreating _secondary_ dependents of a CASCADE-dropped generated column — indexes / CHECK constraints / RLS policies anchored on the column rather than the routine — driven by a `pg_catalog.pg_depend` snapshot captured at dump time; same machinery also runs after the virtual-generated-column drop+add path in `TableColumn::get_alter_script` (Issue #188)
- Recreating a publication that leaves `FOR ALL TABLES`, since no ALTER can. Emptying a publication with `DROP TABLE ONLY` / `DROP TABLES IN SCHEMA`, leaving out tables the migration already dropped.
- Putting back the publication membership rows lost when the migration recreates a member table or an EXCEPT table.
- Writing inheritance-parent publication members as `ONLY`, so each member stays exactly one `pg_publication_rel` row.
- Creating and altering publication row filters, column lists and `FOR TABLES IN SCHEMA` members, with `publish_via_partition_root` changes ordered around the member change (PG15+).
- Detecting `publish_generated_columns` changes (PG18+).
- Creating and altering `FOR ALL SEQUENCES` and `FOR ALL TABLES EXCEPT (TABLE ONLY ...)` publications, in place with `ALTER PUBLICATION ... SET ALL ...` wherever PostgreSQL allows it (PG19+).
- Dumping only the current database's subscriptions (`pg_subscription` is cluster-wide; filtered by `subdbid`)
- Switching subscriptions between `CONNECTION` and `SERVER` sources and between servers (PG19+): after the target server and user mapping exist, before FROM-only servers and wrappers are dropped, and after an owner change so the new owner's user mapping is the one checked
- Detecting subscription `retain_dead_tuples`, `max_retention_duration` and `wal_receiver_timeout` changes, with the `retain_dead_tuples` `SET` placed before or after the source change as PostgreSQL requires (PG19+)
- Detecting foreign data wrapper `CONNECTION` function additions, changes and removals (PG19+)
- Dropping FROM-only subscriptions before any other statement, and FROM-only foreign servers (then wrappers) only after the subscription changes
- Updating changed aggregates in place with `CREATE OR REPLACE AGGREGATE` (options, PARALLEL, SORTOP as `OPERATOR(...)`, negative (unbounded) SSPACE / MSSPACE), and dropping first when the routine kind, aggregate kind or number of direct arguments changes
- Re-applying the privileges of a routine that is dropped and recreated (return type or kind change), including a `REVOKE … FROM PUBLIC`, in the same migration
- Detecting aggregate SUPPORT function changes via `CREATE OR REPLACE AGGREGATE` (PG19+)
- Emitting list-parameter (`GUC_LIST_QUOTE`) SET values element by element when an element is empty or an identifier longer than 63 bytes (`SET search_path TO '', 'public'`, numbers kept bare), and an empty list as `SET … TO NULL` when the migrated server is PG19+ (PG19+)
- Altering CHECK constraint enforceability in place with `ALTER CONSTRAINT ... [NOT] ENFORCED`, with inheritance parents before their children (PG19+). Drop + re-add on PG14-18, and on PG19 when the table re-creates a column or a child's copy must stay enforced.
- Changing generated column expressions in place with `ALTER COLUMN ... SET EXPRESSION`: STORED on PG17+; VIRTUAL on PG19+, or PG18 without CHECK constraints in the family; never for explicit publication members. `ONLY` on non-partitioned tables, recursing on partitioned ones. After every `ADD COLUMN` of the table.
- Falling back to `DROP COLUMN` + `ADD COLUMN` for generated columns (type changes, kind flips, servers without SET EXPRESSION). Dependent views are pre-dropped, dependents restored, and one shared drop + add is used per classical-inheritance family.
- Ordering classical inheritance (`INHERITS`) parents before children on create and children before parents on drop
- Applying text search dictionary option-only changes in place with `ALTER TEXT SEARCH DICTIONARY ... (removed_option, option = value, ...)`, never `DROP ... CASCADE`, so dependent configurations, generated tsvector columns and indexes survive. These are emitted before the tables, so a generated column added in the same migration uses the new options. Template changes are still drop + recreate.
- Switching a Snowball dictionary from `dutch` to `dutch_porter` in place, and creating the new Polish dictionaries and copies of the Esperanto configuration (PG19+)
- Round-tripping oid8 columns, arrays, defaults, CHECKs, hash indexes, domains, range types, functions and views; bigint/oid ↔ oid8 as `ALTER COLUMN TYPE` without `USING` (PG19+)
- Round-tripping views and materialized views that use window `IGNORE NULLS`; `RESPECT NULLS` is the default and produces no diff (PG19+)
- Detecting the move from the pre-19 `JSON_ARRAYAGG` rewrite to PG19's `JSON_ARRAY(query)` (PG19; no diff on PG16–18)
- Detecting the heap reloptions `autovacuum_parallel_workers` and `log_autoanalyze_min_duration` on tables, partitions and materialized views (PG19+)
- Never pinning an extension `VERSION` the migrated (FROM) server cannot install: the extension is created at the server default, or the version is left unchanged, each with an explanatory comment (PG19 btree_gist 1.9)
- Starting the migration with `set standard_conforming_strings = on;` when the migrated server's clients default to `off` (PG14–18), so backslashes in literals are stored as written
- Dumping tables whose names start with `pg` followed by any character other than `_` (`pgbench_accounts`)
- Dumping SQL-standard routine bodies (`BEGIN ATOMIC ... END`, `RETURN expr`) under an empty `search_path` and emitting them verbatim after every option (PG14+). This includes body changes, switches between a quoted body and a SQL-standard one, and `INSERT ... ON CONFLICT DO SELECT` bodies (PG19+).
- Treating a routine with a SQL-standard body as a hard dependent of what it uses:
  - It is dropped before a table, sequence or view it reads is dropped or rebuilt, a column it reads is dropped or retyped, or a routine or operator it uses is dropped and re-created.
  - It is re-created afterwards with its grants and dependents.
  - It is created after the routines, views, operators and casts it uses.
- Detecting a domain whose only change is its CHECK constraints (body changed, constraint added or removed, `NOT VALID` ↔ validated, with `VALIDATE CONSTRAINT` in place for `NOT VALID` → validated), and an enum whose only change is a label, including in dumps written by pgc 1.0.x
- Adding and validating domain CHECKs after the tables, routines, operators and casts they check against; dropping them early
- Adding enum values before `begin;` with `--use-single-transaction`, and noting (not ignoring) an enum whose labels are in a different order
- Applying rule changes with `CREATE OR REPLACE RULE`, never `CREATE RULE` on an existing rule. Every rule statement ends in a single semicolon, and rule and table names that need quoting are dropped and commented under their real names.
- Ordering rules around the rest of the migration:
  - Rules are created and changed after routines, views, operators and casts.
  - Removed rules are dropped before any view or table DDL. So are rules standing in the way of a table, foreign table, column, view, sequence or routine change; those are recreated afterwards.
  - Unchanged rules are recreated when the migration drops them along the way: their table is dropped or recreated (a partition-key change, a table turned into a view), their view is rebuilt, or a routine's CASCADE takes their view or a view they read. A comment TO removed is cleared on the recreated rule.
  - This is driven by a `pg_depend` snapshot of each rule captured at dump time (`Rule::depends_on`), and by the rule's own relation for dumps without one.
- Revoking, in full grants mode, privileges granted by a role other than the object's owner by naming the grantor (`REVOKE ... GRANTED BY` on PG19+, `SET ROLE <grantor>` before). Entries granted under a grant option the migration removes are revoked leaf first, and the owner grants back what TO still wants. The GRANT side stays grantor-insensitive, so differences only in grantor produce no diff. In production output these revokes are guarded so a replayed migration does not fail.
- Granting back, in the same run, the column privileges that a table-level `REVOKE` strips (PostgreSQL applies it to every column), and first revoking the column entries that depend on a column grant option the table statement removes
- Using serial/bigserial/smallserial for a new plain table's column only when the pseudo-type recreates the owned sequence exactly and no new table created before it names that sequence. Any other owned sequence is created explicitly before the tables and linked with `ALTER SEQUENCE ... OWNED BY` after them. That covers a non-integer or `oid8` column, a mismatched type, a custom or renamed sequence name, an existing table, a partition or typed table, a nullable column, tuned options, a comment, a changed persistence, and a sequence an earlier table references. Only an identity column's own identity sequence is left to the column.
- Creating tables whose quoted column names contain spaces
- Writing object and role names into the migration's comments so that none can end its comment. A line feed or bare carriage return becomes a space, `*/` and `/*` are split apart, and a commented-out statement has `-- ` in front of every physical line. The `USE_COMMENTS=false` stripper and the production rewriter end a `--` comment at a bare carriage return, as PostgreSQL does.
- Naming materialized views through `quote_ident`, as every other object is named. Materialized views in a dump written by pgc 1.0.x are quoted on load.
- Dollar-quoting the guarded partition `REPLICA IDENTITY` block with a tag that no name inside it contains.
- Changing foreign-table options key by key with `SET` / `DROP` / `ADD`.
- Putting back everything a routine's `DROP ... CASCADE` takes along, whether the routine is dropped ahead of the table DDL (a SQL-standard body in the way) or in the routine phase (return type, arguments or kind change, removal):
  - domain CHECKs;
  - triggers whose function or `WHEN` clause calls it, and the enabled state and comment of their partition clones;
  - extended statistics;
  - indexes on materialized views;
  - foreign-table column defaults;
  - routines whose argument defaults or SQL-standard bodies call it, with their grants;
  - CHECK/EXCLUDE constraints, indexes, defaults, generated columns and policies that the table changes created or changed to call it.
- Re-creating in the routine phase, not ahead of the tables, a routine that the routine phase's CASCADE takes along through its argument default, so the table changes can still use it.
- Leaving out of the table changes the constraints, indexes, policies and defaults that call a routine dropped ahead of them, and creating them once the routine is back.
- Detaching a domain `DEFAULT` that calls a routine right before the routine is dropped with CASCADE, and setting it again afterwards.
- Never letting a CASCADE silently drop a partitioned table (through its partition key), a range or base type, a foreign-data wrapper, or an operator or cast pgc would not re-create. Also never letting one make a new column's default go missing. The routine is either not dropped ahead of the change, or dropped without CASCADE, and the migration fails as pgc 1.0.x did.
- Dropping a partition retyped through its parent from a publication like any other member, before `SET ALL TABLES [EXCEPT]` (PG19) and when the list empties. Such a partition is never dropped, so it keeps its `pg_publication_rel` row. An unchanged publication listing it is left alone.
- Relaxing a CHECK (`ALTER CONSTRAINT ... NOT ENFORCED`) before an `ALTER COLUMN ... TYPE` / `SET EXPRESSION` of the table, and tightening one (`... ENFORCED`) after it, so the rewrite is checked against the target enforcement (PG19+)
- Relaxing or dropping a partition's or an inheritance child's own CHECK ahead of every table alter when a parent's recursing `ALTER COLUMN ... TYPE` / `SET EXPRESSION` rewrites that table. The re-add or the tightening follows the rewrite (all versions).
- Planning the column privileges of a re-created column from nothing: a generated column dropped and re-added (including a partition's or an INHERITS follower's copy), or any column of a dropped-and-recreated table. TO's grants come back in the same run, and nothing already gone is revoked. A dropped-and-recreated table's table-level privileges start from the default privileges (`full`) or from nothing (`addonly`). A partition retyped through its parent, and any table whose DROP TABLE is only commented out (no `--use-drop`), keeps its table and column privileges, and those are diffed as such.
- Refusing to compare a FROM dump written by pgc 1.1+ with a TO dump written by pgc 1.0.x (non-zero exit, no script), because every gap in the old dump would become a drop, an emptied routine or a stripped publication
- Comparing a FROM dump written by pgc 1.0.x with a TO dump written by pgc 1.1+ with a warning on stderr (naming the tables pgc 1.0.x could not see and the subscriptions it could not place), a comment at the top of the script, and `set standard_conforming_strings = on;` first
- Creating every TO subscription when the FROM dump was written by pgc 1.0.x, which lists the subscriptions of the whole cluster, instead of taking another database's subscription of the same name as present
- Recreating, in the same migration, every partition that the `DROP TABLE` of a recreated table takes along, at any depth and whether or not its own definition changed. A table is recreated on a partition-key change, a type change on a partition-key column, or a plain table turned partitioned. This includes each partition's indexes, grants, statistics, publication membership, rules and own triggers, and the views and SQL-body routines that read it. The parent's cloned row triggers are not created a second time, and a partition's own trigger named like one of the parent's still is.
- Attaching a table to, or moving it under, a recreated table only after that table is back, so the `DROP TABLE` does not take it along; the same holds for a new partition of a recreated table, mixed-case names included.
- Keeping the sequence of a serial column on a recreated table, with its value and privileges: `ALTER SEQUENCE ... OWNED BY NONE` before the `DROP TABLE`, `OWNED BY` again after the tables.
- Dropping the foreign keys that block the `DROP TABLE` of a recreated table before the table DDL and adding them back after it. These are keys referencing the table or one of its partitions, including one naming a `public` table without its schema, and keys referencing a partitioned table whose recreated sub-partition PostgreSQL copies them onto. The recreated table's own foreign keys are added back too.
- Under `--output-for-production`, foreign keys are added `NOT VALID` and then validated, except on a partitioned table before PG18, which cannot take `NOT VALID`.
- Keeping the case of quoted identifiers in `ALTER TABLE ... ADD CONSTRAINT`.
- Dropping a view that calls a routine the routine phase drops with `DROP ... CASCADE` ahead of the table changes when another view, a materialized view or a SQL-standard routine reads it.
  - Its readers, and the rules on or reading them, are dropped with it. All of them are re-created after the routine in dependency order, with their owners, comments, indexes, statistics and privileges.
  - Which views call the routine is read from `pg_depend` (the dump's per-view `routine_dependencies`), so a view calling an unchanged overload, or a built-in of the same name, is left alone.
  - A view whose plain `DROP VIEW` would fail, and every view of a pgc 1.0.x FROM dump, is left to the CASCADE as before.
- Re-applying, in the same migration, the privileges of a view re-created after a routine's `DROP ... CASCADE` took it, planned from the schema's default privileges. A view matched only by the routine's name, which the CASCADE does not take, keeps its privileges, grantor-named revokes included.
- Planning the privileges of a routine the migration creates (new, or dropped and re-created) from the FROM default privileges for functions of every role that may create it: a database-wide entry replaces PUBLIC's `EXECUTE`, and a schema entry adds to it.
- Comparing a NULL routine ACL in TO with an explicit one in FROM as the default it stands for (PUBLIC's `EXECUTE`). No `REVOKE ... FROM PUBLIC` is emitted for it, and PUBLIC's `EXECUTE` is granted back where FROM lacks it.
- Spelling grantees whose names need quotes (mixed case, keywords, spaces) as quoted identifiers in object and column `GRANT` / `REVOKE` and in `ALTER DEFAULT PRIVILEGES`, the same way as the grantor-named revokes. The owner's own aclitem entry is recognised whatever its case.
- Comparing a routine's or type's NULL ACL as the default privileges it stands for (PUBLIC `EXECUTE` / `USAGE`), so `REVOKE ... FROM PUBLIC` and its reverse are migrated in the first run. An ACL with everything revoked (`{}`) is kept distinct from NULL.
- Giving new routines, recreated routines, new types and recreated range types their target ACL whichever role runs the migration. What the target lacks is revoked against acldefault and anything a FROM default privilege of any role could add. What it holds is granted, including PUBLIC's default whenever a database-wide default privilege could have removed it.
- Emitting no `GRANT` / `REVOKE` for multirange types, and not altering (or dropping and recreating) a type whose ACL alone differs.
- Quoting subscription publication names in `CREATE SUBSCRIPTION` and `ALTER SUBSCRIPTION ... SET PUBLICATION`.
- Writing `SET PUBLICATION ... WITH (refresh = false)` for a subscription that is disabled at that point, with `REFRESH PUBLICATION` after its `ENABLE`. With `--use-single-transaction` the refresh always comes after `commit;`.
- Releasing the publication rows that block a table change before the tables, and restoring them after. These are members whose column list or row filter uses a dropped, retyped or re-added column, members made UNLOGGED, and on PG19 EXCEPT entries made UNLOGGED or attached as partitions. The target membership is restored, or the publication's own list when the migration keeps it as it is (FROM-only without `use_drop`, or a commented-out recreate). A note and a warning cover rows that cannot come back.
- Dropping a generated column (STORED or VIRTUAL) before a retype of a column it reads, and re-adding it from TO afterwards with its dependents restored. A generated column that becomes plain keeps its values (`DROP EXPRESSION` first). A generated column that TO drops, or that reads a column TO drops, is dropped first.
- Leaving the retype of an inherited column to the `INHERITS` ancestor whose `ALTER COLUMN ... TYPE` recurses. A child's generated columns that read it are dropped ahead of every table alter and re-added in the child's alter.
- Dropping, ahead of every table alter, the policies, the triggers (`UPDATE OF`, `WHEN`) and other tables' foreign keys that block the drop of a column the migration drops and re-adds, and putting back the TO side's afterwards. On PG18+ a named NOT NULL is re-added under its name. Constraint definitions keep quoted identifiers' case.
- Comparing extended statistics with their plain columns as a set, since PostgreSQL prints them in attnum order. Expressions keep their order.
- Dropping every copy of a CHECK in an inheritance family with a merged child copy (parents first, ahead of all table alters) before re-adding any, when the enforceability flip (PG14–18) or an expression change cannot be made in place. An unchanged ancestor copy is dropped and re-added with it.
- Never creating or dropping a partition's clone of its parent's row trigger. Only a clone's own enabled state and comment are dumped and applied, after the parent's trigger:
  - a partition whose parent is not dumped records every clone, and a new such partition gets its clones' TO state set;
  - pgc 1.0.x dumps that list clones as the partition's own triggers are read the same way.
- Dropping a table's own trigger before it is attached to a partitioned parent whose row trigger of the same name the attach clones onto it.
- Clearing the round-trip fixture databases (`--command clear`) without an error and without `CASCADE`, in the order `pg_depend` records:
  - event triggers first;
  - domains before the functions their CHECKs call and before their base domains;
  - `INHERITS` children before their parents;
  - tables before the views their rules read;
  - aggregates before their state functions;
  - casts and operators before their functions;
  - on PostgreSQL 14, the readers of a generated column before the function it calls;
  - foreign-data wrappers, servers, user mappings and `SERVER` subscriptions before the extension they depend on.
- Breaking a dependency cycle through a table's constraint, default, stored generated expression (`DROP EXPRESSION`), trigger, rule, policy or index by dropping that part first.
- Leaving database-wide objects alone when they hang off an extension in an unselected `public`, or when something outside the selected schemas still depends on them.
- Leaving an aggregate's `SUPPORT` out, with a note, when the migrated (FROM) server is known to be older than PostgreSQL 19. This applies only to cross-version compares.
- Putting back what a routine's `DROP ... CASCADE` takes with a stored generated column that calls it:
  - the views, rules and SQL-standard routines reading it, re-created once the column is back;
  - its privileges and its `UPDATE OF` / `WHEN` triggers;
  - its publication rows;
  - a partition's own objects on its copy;
  - what only TO has on it (index, CHECK, UNIQUE and the FK referencing it, policy).
  The table phase leaves the column's type to the re-add.
- Restoring what calls a routine that is only re-created after that re-add — column defaults, CHECKs, policies — after the routine, not before.
- Creating extended statistics on a materialized view after the view, also when the view goes with the routines held back for operators or casts.
- Releasing and restoring the publication rows that use a generated column dropped and re-added around a retype, on a table or a partition.
- Restoring a partition's own index, CHECK, UNIQUE (with its referencing foreign key), statistics and policy on a generated column its parent drops and re-adds around a retype.
- On PostgreSQL 14:
  - dropping and re-adding a generated column over a non-built-in function or operator whose type changes;
  - rewriting, with its values kept, one that becomes plain with a new type or whose function the migration drops.
- Dropping, ahead of the table alters, the policies and triggers that block an in-place `ALTER COLUMN ... TYPE`, including another table's policy reading the column through a subquery, and creating TO's afterwards.
- Writing a column's `COLLATE` clause with the collation's schema and both parts quoted as `quote_ident` does (`collate test_schema.rv_r3d_ci`, `collate public."ci""; ..."`). `pg_catalog` collations keep `collate "C"`. A collation change alone, the schema included, is migrated with `ALTER COLUMN ... TYPE ... COLLATE`, except on a partition-key column, where the table is dropped and recreated as for any type change there. A pgc 1.0.x dump still matches a current dump of the same table.
- Creating the collations TO adds before the types and tables, so a column, a new table or a domain can use them.
- Dropping the policies and triggers (`WHEN`, `UPDATE OF`) that depend on a column retyped in place ahead of the table alters, and putting back TO's afterwards.
- Naming an event trigger whose name needs quotes exactly once (`"rv_r3d_Keep Trg"`) in its create, alter, comment and drop.
- Keeping primary-key column names that hold `)`, `,` or `"` whole in `CREATE TABLE`, followed by the key's `INCLUDE (...)` and `DEFERRABLE ...`.
- Dropping ahead of a `DROP TABLE` only the foreign keys that reference the dropped table or one of its partition ancestors, dropping each once, and adding back those TO keeps.
- Quoting, in a pgc 1.0.x dump, materialized views named after column-name or type/function-name keywords as `quote_ident` does on the server of the dump they are compared with (`left` always; `json_object` from PostgreSQL 16, `json` from 17), so they are not dropped and recreated.
- Altering composite types attribute by attribute (`ALTER TYPE ... ADD / DROP / ALTER ATTRIBUTE`), with `CASCADE` when typed tables use the type.
  - The alterations run after the view drops.
  - Attributes are dropped and re-added to reach the target order only when nothing stores the type.
  - A retype PostgreSQL refuses (a column stores the type) is left as a note and a stderr warning.
  - A `CASCADE` attribute drop is announced with a note and a stderr warning.
- Leaving a typed table's added, dropped and retyped columns to its type.
  - A column the type adds is filled with its default before `SET NOT NULL`.
  - A typed table whose type is not compared gets a note and a stderr warning.
- Changing a sequence's data type with `ALTER SEQUENCE ... AS`.
- Changing the owner of a linked sequence:
  - leaving a serial or identity sequence's owner to its table's `OWNER TO`;
  - unlinking a sequence that moves to another table (or to none) before its owner changes.
- Creating a range type's multirange in the schema the target has it in, and moving a multirange an earlier pgc created in the `search_path` schema with `ALTER TYPE ... SET SCHEMA`.
- Comparing a routine whose `SET <list GUC> TO NULL` a pre-19 migrated server can only store as `= ''` in that form, with a note and a warning.
- Printing a stderr warning, whatever `USE_COMMENTS` says, for every type or enum difference pgc leaves in place:
  - an enum value it cannot remove or reorder;
  - a domain base type or collation change;
  - a type kind change;
  - a type it cannot create.
  The same applies to an extension owner change.
- Keeping the enforceability TO has for a CHECK copy a table only inherits. The dump now records such a copy when it is ENFORCED under NOT ENFORCED parents. After every table alter, parents first, a copy that has to change gets `ALTER TABLE ... ALTER CONSTRAINT ... [NOT] ENFORCED` (PG19), or a comment and a warning (PG18). A merged copy that a recursing `NOT ENFORCED` reaches is set ENFORCED again when TO keeps it so.
- Attaching a table to a partitioned parent: the column changes and constraint drops it needs come first. The parents' CHECKs are added, re-added or given TO's enforceability before the attach, a merged one is never dropped, and the table and its own partitions are altered after its new parents.
- Diffing the CHECK copies a detach or `NO INHERIT` leaves behind right after it.
- Migrating `INHERITS` changes with `NO INHERIT` / `INHERIT`. A CHECK that TO has the table only inherit is dropped and re-added by its holders around the `INHERIT`.
- Pairing PG18+ NOT NULL constraints by column. A drifted generated name is left alone. A name TO chose, moved or swapped is renamed through guarded `DO` blocks (with a temporary name for a cycle), never dropped.
- Re-creating a foreign server whose wrapper changes without `CASCADE`, together with:
  - its user mappings, grants and the foreign tables on it (owner, comment, privileges, column privileges, partition attach);
  - the views over those foreign tables.
  It is refused while a PG19 `SERVER` subscription that the migration keeps uses it.
- Replacing a text search dictionary whose template changes (or whose options the in-place ALTER cannot make) in one `DO` block before the tables, without `CASCADE`. The configurations move to the new dictionary, and the columns and indexes built through them stay.
- Recording the server's version, installable extensions and `standard_conforming_strings` default in a dump whose schema pattern matches no accessible schema, together with the database-wide objects (event triggers, publications, subscriptions, foreign-data wrappers, servers, user mappings, extensions in `public`) a dump of any schema of the same database holds. Such a dump then compares as the pgc 1.1+ dump it is in either position: a first deployment into a fresh database, or a TO without the schema. Neither position drops or re-creates a database-wide object both databases hold.
- Changing `retain_dead_tuples` on an enabled subscription by disabling it first, waiting (60 s at most) until `pg_stat_subscription` shows none of its workers, and making every other change while it is disabled, `SET PUBLICATION ... WITH (refresh = false)` included. `ENABLE` and `REFRESH PUBLICATION` follow (PG19+).
- Dropping, ahead of a composite type's `ALTER ATTRIBUTE ... TYPE ... CASCADE`, what is in its way:
  - the policies and triggers on the column it retypes, in every table it reaches: the typed tables, their partitions and `INHERITS` descendants, and typed tables the table phase drops or recreates later;
  - other tables' policies reading such a column;
  - a descendant's generated columns reading it, after their own blockers. A dropped descendant leaves an inherited copy to its ancestor.
  Attributes are matched to their columns by their unquoted name. TO's policies and triggers are created once, after every table alter, and the generated columns re-added in their table's alter.
- Recording a composite attribute's own collation (hashed only when set), creating it with the type, and migrating a change of it with `ALTER ATTRIBUTE ... SET DATA TYPE ... COLLATE ... CASCADE`.
- Not counting as a retype of the table a typed table's column, or its copy in a partition or inheritance child kept in the family: a view, rule, routine body, publication member, policy or trigger over it is left in place when the type does not retype it. A descendant that leaves the family retypes itself, with its blockers out of the way.
- Creating a policy that a column drop + add takes along, and that also reads a column another table retypes in place, once after every table alter.
- Creating a typed table as `CREATE TABLE ... OF <type>`, with only `<column> WITH OPTIONS` defaults and NOT NULLs and the primary key in its list, or with no list at all.
- Creating a subscription that is disabled or has no replication slot `WITH (connect = false, ...)`, so the migration never contacts its publisher. Its slot is named (`slot_name = NONE` / `'...'`) when it is not the subscription's own. Migrating a slot change with `ALTER SUBSCRIPTION ... SET (slot_name = ...)`, which disables an enabled subscription first for `NONE`.
- Never writing a slot name PostgreSQL refuses as `slot_name` (not 1 to 63 lower-case letters, digits and underscores; `none`; `pg_conflict_detection` on PostgreSQL 19+). The slot is left out with a `--` note and a stderr warning, so the migration applies and round 2 is empty.


## Role privileges for the dumping user

The `pgc` dump path reads table column metadata directly from
`pg_catalog` (`pg_attribute`, `pg_attrdef`, `pg_depend`, `pg_sequence`,
`pg_collation`, `pg_rewrite`) rather than from the
privilege-filtered `information_schema.columns` /
`information_schema.view_column_usage` views. As a result, fixture
authors do not need to grant `SELECT` on every fixture table to the
role running `pgc` — the column list, column defaults, identity
metadata, generation expressions, and view dependency lookups are
**privilege-independent**.

This also means that any role with `CONNECT` on the database and
`USAGE` on the relevant schemas can produce a faithful dump; a
non-owner role no longer silently loses columns belonging to tables
it cannot `SELECT` from. Keep this in mind when adding fixtures that
exercise grant-related behaviour: the column introspection path itself
is not the place to test row-level filtering.
