# Database Schema Comparison Test Scripts

This folder contains two comprehensive PostgreSQL schemas designed to test all possible differences that the PGC (PostgreSQL Database Comparer) tool can detect.

## Files

- `schema_a.sql` - The "FROM" database schema
- `schema_b.sql` - The "TO" database schema

## Testing Coverage

These schemas are designed to test comparison capabilities for the following PostgreSQL objects:

### 1. Extensions
- **Added**: `hstore` extension in Schema B
- **Removed**: `pgcrypto` extension in Schema B
- **Unchanged**: `uuid-ossp`, `pg_trgm` extensions

### 2. Schemas
- **Added**: `new_reporting_schema` in Schema B
- **Unchanged**: `test_schema`, `shared_schema`

### 3. Custom Types

#### Enums
- **Modified**: `status_type` - added 'suspended' value in Schema B
- **Removed**: `priority_type` in Schema B
- **Added**: `contact_type` in Schema B

#### Composite Types
- **Modified**: `address_type` - added 'region' field in Schema B
- **Modified**: `user_profile` - added 'phone' field in Schema B

#### Domain Types
- **Modified**: `positive_integer` - added upper limit constraint in Schema B

### 4. Sequences
- **Modified**: `user_id_seq` - different start value in Schema B
- **Modified**: `global_counter_seq` - different cache size in Schema B
- **Removed**: `order_id_seq` in Schema B
- **Added**: `report_id_seq` in Schema B

### 5. Tables

#### Modified Tables
- **users**: Added columns (`preferred_contact`, `timezone`, `two_factor_enabled`), modified column length (`username`)
- **categories**: Added columns (`category_code`, `icon_url`)
- **products**: Added columns (`barcode`, `manufacturer`, `warranty_months`, `is_featured`), removed columns (`weight`, `priority`), modified precision (`price`)
- **audit_logs**: Added columns (`session_id`, `request_id`)
- **identity_update_test**: Modified identity column options (`START WITH`, `INCREMENT BY`)

#### Removed Tables
- **orders**: Completely removed in Schema B
- **order_items**: Completely removed in Schema B

#### Added Tables
- **reviews**: New table in Schema B
- **daily_stats**: New table in new schema in Schema B

### 6. Indexes
- **Modified**: Various indexes updated for new/changed columns
- **Removed**: Indexes related to removed tables/columns
- **Added**: Indexes for new tables and columns

### 7. Functions

#### Modified Functions
- **generate_order_number**: Different prefix ('REF-' instead of 'ORD-') in Schema B

#### Removed Functions
- **calculate_order_total**: Removed in Schema B (related table removed)
- **get_user_order_count**: Removed in Schema B (related table removed)

#### Added Functions
- **calculate_average_rating**: New function in Schema B
- **get_user_review_count**: New function in Schema B
- **update_daily_stats**: New function in Schema B
- **SQL routines**: Added `get_active_usernames_sql` and `product_price_with_tax_sql` to cover SQL-language routines with and without dependencies on plpgsql functions (signature differs in Schema B)

### 8. Procedures

#### Removed Procedures
- **cleanup_old_orders**: Removed in Schema B (related table removed)

#### Added Procedures
- **cleanup_old_reviews**: New procedure in Schema B

### 9. Triggers

#### Removed Triggers
- **trigger_orders_audit**: Removed in Schema B (related table removed)

#### Added Triggers
- **trigger_reviews_update_timestamp**: New trigger in Schema B
- **trigger_reviews_audit**: New trigger in Schema B

### 10. Views

#### Modified Views
- **product_inventory**: Added new columns, modified threshold logic in Schema B

#### Removed Views
- **user_order_summary**: Removed in Schema B (related table removed)

#### Added Views
- **user_review_summary**: New view in Schema B
- **product_review_stats**: New view in Schema B

### 11. Constraints

#### Modified Constraints
- Various check constraints updated for new business rules

#### Removed Constraints
- Constraints related to removed columns

#### Added Constraints
- New constraints for new columns and tables

### 12. Comments
- **Modified**: Updated comments for existing objects
- **Added**: Comments for new objects

## Usage

To test the comparison tool with these schemas:

1. Create two separate PostgreSQL databases
2. Run `schema_a.sql` on the first database (FROM)
3. Run `schema_b.sql` on the second database (TO)
4. Use the PGC tool to compare the schemas:

```bash
# Create dumps
pgc --command dump --database db_a --scheme test_schema --output schema_a.dump
pgc --command dump --database db_b --scheme test_schema --output schema_b.dump

# Compare
pgc --command compare --from schema_a.dump --to schema_b.dump --output comparison_result.sql
```

The resulting comparison script should contain SQL statements to transform the "FROM" database structure to match the "TO" database structure, including all the differences listed above.

## Expected Comparison Results

The comparison should detect and generate SQL for:
- Dropping removed extensions, types, tables, functions, etc.
- Adding new extensions, types, tables, functions, etc.
- Modifying existing structures (ALTER statements)
- Updating constraints, indexes, triggers, and views
- Handling dependencies correctly (drop in correct order, create in correct order)

This comprehensive test suite ensures that the PGC tool can handle all major PostgreSQL schema differences that might occur in real-world database evolution scenarios.
