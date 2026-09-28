-- Schema A: Complex PostgreSQL schema for testing database comparison
-- This schema represents the "FROM" database in comparisons

CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE SCHEMA IF NOT EXISTS shared_schema;

-- Roles used for owner change comparison cases
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_owner_from') THEN
          CREATE ROLE pgc_owner_from;
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_owner_to') THEN
          CREATE ROLE pgc_owner_to;
     END IF;
END;
$$;

-- Roles used for grant comparison cases
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_grant_reader') THEN
          CREATE ROLE pgc_grant_reader;
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_grant_writer') THEN
          CREATE ROLE pgc_grant_writer;
     END IF;
END;
$$;
-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS "pg_trgm" WITH SCHEMA public;
-- Issue #241: an extension that MOVES schema between FROM and TO. Its name,
-- not its schema, is its identity (pg_extension has a unique index on
-- extname), so the diff must relocate it with ALTER EXTENSION ... SET SCHEMA
-- rather than treat the move as an unrelated create plus drop.
CREATE EXTENSION IF NOT EXISTS "fuzzystrmatch" WITH SCHEMA shared_schema;

-- Custom types
CREATE TYPE test_schema.status_type AS ENUM ('active', 'inactive', 'pending');
CREATE TYPE test_schema.priority_type AS ENUM ('low', 'medium', 'high', 'critical');
CREATE TYPE shared_schema.address_type AS (
    street VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50)
);

-- Composite type
CREATE TYPE test_schema.user_profile AS (
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    birth_date DATE
);

-- Composite type migration case (FROM only): should be dropped when TO lacks it
CREATE TYPE test_schema.test_type_A AS (
    first_name_2 VARCHAR(50),
    last_name_2 VARCHAR(50)
);

-- Domain type
CREATE DOMAIN test_schema.positive_integer AS INTEGER CHECK (VALUE > 0);

-- Sequences
CREATE SEQUENCE test_schema.user_id_seq
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 10;

CREATE SEQUENCE test_schema.order_id_seq
    START WITH 5000
    INCREMENT BY 5
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 20;

CREATE SEQUENCE shared_schema.global_counter_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 999999999
    CACHE 1;

-- Sequence used to test MINVALUE raise: start/minvalue begin at 1 so that when Schema B
-- raises both to 10000000 the comparer must emit RESTART WITH to avoid:
--   ERROR: RESTART value (1) cannot be less than MINVALUE (10000000)
CREATE SEQUENCE test_schema.minvalue_raise_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 999999999
    CACHE 1
    NO CYCLE;

-- Tables
CREATE TABLE test_schema.users (
    id INTEGER PRIMARY KEY DEFAULT nextval('test_schema.user_id_seq'),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    profile test_schema.user_profile,
    status test_schema.status_type DEFAULT 'active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    login_count test_schema.positive_integer DEFAULT 0,
    is_admin BOOLEAN DEFAULT FALSE,
    metadata JSONB,
    avatar_url TEXT,
    last_login TIMESTAMP WITH TIME ZONE
);

CREATE TABLE test_schema.categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    parent_id INTEGER REFERENCES test_schema.categories(id),
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_schema.products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    category_id INTEGER NOT NULL REFERENCES test_schema.categories(id) ON DELETE CASCADE,
    sku VARCHAR(100) UNIQUE,
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    weight DECIMAL(8,3),
    dimensions JSONB,
    tags TEXT[],
    status test_schema.status_type DEFAULT 'active',
    priority test_schema.priority_type DEFAULT 'medium',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER REFERENCES test_schema.users(id)
);

CREATE TABLE test_schema.orders (
    id INTEGER PRIMARY KEY DEFAULT nextval('test_schema.order_id_seq'),
    user_id INTEGER NOT NULL REFERENCES test_schema.users(id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    status test_schema.status_type DEFAULT 'pending',
    shipping_address shared_schema.address_type,
    billing_address shared_schema.address_type,
    payment_method VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE test_schema.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES test_schema.orders(id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES test_schema.products(id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    total_price DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Generated column change test (FROM: plain column)
CREATE TABLE test_schema.generated_pricing (
    id SERIAL PRIMARY KEY,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total DECIMAL(12,2) NOT NULL
);

CREATE TABLE shared_schema.audit_logs (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_id VARCHAR(255),
    old_values JSONB,
    new_values JSONB,
    changed_by INTEGER,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ip_address INET,
    user_agent TEXT
);

-- Indexes
CREATE INDEX idx_users_username ON test_schema.users(username);
CREATE INDEX idx_users_email ON test_schema.users(email);
CREATE INDEX idx_users_status ON test_schema.users(status);
CREATE INDEX idx_users_created_at ON test_schema.users(created_at);
CREATE INDEX idx_users_metadata_gin ON test_schema.users USING GIN(metadata);

CREATE INDEX idx_products_name ON test_schema.products(name);
CREATE INDEX idx_products_category_id ON test_schema.products(category_id);
CREATE INDEX idx_products_sku ON test_schema.products(sku);
CREATE INDEX idx_products_status ON test_schema.products(status);
CREATE INDEX idx_products_price ON test_schema.products(price);
CREATE INDEX idx_products_tags_gin ON test_schema.products USING GIN(tags);
CREATE INDEX idx_products_dimensions_gin ON test_schema.products USING GIN(dimensions);

CREATE INDEX idx_orders_user_id ON test_schema.orders(user_id);
CREATE INDEX idx_orders_order_number ON test_schema.orders(order_number);
CREATE INDEX idx_orders_status ON test_schema.orders(status);
CREATE INDEX idx_orders_created_at ON test_schema.orders(created_at);

CREATE INDEX idx_order_items_order_id ON test_schema.order_items(order_id);
CREATE INDEX idx_order_items_product_id ON test_schema.order_items(product_id);

CREATE INDEX idx_audit_logs_table_name ON shared_schema.audit_logs(table_name);
CREATE INDEX idx_audit_logs_operation ON shared_schema.audit_logs(operation);
CREATE INDEX idx_audit_logs_changed_at ON shared_schema.audit_logs(changed_at);
CREATE INDEX idx_audit_logs_changed_by ON shared_schema.audit_logs(changed_by);
-- Standalone unique index (FROM) to validate detection of unique indexes not backed by constraints
CREATE UNIQUE INDEX idx_audit_logs_table_op_changed_at
    ON shared_schema.audit_logs(table_name, operation, changed_at);

-- Functions and procedures
CREATE OR REPLACE FUNCTION test_schema.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_schema.calculate_order_total(order_id_param INTEGER)
RETURNS DECIMAL(12,2) AS $$
DECLARE
    total DECIMAL(12,2);
BEGIN
    SELECT COALESCE(SUM(total_price), 0)
    INTO total
    FROM test_schema.order_items
    WHERE order_id = order_id_param;
    
    RETURN total;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_schema.get_user_order_count(user_id_param INTEGER)
RETURNS INTEGER AS $$
DECLARE
    order_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO order_count
    FROM test_schema.orders
    WHERE user_id = user_id_param;
    
    RETURN order_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION shared_schema.generate_order_number()
RETURNS VARCHAR(50) AS $$
DECLARE
    seq_val BIGINT;
    order_num VARCHAR(50);
BEGIN
    seq_val := nextval('shared_schema.global_counter_seq');
    order_num := 'ORD-' || LPAD(seq_val::TEXT, 8, '0');
    RETURN order_num;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION shared_schema.audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO shared_schema.audit_logs(table_name, operation, record_id, old_values, changed_at)
        VALUES (TG_TABLE_NAME, TG_OP, OLD.id::TEXT, row_to_json(OLD), CURRENT_TIMESTAMP);
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO shared_schema.audit_logs(table_name, operation, record_id, old_values, new_values, changed_at)
        VALUES (TG_TABLE_NAME, TG_OP, NEW.id::TEXT, row_to_json(OLD), row_to_json(NEW), CURRENT_TIMESTAMP);
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO shared_schema.audit_logs(table_name, operation, record_id, new_values, changed_at)
        VALUES (TG_TABLE_NAME, TG_OP, NEW.id::TEXT, row_to_json(NEW), CURRENT_TIMESTAMP);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger ordering scenario (FROM): function then trigger on same table
CREATE OR REPLACE FUNCTION test_schema.fn_order_from()
RETURNS TRIGGER AS $$
BEGIN
    NEW.description := COALESCE(NEW.description, 'from');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE test_schema.trigger_order_test (
    id SERIAL PRIMARY KEY,
    description TEXT
);

CREATE TRIGGER trg_order_from
BEFORE INSERT ON test_schema.trigger_order_test
FOR EACH ROW EXECUTE FUNCTION test_schema.fn_order_from();

-- Partition type-change scenario (FROM): partition key uses TEXT
CREATE TABLE test_schema.part_type_change_parent (
    tenant TEXT NOT NULL,
    id INTEGER NOT NULL,
    note TEXT,
    PRIMARY KEY (tenant, id)
) PARTITION BY LIST (tenant);

CREATE TABLE test_schema.part_type_change_child PARTITION OF test_schema.part_type_change_parent
FOR VALUES IN ('from');

-- Drop-order dependency scenario (exists only in FROM)
-- Custom type used by a table that is referenced by another table and has a trigger/function
CREATE TYPE test_schema.drop_status AS ENUM ('draft', 'published');

CREATE TABLE test_schema.drop_parent (
    id SERIAL,
    status test_schema.drop_status NOT NULL,
    PRIMARY KEY (id, status)
)
PARTITION BY LIST (status);

CREATE TABLE test_schema.drop_child PARTITION OF test_schema.drop_parent
FOR VALUES IN ('draft');

CREATE OR REPLACE FUNCTION test_schema.drop_fn()
RETURNS TRIGGER AS $$
BEGIN
    NEW.status := 'draft';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE test_schema.drop_orders (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    parent_status test_schema.drop_status NOT NULL DEFAULT 'draft',
    note TEXT,
    CONSTRAINT drop_orders_parent_fk FOREIGN KEY (parent_id, parent_status)
        REFERENCES test_schema.drop_parent(id, status)
);

CREATE TRIGGER trg_drop_orders
BEFORE INSERT ON test_schema.drop_orders
FOR EACH ROW EXECUTE FUNCTION test_schema.drop_fn();

CREATE OR REPLACE FUNCTION test_schema.get_users_by_status(p_status test_schema.status_type)
RETURNS TABLE(user_id integer, username varchar, email varchar)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY SELECT id, username, email FROM test_schema.users WHERE status = p_status;
END;
$$;

-- FROM-only routine depending on FROM-only type (priority_type)
CREATE OR REPLACE FUNCTION test_schema.get_products_by_priority(p_priority test_schema.priority_type)
RETURNS TABLE(product_id uuid, product_name varchar)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT p.id, p.name
    FROM test_schema.products p
    WHERE p.priority = p_priority;
END;
$$;

-- Function containing nested $$ to exercise custom dollar quoting
CREATE OR REPLACE FUNCTION test_schema.fn_dollar_from()
RETURNS text
LANGUAGE plpgsql
AS $b$
DECLARE
    inner_text text := $$inner-from$$;
BEGIN
    RETURN inner_text;
END;
$b$;

-- Procedure (PostgreSQL 11+)
CREATE OR REPLACE PROCEDURE test_schema.cleanup_old_orders(days_old INTEGER DEFAULT 365)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM test_schema.orders
    WHERE created_at < CURRENT_DATE - INTERVAL '1 day' * days_old
    AND status = 'delivered';

    COMMIT;
END;
$$;

-- Procedure with comma-in-string default (Issue #154 regression test)
CREATE OR REPLACE PROCEDURE test_schema.format_csv_line(
    p_value varchar,
    p_delimiter varchar DEFAULT ','
)
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '%', p_value || p_delimiter;
END;
$$;

-- Dollar-quoted body newline preservation test (identical in FROM and TO).
-- The procedure body contains intentional runs of 3+ consecutive blank lines.
-- When --use-comments=false is used, the newline-collapsing pass must NOT
-- alter content inside dollar-quoted strings; otherwise the hash changes and
-- the comparer reports a false positive diff.
CREATE OR REPLACE PROCEDURE test_schema.dollar_newline_test()
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'block 1';



    RAISE NOTICE 'block 2';




    RAISE NOTICE 'block 3';
END;
$$;

-- =============================================================================
-- Overloaded routine test: identical overloads in FROM and TO
-- Two procedures with the same name but different argument signatures.
-- Both overloads are identical in FROM and TO, so no diff should be emitted.
-- =============================================================================
CREATE OR REPLACE PROCEDURE test_schema.notify_event(pJobId uuid, pEventType varchar, pAttributes jsonb)
LANGUAGE plpgsql AS $$
BEGIN
    CALL test_schema.notify_event(pJobId, pEventType, null, pAttributes, null);
END;
$$;

CREATE OR REPLACE PROCEDURE test_schema.notify_event(pJobId uuid, pEventType varchar, pUserId varchar, pAttributes jsonb, pSessionSeed jsonb DEFAULT NULL)
LANGUAGE plpgsql AS $$
BEGIN
    -- full implementation placeholder
    RAISE NOTICE 'notify_event: job=%, type=%, user=%, attrs=%, seed=%', pJobId, pEventType, pUserId, pAttributes, pSessionSeed;
END;
$$;

-- Triggers
CREATE TRIGGER trigger_users_update_timestamp
    BEFORE UPDATE ON test_schema.users
    FOR EACH ROW
    EXECUTE FUNCTION test_schema.update_timestamp();

CREATE TRIGGER trigger_products_update_timestamp
    BEFORE UPDATE ON test_schema.products
    FOR EACH ROW
    EXECUTE FUNCTION test_schema.update_timestamp();

CREATE TRIGGER trigger_users_audit
    AFTER INSERT OR UPDATE OR DELETE ON test_schema.users
    FOR EACH ROW
    EXECUTE FUNCTION shared_schema.audit_trigger();

CREATE TRIGGER trigger_orders_audit
    AFTER INSERT OR UPDATE OR DELETE ON test_schema.orders
    FOR EACH ROW
    EXECUTE FUNCTION shared_schema.audit_trigger();

-- Views

-- MATERIALIZED VIEWS
-- Case 1: exists in FROM only → should be dropped
CREATE MATERIALIZED VIEW test_schema.from_only_mat AS
SELECT
    u.id,
    u.username,
    u.email,
    u.status
FROM test_schema.users u
WHERE u.is_admin = FALSE;

-- Case 2: exists in both, definition differs → should be dropped and recreated
CREATE MATERIALIZED VIEW test_schema.active_users_mat AS
SELECT
    u.id,
    u.username,
    u.email
FROM test_schema.users u
WHERE u.status = 'active';

-- Case 3: exists in both, unchanged → no change expected
CREATE MATERIALIZED VIEW test_schema.user_count_mat AS
SELECT
    status,
    COUNT(*) AS cnt
FROM test_schema.users
GROUP BY status;

CREATE VIEW test_schema.user_order_summary AS
SELECT 
    u.id,
    u.username,
    u.email,
    u.status as user_status,
    COUNT(o.id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent,
    MAX(o.created_at) as last_order_date
FROM test_schema.users u
LEFT JOIN test_schema.orders o ON u.id = o.user_id
GROUP BY u.id, u.username, u.email, u.status;

CREATE VIEW test_schema.product_inventory AS
SELECT 
    p.id,
    p.name,
    p.sku,
    p.price,
    p.stock_quantity,
    c.name as category_name,
    p.status,
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity <= 10 THEN 'Low Stock'
        ELSE 'In Stock'
    END as inventory_status
FROM test_schema.products p
JOIN test_schema.categories c ON p.category_id = c.id;

-- Constraints (additional ones)
ALTER TABLE test_schema.users ADD CONSTRAINT chk_users_email_format 
    CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

ALTER TABLE test_schema.products ADD CONSTRAINT chk_products_weight_positive 
    CHECK (weight IS NULL OR weight > 0);

ALTER TABLE test_schema.orders ADD CONSTRAINT chk_orders_dates 
    CHECK (shipped_at IS NULL OR shipped_at >= created_at);

ALTER TABLE test_schema.orders ADD CONSTRAINT chk_orders_delivery_dates 
    CHECK (delivered_at IS NULL OR delivered_at >= shipped_at);

-- Create some sample data
INSERT INTO test_schema.categories (name, description) VALUES 
('Electronics', 'Electronic devices and gadgets'),
('Books', 'Physical and digital books'),
('Clothing', 'Apparel and accessories'),
('Home & Garden', 'Home improvement and garden items');

-- Comments
COMMENT ON SCHEMA test_schema IS 'Main application schema for testing';
COMMENT ON TABLE test_schema.users IS 'User accounts and profiles';
COMMENT ON TABLE test_schema.products IS 'Product catalog';
COMMENT ON TABLE test_schema.orders IS 'Customer orders';
COMMENT ON COLUMN test_schema.users.metadata IS 'Additional user data in JSON format';
COMMENT ON COLUMN test_schema.products.dimensions IS 'Product dimensions (length, width, height) in JSON';
COMMENT ON TYPE test_schema.status_type IS 'User status values (active/inactive/pending)';
COMMENT ON DOMAIN test_schema.positive_integer IS 'Positive integer with no upper bound in FROM';
COMMENT ON SEQUENCE test_schema.user_id_seq IS 'User id sequence starting at 1000 (FROM)';
COMMENT ON VIEW test_schema.product_inventory IS 'Inventory overview with basic stock buckets (FROM)';
COMMENT ON MATERIALIZED VIEW test_schema.active_users_mat IS 'Active users snapshot (FROM version)';
COMMENT ON FUNCTION test_schema.get_users_by_status(test_schema.status_type) IS 'Returns users filtered by status (FROM)';
COMMENT ON FUNCTION test_schema.get_products_by_priority(test_schema.priority_type) IS 'FROM-only routine using FROM-only type to validate drop order';

-- Special characters test
CREATE TABLE test_schema."special$table" (
    "id" SERIAL PRIMARY KEY,
    "user@name" VARCHAR(50),
    "e-mail" VARCHAR(100)
);

CREATE TABLE test_schema."table with spaces" (
    "id" SERIAL PRIMARY KEY,
    "column with spaces" TEXT
);

-- Row-level security (FROM: one policy, simple predicate)
ALTER TABLE test_schema.users ENABLE ROW LEVEL SECURITY;
CREATE POLICY users_rls_select ON test_schema.users
    FOR SELECT
    TO public
    USING ((metadata ->> 'tenant_id') = current_setting('app.current_tenant'));

-- Complex Foreign Keys test
CREATE TABLE test_schema.composite_pk (
    part_one INT,
    part_two INT,
    data TEXT,
    PRIMARY KEY (part_one, part_two)
);

-- Function argument change test
CREATE OR REPLACE FUNCTION test_schema.calculate_tax(price numeric, tax_rate numeric)
RETURNS numeric
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN price * tax_rate;
END;
$$;

-- =============================================================================
-- Routine flags & special routine kinds test
-- =============================================================================

-- STABLE STRICT PARALLEL SAFE function (FROM)
-- TO changes flags to VOLATILE PARALLEL UNSAFE → flag-only diff
CREATE OR REPLACE FUNCTION test_schema.lookup_username(p_id integer)
RETURNS varchar
LANGUAGE sql
STABLE STRICT PARALLEL SAFE
AS $$
    SELECT username FROM test_schema.users WHERE id = p_id;
$$;

-- IMMUTABLE LEAKPROOF function (FROM only → should be dropped in TO)
CREATE OR REPLACE FUNCTION test_schema.normalize_email_from(raw text)
RETURNS text
LANGUAGE sql
IMMUTABLE LEAKPROOF PARALLEL SAFE
AS $$
    SELECT lower(trim(raw));
$$;

-- SECURITY DEFINER function (unchanged between FROM and TO)
CREATE OR REPLACE FUNCTION test_schema.get_secure_setting(key text)
RETURNS text
LANGUAGE plpgsql
STABLE SECURITY DEFINER
AS $$
BEGIN
    RETURN current_setting(key, true);
END;
$$;

-- SECURITY DEFINER procedure (FROM)
-- TO changes to SECURITY INVOKER (default) → flag-only diff
CREATE OR REPLACE PROCEDURE test_schema.admin_reset_counters()
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
    UPDATE test_schema.users SET login_count = 1;
END;
$$;

-- IMMUTABLE STRICT function (FROM)
-- TO changes to STABLE STRICT PARALLEL RESTRICTED → volatility & parallel change
CREATE OR REPLACE FUNCTION test_schema.tax_label(rate numeric)
RETURNS text
LANGUAGE sql
IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT CASE WHEN rate > 0.2 THEN 'high' ELSE 'low' END;
$$;

-- =============================================================================
-- Routine SET configuration parameters (proconfig) test
-- =============================================================================

-- Function with SET params (unchanged between FROM and TO)
CREATE OR REPLACE FUNCTION test_schema.get_session_user_safe()
RETURNS text
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path = 'public, pg_temp'
AS $$
BEGIN
    RETURN session_user;
END;
$$;

-- Function with SET params (FROM)
-- TO changes search_path and adds lock_timeout → config-only diff
CREATE OR REPLACE FUNCTION test_schema.secure_lookup(key text)
RETURNS text
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path = 'public'
AS $$
BEGIN
    RETURN current_setting(key, true);
END;
$$;

-- apply_secure_settings procedure does not exist in FROM → TO creates it with SET params

-- AGGREGATE function (unchanged between FROM and TO)
CREATE OR REPLACE FUNCTION test_schema.running_sum_sfunc(state bigint, val integer)
RETURNS bigint
LANGUAGE sql
IMMUTABLE STRICT
AS $$
    SELECT state + val::bigint;
$$;

CREATE AGGREGATE test_schema.running_sum(integer) (
    SFUNC = test_schema.running_sum_sfunc,
    STYPE = bigint,
    INITCOND = '0'
);

-- AGGREGATE function (FROM only → should be dropped in TO)
CREATE OR REPLACE FUNCTION test_schema.concat_agg_sfunc(state text, val text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE WHEN state = '' THEN val ELSE state || ',' || val END;
$$;

CREATE AGGREGATE test_schema.concat_agg(text) (
    SFUNC = test_schema.concat_agg_sfunc,
    STYPE = text,
    INITCOND = ''
);
COMMENT ON AGGREGATE test_schema.concat_agg(text) IS 'Simple string concatenation aggregate (FROM only)';

CREATE TABLE test_schema.composite_fk (
    id SERIAL PRIMARY KEY,
    ref_part_one INT,
    ref_part_two INT,
    CONSTRAINT fk_composite FOREIGN KEY (ref_part_one, ref_part_two) 
        REFERENCES test_schema.composite_pk (part_one, part_two)
);

-- Named primary key fixture (FROM side): table intentionally absent.
-- TO defines test_schema.table1 with CONSTRAINT pk_table1_id PRIMARY KEY (id).

-- Identity column test (Schema exists, table missing)
CREATE SCHEMA IF NOT EXISTS data;

-- Identity column update test (FROM)
CREATE TABLE test_schema.identity_update_test (
    id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    payload TEXT
);

-- Multi-level partition test (FROM: 2-level flat, events_2023 is a plain leaf partition)
CREATE TABLE data.events (
    id        BIGINT NOT NULL,
    year      INT    NOT NULL,
    region    TEXT   NOT NULL,
    payload   TEXT,
    CONSTRAINT events_pkey PRIMARY KEY (id, year, region)
) PARTITION BY RANGE (year);

CREATE TABLE data.events_2023
    PARTITION OF data.events
    FOR VALUES FROM (2023) TO (2024);

-- Sub-partition drop-order test (FROM only: 3-level hierarchy that is fully removed in TO)
CREATE TABLE data.sensor_data (
    id         BIGINT NOT NULL,
    sensor INT NOT NULL,
    region TEXT NOT NULL,
    CONSTRAINT sensor_data_pkey PRIMARY KEY (id, sensor, region)
) PARTITION BY LIST (region);

CREATE TABLE data.sensor_data_eu
    PARTITION OF data.sensor_data
    FOR VALUES IN ('eu')
    PARTITION BY RANGE (sensor);

CREATE TABLE data.sensor_data_eu_1
    PARTITION OF data.sensor_data_eu
    FOR VALUES FROM (1) TO (100);

-- Partition bound change test (FROM: value 'active')
CREATE TABLE data.partition_bound_test (
    id int,
    status text,
    CONSTRAINT partition_bound_test_pkey PRIMARY KEY (id, status)
) PARTITION BY LIST (status);

CREATE TABLE data.partition_bound_test_active PARTITION OF data.partition_bound_test FOR VALUES IN ('active');

-- =============================================================================
-- Partition DDL inheritance test: structural changes on a partitioned table
-- must only be applied to the parent table. Partitions inherit column add/drop,
-- alter (NOT NULL, DEFAULT), and non-FK constraint changes automatically.
-- The comparer must NOT emit these DDL statements for partition children.
--
-- FROM state:
--   parent: data.customers  (PARTITION BY RANGE (created_at))
--     columns: id (PK, NOT NULL), name (NOT NULL), email (nullable), legacy (nullable)
--     constraints: customers_pkey, chk_customers_name
--   partition: data.customers_2024  (inherits structure)
--   partition: data.customers_2025  (inherits structure)
-- =============================================================================
CREATE TABLE data.customers (
    id         BIGINT       NOT NULL,
    name       TEXT         NOT NULL,
    email      TEXT,
    legacy     TEXT,
    created_at DATE         NOT NULL,
    CONSTRAINT customers_pkey PRIMARY KEY (id, created_at),
    CONSTRAINT chk_customers_name CHECK (name <> '')
) PARTITION BY RANGE (created_at);

CREATE TABLE data.customers_2024
    PARTITION OF data.customers
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE data.customers_2025
    PARTITION OF data.customers
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX idx_customers_email ON data.customers (email);

-- =============================================================================
-- Partition index test: existing partitioned table with index gains a new partition
-- FROM has parent + index + one partition; TO adds a second partition.
-- The comparer must NOT emit explicit CREATE INDEX for the new partition
-- because PostgreSQL auto-creates inherited indexes on PARTITION OF.
-- =============================================================================
CREATE TABLE data.logs (
    id         BIGINT NOT NULL,
    created_at DATE   NOT NULL,
    message    TEXT,
    CONSTRAINT logs_pkey PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE data.logs_2024
    PARTITION OF data.logs
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE INDEX idx_logs_message ON data.logs (message);

-- =============================================================================
-- Issue #118: non-partition-key column type change on a partitioned table
-- Parent table partitioned by expense_date.  Changing column "amount" from
-- numeric(10,2) to numeric(15,4) must produce only ALTER COLUMN on the parent.
-- The partition must NOT be dropped+recreated.
-- =============================================================================
CREATE TABLE data.expenses (
    id           BIGINT       NOT NULL,
    expense_date DATE         NOT NULL,
    amount       NUMERIC(10,2),
    CONSTRAINT expenses_pkey PRIMARY KEY (id, expense_date)
) PARTITION BY RANGE (expense_date);

CREATE TABLE data.expenses_2024_01
    PARTITION OF data.expenses
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- SQL routines to test dependency handling
CREATE OR REPLACE FUNCTION test_schema.get_active_usernames_sql()
RETURNS TABLE(username varchar)
LANGUAGE sql
AS $$
    SELECT username FROM test_schema.users WHERE status = 'active';
$$;

CREATE OR REPLACE FUNCTION test_schema.product_price_with_tax_sql(p_product_id uuid, p_tax_rate numeric)
RETURNS numeric
LANGUAGE sql
AS $$
    SELECT p.price + test_schema.calculate_tax(p.price, p_tax_rate)
    FROM test_schema.products p
    WHERE p.id = p_product_id;
$$;

-- View ↔ Routine cross-dependency test (FROM side)
-- These objects are intentionally absent in schema_a.
-- Schema_b (TO) defines them so the generated migration script must create
-- them in the correct dependency order:
--   get_user_count()  →  v_user_stats  →  report_user_stats() / print_user_stats()

-- =============================================================================
-- Extension object exclusion test
-- =============================================================================
-- Extensions create their own objects (functions, types, operators, casts, etc.)
-- in the database. These extension-owned objects must NOT be included in the dump
-- or comparison output. Only the extensions themselves should be compared.
--
-- In FROM (schema_a):
--   - uuid-ossp  → creates uuid_generate_v4(), uuid_generate_v1(), etc.
--   - pgcrypto   → creates gen_random_uuid(), crypt(), gen_salt(), digest(), etc.
--   - pg_trgm    → creates similarity(), show_trgm(), gin_trgm_ops, etc.
--
-- Expected behavior:
--   - The dump should include the three extensions above.
--   - The dump must NOT include any functions, types, operators, or casts
--     created by those extensions (deptype = 'e' in pg_depend).
--   - User-defined objects that REFERENCE extension functions/types should
--     still be included in the dump (they are not owned by the extension).
-- =============================================================================

-- User-defined function that wraps an extension function (pgcrypto).
-- This function itself is NOT extension-owned, so it SHOULD appear in the dump.
-- But pgcrypto's own gen_random_bytes(), digest(), etc. should NOT.
CREATE OR REPLACE FUNCTION test_schema.generate_token(length INTEGER DEFAULT 32)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN encode(gen_random_bytes(length), 'hex');
END;
$$;

-- User-defined function that uses uuid-ossp extension function.
-- This should appear in the dump; uuid_generate_v4() itself should not.
CREATE OR REPLACE FUNCTION test_schema.new_entity_id()
RETURNS UUID
LANGUAGE sql
AS $$
    SELECT uuid_generate_v4();
$$;

-- =============================================================================
-- CHECK constraint string literal case preservation test
-- =============================================================================
-- Verifies that mixed-case string literals inside CHECK constraints are
-- not corrupted by lowercasing (lowercase_outside_literals bug fix).
-- chk_category_values is identical in both FROM and TO → must NOT produce
-- a false diff.
-- chk_priority_label differs between FROM and TO → real diff expected.
CREATE TABLE test_schema.check_literal_case_test (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    CONSTRAINT chk_category_values CHECK (category IN ('Electronics', 'Home & Garden', 'Books')),
    CONSTRAINT chk_priority_label CHECK (priority IN ('P1-Critical', 'P2-High', 'P3-Medium', 'P4-Low'))
);

-- Owner change coverage (FROM side)
ALTER SCHEMA test_schema OWNER TO pgc_owner_from;
ALTER TYPE test_schema.status_type OWNER TO pgc_owner_from;
ALTER DOMAIN test_schema.positive_integer OWNER TO pgc_owner_from;
ALTER SEQUENCE test_schema.user_id_seq OWNER TO pgc_owner_from;
ALTER TABLE test_schema.users OWNER TO pgc_owner_from;
ALTER FUNCTION test_schema.update_timestamp() OWNER TO pgc_owner_from;
ALTER VIEW test_schema.product_inventory OWNER TO pgc_owner_from;
ALTER MATERIALIZED VIEW test_schema.active_users_mat OWNER TO pgc_owner_from;

-- Routine dependency ordering test
-- These routines are intentionally absent in schema_a (FROM).
-- Schema_b (TO) defines four routines with inter-dependencies:
--   r_base_value   → no dependencies (leaf)
--   x_step_one     → depends on r_base_value
--   a_middle_layer → depends on x_step_one and r_base_value
--   z_final_report → depends on a_middle_layer
-- The generated migration script must create them in topological
-- (dependency) order, not alphabetical or insertion order.

-- Serial / bigserial / identity column test
-- These tables are intentionally absent in schema_a (FROM).
-- Schema_b (TO) defines them so the generated migration script must:
--   1. Skip creating separate sequences for serial/bigserial columns
--   2. Use serial/bigserial types in the CREATE TABLE statement
--   3. Correctly handle identity columns (skip sequence as well)

-- =============================================================================
-- PG14–18 feature test: Exclusion constraints (btree_gist)
-- =============================================================================
-- Tests exclusion constraint detection and comparison.
-- FROM: table with exclusion constraint.
-- TO: table modified (new column added), exclusion constraint unchanged.
-- Also: FROM has a second exclusion constraint table that is removed in TO.
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

CREATE TABLE test_schema.reservations (
    id SERIAL PRIMARY KEY,
    room_id INTEGER NOT NULL,
    during TSTZRANGE NOT NULL,
    EXCLUDE USING gist (room_id WITH =, during WITH &&)
);

-- Exclusion constraint table only in FROM (should be dropped in TO)
CREATE TABLE test_schema.shift_schedule (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    shift TSTZRANGE NOT NULL,
    EXCLUDE USING gist (employee_id WITH =, shift WITH &&)
);

-- =============================================================================
-- PG14–18 feature test: NULLS NOT DISTINCT (PG15+)
-- =============================================================================
-- FROM: standard UNIQUE constraint (allows multiple NULLs).
-- TO: UNIQUE NULLS NOT DISTINCT (only one NULL allowed).
CREATE TABLE test_schema.unique_nulls_test (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50),
    CONSTRAINT uq_unique_nulls_code UNIQUE (code)
);

-- =============================================================================
-- PG14–18 feature test: NO INHERIT constraint flag
-- =============================================================================
-- FROM: CHECK constraint without NO INHERIT.
-- TO: same CHECK constraint with NO INHERIT flag added.
ALTER TABLE test_schema.categories ADD CONSTRAINT chk_categories_sort_order
    CHECK (sort_order >= 0);

-- =============================================================================
-- PG14–18 feature test: Column STORAGE and COMPRESSION (PG14+)
-- =============================================================================
-- FROM: table with default STORAGE on text column, no explicit compression.
-- TO: STORAGE changed to EXTERNAL, compression set to lz4 (PG14+).
CREATE TABLE test_schema.storage_test (
    id SERIAL PRIMARY KEY,
    payload TEXT,
    blob BYTEA
);

-- =============================================================================
-- PG14–18 feature test: SECURITY INVOKER views (PG15+)
-- =============================================================================
-- FROM: simple view without security_invoker.
-- TO: same view definition but WITH (security_invoker = true).
CREATE VIEW test_schema.security_invoker_view AS
SELECT id, username, email
FROM test_schema.users
WHERE status = 'active';

-- =============================================================================
-- PG14–18 feature test: Range types
-- =============================================================================
-- FROM: range type with subtype_diff.
-- TO: range type without subtype_diff (triggers drop+recreate).
-- Also: FROM has an old_range type that is removed in TO.
-- Also: TO has a new int_range type that is created.
CREATE TYPE test_schema.float_range AS RANGE (
    SUBTYPE = float8,
    SUBTYPE_DIFF = float8mi
);

CREATE TYPE test_schema.old_range AS RANGE (
    SUBTYPE = int4
);

-- =============================================================================
-- PG14–18 feature test: Foreign tables (postgres_fdw)
-- =============================================================================
-- Tests foreign table creation, modification, and removal.
-- Requires postgres_fdw extension and a foreign server.
CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;

CREATE SERVER test_foreign_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', dbname 'postgres');

-- FROM: foreign table with basic columns and options.
-- TO: modified (column type change, new column, changed table options).
CREATE FOREIGN TABLE test_schema.foreign_users (
    id INTEGER NOT NULL,
    username VARCHAR(50),
    email VARCHAR(255)
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'users');

-- FROM-only foreign table (should be dropped in TO)
CREATE FOREIGN TABLE test_schema.foreign_logs (
    id BIGINT NOT NULL,
    message TEXT,
    created_at TIMESTAMP WITH TIME ZONE
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'logs');

-- =============================================================================
-- PG14–18 feature test: Extended statistics (PG10+)
-- =============================================================================
-- FROM: statistics with dependencies and ndistinct.
-- TO: modified (added mcv kind), plus a new statistics object.
-- Also: FROM has a stat that is removed in TO.
CREATE STATISTICS test_schema.stat_users_email_status (dependencies, ndistinct)
    ON email, status FROM test_schema.users;

-- FROM-only statistics (should be dropped in TO)
CREATE STATISTICS test_schema.stat_products_old (dependencies)
    ON name, status FROM test_schema.products;

-- =============================================================================
-- PG18 feature test: NOT ENFORCED constraints (PG18+)
-- =============================================================================
-- FROM: regular enforced CHECK constraint.
-- TO: same constraint with NOT ENFORCED flag.
-- Note: Requires PostgreSQL 18+. Comment out if testing on earlier versions.
ALTER TABLE test_schema.products ADD CONSTRAINT chk_products_sku_format
    CHECK (sku ~ '^[A-Z]{2,4}-[0-9]+$');

-- =============================================================================
-- PG18 feature test: Virtual generated columns (PG18+)
-- =============================================================================
-- FROM: plain computed column (not generated).
-- TO: GENERATED ALWAYS AS ... VIRTUAL column.
-- Note: Requires PostgreSQL 18+.
CREATE TABLE test_schema.virtual_gen_test (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    full_name VARCHAR(101) NOT NULL
);

-- =============================================================================
-- UNLOGGED tables test (FROM side)
-- =============================================================================
-- FROM: regular (logged) table
-- TO: UNLOGGED table
CREATE TABLE test_schema.unlogged_test (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- =============================================================================
-- Storage parameters (reloptions) test (FROM side)
-- =============================================================================
-- FROM: table with fillfactor=70
-- TO: table with fillfactor=90 and autovacuum_enabled=false
CREATE TABLE test_schema.storage_params_test (
    id SERIAL PRIMARY KEY,
    value TEXT
) WITH (fillfactor = 70);

-- =============================================================================
-- REPLICA IDENTITY test (FROM side)
-- =============================================================================
-- FROM: table with REPLICA IDENTITY DEFAULT
-- TO: table with REPLICA IDENTITY FULL
CREATE TABLE test_schema.replica_identity_test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- =============================================================================
-- FORCE ROW LEVEL SECURITY test (FROM side)
-- =============================================================================
-- FROM: table with RLS enabled but NOT forced
-- TO: table with RLS enabled AND forced
CREATE TABLE test_schema.force_rls_test (
    id SERIAL PRIMARY KEY,
    tenant_id INT NOT NULL,
    data TEXT
);
ALTER TABLE test_schema.force_rls_test ENABLE ROW LEVEL SECURITY;
CREATE POLICY force_rls_policy ON test_schema.force_rls_test
    FOR ALL
    USING (tenant_id = current_setting('app.tenant_id')::int);

-- =============================================================================
-- Classical inheritance test (FROM side)
-- =============================================================================
-- FROM: parent table with child
-- TO: parent table with modified child
CREATE TABLE test_schema.inheritance_parent (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
CREATE TABLE test_schema.inheritance_child (
    child_data TEXT
) INHERITS (test_schema.inheritance_parent);

-- =============================================================================
-- Typed table (OF type) test (FROM side)
-- =============================================================================
-- FROM: typed table of a specific type
-- TO: modified typed table
CREATE TYPE test_schema.address_type AS (
    street VARCHAR(200),
    city VARCHAR(100),
    zip VARCHAR(20)
);
CREATE TABLE test_schema.typed_table_test OF test_schema.address_type;

-- =============================================================================
-- Per-column statistics target test (FROM side)
-- =============================================================================
-- FROM: column with statistics target 100
-- TO: column with statistics target 500
CREATE TABLE test_schema.col_stats_test (
    id SERIAL PRIMARY KEY,
    searchable_data TEXT
);
ALTER TABLE test_schema.col_stats_test ALTER COLUMN searchable_data SET STATISTICS 100;

-- =============================================================================
-- Function COST/ROWS test (FROM side)
-- =============================================================================
-- FROM: function with COST 100 and ROWS 1000
-- TO: function with COST 200 and ROWS 500
CREATE OR REPLACE FUNCTION test_schema.cost_rows_test(n INT)
RETURNS SETOF INT
LANGUAGE sql
COST 100
ROWS 1000
AS $$
    SELECT generate_series(1, n);
$$;

-- =============================================================================
-- Grants comparison test (FROM side)
-- =============================================================================
-- These GRANT statements establish the FROM baseline for grant comparison.
-- Schema B (TO) has different grants to exercise all diff scenarios:
--   unchanged, modified, added, and removed grants.

-- Schema grants
GRANT USAGE ON SCHEMA test_schema TO pgc_grant_reader;

-- Table grants
GRANT SELECT ON test_schema.users TO pgc_grant_reader;
GRANT SELECT, INSERT, UPDATE ON test_schema.users TO pgc_grant_writer;
GRANT SELECT ON test_schema.products TO pgc_grant_reader;

-- Sequence grants
GRANT USAGE ON SEQUENCE test_schema.user_id_seq TO pgc_grant_reader;

-- View grants
GRANT SELECT ON test_schema.product_inventory TO pgc_grant_reader;

-- Function grants
GRANT EXECUTE ON FUNCTION test_schema.update_timestamp() TO pgc_grant_reader;

-- =============================================================================
-- Rules comparison test (FROM side)
-- =============================================================================
-- FROM: rule_products_no_delete — INSTEAD NOTHING on products DELETE (unchanged in TO)
-- FROM: rule_users_soft_delete  — INSTEAD UPDATE on users DELETE (FROM-only, dropped in TO)
-- TO:   rule_reviews_audit      — DO ALSO on reviews INSERT (TO-only, added in TO)
CREATE RULE rule_products_no_delete AS ON DELETE TO test_schema.products DO INSTEAD NOTHING;

CREATE RULE rule_users_soft_delete AS ON DELETE TO test_schema.users
    DO INSTEAD
        UPDATE test_schema.users SET status = 'inactive' WHERE id = OLD.id;

-- =============================================================================
-- Event triggers comparison test (FROM side)
-- =============================================================================
-- FROM: test_etrig_unchanged — fires on ddl_command_start (same in both schemas)
-- FROM: test_etrig_from_only — fires on ddl_command_end (FROM-only, dropped in TO)
-- TO:   test_etrig_to_only   — fires on ddl_command_end (TO-only, added in TO)
CREATE OR REPLACE FUNCTION test_schema.etrig_log_ddl() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'DDL event: % on %', TG_EVENT, TG_TAG;
END;
$$;

CREATE OR REPLACE FUNCTION test_schema.etrig_from_only_fn() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    NULL;
END;
$$;

CREATE EVENT TRIGGER test_etrig_unchanged
    ON ddl_command_start
    EXECUTE FUNCTION test_schema.etrig_log_ddl();

CREATE EVENT TRIGGER test_etrig_from_only
    ON ddl_command_end
    EXECUTE FUNCTION test_schema.etrig_from_only_fn();

-- =============================================================================
-- Collations comparison test (FROM side)
-- =============================================================================
-- FROM: test_coll_unchanged (kept in TO), test_coll_from_only (dropped in TO)
-- TO:   test_coll_unchanged (same),        test_coll_to_only (added in TO)
CREATE COLLATION test_schema.test_coll_unchanged (provider = libc, lc_collate = 'C', lc_ctype = 'C');
CREATE COLLATION test_schema.test_coll_from_only (provider = libc, lc_collate = 'C', lc_ctype = 'C');

-- =============================================================================
-- Text search dictionary comparison test (FROM side)
-- =============================================================================
-- FROM: test_dict_unchanged (kept), test_dict_from_only (dropped), test_dict_modified (stopwords removed in TO)
-- TO:   test_dict_unchanged (same),  test_dict_modified (no stopwords),  test_dict_to_only (added)
CREATE TEXT SEARCH DICTIONARY test_schema.test_dict_unchanged (
    TEMPLATE = pg_catalog.simple,
    STOPWORDS = english
);
CREATE TEXT SEARCH DICTIONARY test_schema.test_dict_from_only (
    TEMPLATE = pg_catalog.simple
);
CREATE TEXT SEARCH DICTIONARY test_schema.test_dict_modified (
    TEMPLATE = pg_catalog.simple,
    STOPWORDS = english
);

-- =============================================================================
-- Text search configuration comparison test (FROM side)
-- =============================================================================
-- FROM: test_tsconfig_unchanged (kept), test_tsconfig_from_only (dropped)
-- TO:   test_tsconfig_unchanged (same), test_tsconfig_to_only (added)
CREATE TEXT SEARCH CONFIGURATION test_schema.test_tsconfig_unchanged (
    PARSER = pg_catalog.default
);
CREATE TEXT SEARCH CONFIGURATION test_schema.test_tsconfig_from_only (
    PARSER = pg_catalog.default
);

-- =============================================================================
-- Cast comparison test (FROM side)
-- =============================================================================
-- FROM-only: test_type_A → text (dropped in TO because test_type_A is FROM-only)
-- Shared:    user_profile → text (unchanged in both schemas)
CREATE OR REPLACE FUNCTION test_schema.test_type_a_to_text(test_schema.test_type_A) RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT ($1).first_name_2 || ' ' || ($1).last_name_2; $$;

CREATE CAST (test_schema.test_type_A AS text)
    WITH FUNCTION test_schema.test_type_a_to_text(test_schema.test_type_A);

CREATE OR REPLACE FUNCTION test_schema.user_profile_to_text(test_schema.user_profile) RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT ($1).first_name || ' ' || ($1).last_name; $$;

CREATE CAST (test_schema.user_profile AS text)
    WITH FUNCTION test_schema.user_profile_to_text(test_schema.user_profile);

-- =============================================================================
-- Operator comparison test (FROM side)
-- =============================================================================
-- Unchanged: ~< (text, int) → bool — length($1) < $2 (same in both schemas)
-- FROM-only: ~> (text, int) → bool — length($1) > $2 (dropped in TO)
CREATE OR REPLACE FUNCTION test_schema.text_shorter_than(text, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT length($1) < $2; $$;

CREATE OPERATOR test_schema.~< (
    LEFTARG = text,
    RIGHTARG = integer,
    FUNCTION = test_schema.text_shorter_than
);

CREATE OR REPLACE FUNCTION test_schema.text_longer_than(text, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT length($1) > $2; $$;

CREATE OPERATOR test_schema.~> (
    LEFTARG = text,
    RIGHTARG = integer,
    FUNCTION = test_schema.text_longer_than
);

-- =============================================================================
-- Default privileges comparison test (FROM side)
-- =============================================================================
-- FROM: reader SELECT on new tables in test_schema (unchanged in TO)
-- FROM: writer INSERT only on new tables (modified to SELECT, INSERT, UPDATE in TO)
ALTER DEFAULT PRIVILEGES IN SCHEMA test_schema
    GRANT SELECT ON TABLES TO pgc_grant_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA test_schema
    GRANT INSERT ON TABLES TO pgc_grant_writer;

-- =============================================================================
-- Foreign server and user mapping comparison test (FROM side)
-- =============================================================================
-- (postgres_fdw extension and test_foreign_server already created earlier for foreign table tests)
-- test_server_modified: OPTIONS change in TO (host and dbname updated)
-- test_server_from_only: only in FROM (dropped in TO)
CREATE SERVER test_server_modified
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'server-host-a', dbname 'db_a', port '5432');

CREATE SERVER test_server_from_only
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'legacy-host', dbname 'legacy_db');

-- User mapping unchanged: PUBLIC on test_foreign_server (same in TO)
-- User mapping dropped:   PUBLIC on test_server_from_only (server removed in TO)
CREATE USER MAPPING FOR PUBLIC
    SERVER test_foreign_server
    OPTIONS (user 'readonly_user');

CREATE USER MAPPING FOR PUBLIC
    SERVER test_server_from_only
    OPTIONS (user 'legacy_user');

-- =============================================================================
-- Publications comparison test (FROM side)
-- =============================================================================
-- Requires wal_level = logical.
-- test_pub_from_only: dropped in TO; test_pub_unchanged: same in both schemas
CREATE PUBLICATION test_pub_from_only FOR TABLE test_schema.users;
CREATE PUBLICATION test_pub_unchanged FOR TABLE test_schema.users, test_schema.products;

-- =============================================================================
-- Issue #179 — DROP FUNCTION ... CASCADE recreate test (FROM side)
-- =============================================================================
-- Function `cascade_compute(integer)` returns INTEGER here. Schema B
-- changes the return type to BIGINT. The argument list (`integer`) is
-- unchanged, so the routine's signature in PostgreSQL terms is the
-- same on both sides — but PostgreSQL has no `ALTER FUNCTION` for
-- return types, so the comparer still emits DROP+CREATE. PostgreSQL
-- silently CASCADE-drops every object whose `pg_depend` row points at
-- the function: the CHECK constraint, the functional index, the
-- generated column, the column DEFAULT, and the RLS policy. The
-- comparer must re-emit each one (Phase 7 of
-- `compare_routines_and_views`) — otherwise the migrated database is
-- missing those objects until a second `pgc compare` run notices the
-- drift.
CREATE FUNCTION test_schema.cascade_compute(x integer)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT x * 2;
$$;

CREATE TABLE test_schema.cascade_items (
    id      serial  PRIMARY KEY,
    value   integer NOT NULL,
    -- DEFAULT expression depends on cascade_compute → cascaded by CASCADE
    def_col integer DEFAULT test_schema.cascade_compute(0)::integer,
    -- Generated column depends on cascade_compute → column dropped by CASCADE
    gen_col integer GENERATED ALWAYS AS (test_schema.cascade_compute(value)) STORED
);

-- CHECK constraint depends on cascade_compute → cascaded by CASCADE
ALTER TABLE test_schema.cascade_items
    ADD CONSTRAINT chk_cascade_compute CHECK (test_schema.cascade_compute(value) > 0);

-- Functional index depends on cascade_compute → cascaded by CASCADE
CREATE INDEX idx_cascade_compute
    ON test_schema.cascade_items (test_schema.cascade_compute(value));

-- RLS policy depends on cascade_compute → cascaded by CASCADE
ALTER TABLE test_schema.cascade_items ENABLE ROW LEVEL SECURITY;
CREATE POLICY pol_cascade_items ON test_schema.cascade_items
    USING (test_schema.cascade_compute(value) > 0);

-- =============================================================================
-- Issue #188 — Secondary dependents of CASCADE-dropped generated column
-- =============================================================================
-- Function `cascade_compute_v2(integer)` returns INTEGER here; Schema B
-- bumps the return type to BIGINT. The CASCADE drop of the function
-- removes the generated column `gen_total` AND every object PostgreSQL
-- attached to that column via `pg_depend` — even objects whose own
-- definition NEVER NAMES the function. Phase 7's text scanner can't
-- find those secondary dependents; the comparer must walk the
-- column-dependent graph harvested from `pg_depend` at dump time and
-- re-emit them.
CREATE FUNCTION test_schema.cascade_compute_v2(x integer)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT x + 1;
$$;

CREATE TABLE test_schema.cascade_col_dep_items (
    id        serial  PRIMARY KEY,
    value     integer NOT NULL,
    -- Generated column depends on cascade_compute_v2 → CASCADE drops it.
    gen_total integer GENERATED ALWAYS AS (test_schema.cascade_compute_v2(value)) STORED
);

-- Plain index on the generated column — does NOT name the function.
-- When the function drop cascades to gen_total, this index goes with
-- it. Phase 7's routine-text scan can't find it; the pg_depend graph
-- must.
CREATE INDEX idx_cascade_col_dep_gen_total
    ON test_schema.cascade_col_dep_items (gen_total);

-- CHECK constraint on the generated column — names only the column.
-- Same dependency-chain story as the index above.
ALTER TABLE test_schema.cascade_col_dep_items
    ADD CONSTRAINT chk_cascade_col_dep_gen_total CHECK (gen_total >= 0);

-- UNIQUE constraint on the generated column — required so a foreign
-- key on a separate table can target gen_total. Its backing index
-- shares the constraint name and is recorded as a separate pg_depend
-- edge; the comparer dedup must skip emitting the backing index
-- because the ADD CONSTRAINT ... UNIQUE form recreates it implicitly.
ALTER TABLE test_schema.cascade_col_dep_items
    ADD CONSTRAINT uq_cascade_col_dep_gen_total UNIQUE (gen_total);

-- Child table whose FK references gen_total on the parent. The FK's
-- `conrelid` points at this child, but its pg_depend row anchors on
-- the parent column — the asymmetric case PR #196 review called out.
-- When CASCADE drops gen_total via the function rewrite, this FK is
-- silently dropped too; the column-dependent graph must restore it,
-- AFTER the UNIQUE target above is back (two-pass ordering).
CREATE TABLE test_schema.cascade_col_dep_children (
    id        serial  PRIMARY KEY,
    ref_total integer
);

ALTER TABLE test_schema.cascade_col_dep_children
    ADD CONSTRAINT fk_cascade_col_dep_children_ref_total
        FOREIGN KEY (ref_total)
        REFERENCES test_schema.cascade_col_dep_items (gen_total);

-- =============================================================================
-- Issue #180 — FK-ordered SET LOGGED/UNLOGGED + redundant owned-sequence ALTER
-- =============================================================================
-- FROM has three LOGGED tables connected by a foreign-key chain
-- (child -> parent -> grandparent). Schema B converts every table in
-- the chain to UNLOGGED. The migration script must emit
-- `ALTER TABLE ... SET UNLOGGED` in FK-leaf-first order
-- (child -> parent -> grandparent); alphabetical order — which the
-- comparer used before this fix — fails with:
--   ERROR: could not change table "grandparent" to unlogged because
--   it references logged table "parent"
--
-- The reverse direction (UNLOGGED -> LOGGED) requires the opposite
-- order (roots first); both directions are exercised by the comparer's
-- `Comparer::emit_persistence_changes` phase, which sorts each
-- direction topologically over the FK adjacency derived from
-- `pg_get_constraintdef`.
--
-- The serial PKs auto-create owned sequences. PostgreSQL propagates
-- the table's `ALTER TABLE SET UNLOGGED` to every owned sequence
-- automatically, so the comparer must NOT emit an explicit
-- `ALTER SEQUENCE ... SET UNLOGGED` for them — when the only diff is
-- `is_unlogged` and the owning table is also flipping, the entire
-- ALTER SEQUENCE block is suppressed (issue #180).
CREATE SCHEMA IF NOT EXISTS test_order;

CREATE TABLE test_order.grandparent (
    id serial PRIMARY KEY
);

CREATE TABLE test_order.parent (
    id serial PRIMARY KEY,
    grandparent_id integer REFERENCES test_order.grandparent(id)
);

CREATE TABLE test_order.child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES test_order.parent(id)
);

-- =============================================================================
-- Issue #191 — Persistence-ordering with mutual FK cycle
-- =============================================================================
-- Two tables with mutual deferrable FKs: `cycle_a → cycle_b` and
-- `cycle_b → cycle_a`. PostgreSQL only permits this shape when both
-- FKs are DEFERRABLE INITIALLY DEFERRED (otherwise the first INSERT
-- on either side would have nothing to reference). The cycle has NO
-- valid SET LOGGED|UNLOGGED order: `SET UNLOGGED cycle_a` requires
-- cycle_b to already be UNLOGGED, and vice versa. Pre-fix the
-- comparer's `kahn_toposort` silently fell back to alphabetical for
-- the cyclic remainder, the migration emitted the SETs in that order,
-- and PostgreSQL rejected the second SET at apply time.
--
-- The fix detects the cycle, drops both FKs BEFORE the SET block,
-- runs both SETs (order no longer matters), and re-emits the FKs
-- from their TO definitions AFTER. Schema B flips both tables to
-- UNLOGGED so this fixture exercises the drop+SET+re-add pattern
-- end-to-end against a real `pg_get_constraintdef` output.
CREATE TABLE test_order.cycle_a (
    id serial PRIMARY KEY,
    b_id integer
);

CREATE TABLE test_order.cycle_b (
    id serial PRIMARY KEY,
    a_id integer
);

ALTER TABLE test_order.cycle_a
    ADD CONSTRAINT cycle_a_b_id_fkey
    FOREIGN KEY (b_id) REFERENCES test_order.cycle_b(id)
    DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE test_order.cycle_b
    ADD CONSTRAINT cycle_b_a_id_fkey
    FOREIGN KEY (a_id) REFERENCES test_order.cycle_a(id)
    DEFERRABLE INITIALLY DEFERRED;

-- =============================================================================
-- Issue #190 — Persistence-ordering FK adjacency with unqualified FK targets
-- =============================================================================
-- Companion case to the test_order chain above, but anchored in `public`.
-- The role pgc connects as inherits the default search_path
-- (`"$user", public`), so `pg_get_constraintdef` omits the schema
-- qualifier on FK targets in `public` — the deparsed text is
-- `REFERENCES persistence_chain_parent(id)`, not
-- `REFERENCES public.persistence_chain_parent(id)`. Pre-fix the comparer's
-- `parse_fk_referenced_table` returned None for the unqualified form, the
-- FK edge was missing from the persistence-flip adjacency, and the SET
-- ordering fell back to alphabetical — which PostgreSQL rejects with
-- `could not change table "persistence_chain_parent" to unlogged because
-- it is referenced by ...`. The CI gap that hid this bug was that every
-- existing FK-chain fixture lives in an explicit `test_order` schema,
-- where `pg_get_constraintdef` qualifies the target.
--
-- Schema B mirrors this pair but flipped to UNLOGGED; the migration's SET
-- UNLOGGED must emit `child` before `parent`.
CREATE TABLE public.persistence_chain_parent (
    id serial PRIMARY KEY
);

CREATE TABLE public.persistence_chain_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES public.persistence_chain_parent(id)
);

-- =============================================================================
-- Auto-named NOT NULL collision fixture
-- =============================================================================
-- Issue: PG18 emits NOT NULL as named constraints (`{table}_{col}_not_null`).
-- When two tables in the same schema generate the same base name (e.g.
-- `nn_coll_a.b_c` and `nn_coll_a_b.c` both produce
-- `nn_coll_a_b_c_not_null`), PostgreSQL appends numeric digits directly
-- to the `not_null` label (NOT with an underscore) to disambiguate.
-- `ChooseConstraintName` is deterministic for a given CREATE TABLE order,
-- but the order varies between databases in the wild (migrations replay
-- in different sequences, restores re-emit DDL in catalog order, etc.),
-- so the comparer must treat such names as semantically equivalent.
--
-- IMPORTANT — load-bearing asymmetry: the CREATE TABLE order in
-- schema_a.sql is the REVERSE of schema_b.sql. Because PG probes
-- `nn_coll_a_b_c_not_null` first and only appends `1` on collision, the
-- two databases end up with the suffix attached to *different* tables:
--
--   Schema A (nn_coll_a created first):
--     test_schema.nn_coll_a   .b_c  →  nn_coll_a_b_c_not_null
--     test_schema.nn_coll_a_b .c    →  nn_coll_a_b_c_not_null1
--
--   Schema B (nn_coll_a_b created first):
--     test_schema.nn_coll_a_b .c    →  nn_coll_a_b_c_not_null
--     test_schema.nn_coll_a   .b_c  →  nn_coll_a_b_c_not_null1
--
-- Do NOT "clean this up" by aligning the CREATE order — the asymmetry
-- IS the test. With the fix in place the comparer must still produce
-- ZERO diffs for both tables; reverting to symmetric order would make
-- PG assign identical names in both DBs and silently regress this
-- regression test back to a no-op.
CREATE TABLE test_schema.nn_coll_a (
    id serial PRIMARY KEY,
    b_c integer NOT NULL
);

CREATE TABLE test_schema.nn_coll_a_b (
    id serial PRIMARY KEY,
    c integer NOT NULL
);

-- =============================================================================
-- Regression: views that reference only functions (issue #219)
-- =============================================================================
-- A view whose body calls only a function has no rows in
-- information_schema.view_table_usage, so it used to be dropped from the dump
-- entirely and any change to it was invisible to `compare`.
--
-- FROM: view body selects straight from the function.
-- TO:   body gains a WHERE clause, so the round-trip must emit a replacement.
CREATE FUNCTION test_schema.fn_only_view_source()
RETURNS TABLE(id integer, val text)
LANGUAGE sql AS $$ SELECT 1, 'hello' $$;

CREATE VIEW test_schema.vw_from_function AS
SELECT id, val FROM test_schema.fn_only_view_source();

-- Same shape, but the view is referenced by another view: the dependent view
-- must still be ordered correctly even though neither touches a table.
CREATE VIEW test_schema.vw_on_function_view AS
SELECT id FROM test_schema.vw_from_function;

-- A DO INSTEAD rule on the view writes to a table the view definition never
-- reads, so table_relation must stay limited to what the definition selects
-- from (vw_rule_base): rules are dumped separately, and crediting their targets
-- to the view would invent dependency edges for drop ordering.
--
-- vw_rule_base is deliberately identical in both schemas. A view whose source
-- table changes is dropped and recreated, and the recreate does not restore
-- rules on the view, so anchoring this fixture on a changing table would make
-- the round-trip re-emit the rule forever for reasons unrelated to the view
-- dependency walk this case exists to cover.
CREATE TABLE test_schema.vw_rule_base (
    id integer PRIMARY KEY,
    name text
);

CREATE TABLE test_schema.vw_rule_audit (
    id integer,
    note text
);

CREATE VIEW test_schema.vw_with_rule AS
SELECT id, name FROM test_schema.vw_rule_base;

CREATE RULE vw_with_rule_ins AS ON INSERT TO test_schema.vw_with_rule
    DO INSTEAD INSERT INTO test_schema.vw_rule_audit(id, note)
    VALUES (NEW.id, NEW.name);

-- =============================================================================
-- Regression: materialized view created WITH NO DATA (issue #220)
-- =============================================================================
-- WITH NO DATA is not part of the stored definition — it survives only as
-- pg_class.relispopulated — so a diff that recreates the view from its definition
-- alone silently populates it. The base table is identical in both schemas to keep
-- the case about the view itself.
--
-- FROM: base table only, no materialized view.
-- TO:   mv_no_data added WITH NO DATA; the diff must carry the clause through.
CREATE TABLE test_schema.mv_nodata_base (
    id integer PRIMARY KEY,
    val text,
    amount numeric
);

-- A populated materialized view over the same table, unchanged in both schemas:
-- it must never acquire a WITH NO DATA clause.
CREATE MATERIALIZED VIEW test_schema.mv_with_data AS
SELECT id, val FROM test_schema.mv_nodata_base;

-- WITH CHECK OPTION is emitted as a trailing clause like WITH NO DATA, so it hits
-- the same trap of landing after the definition's terminating semicolon. The option
-- changes local -> cascaded in TO, which exercises the CREATE OR REPLACE path.
CREATE VIEW test_schema.vw_check_opt AS
SELECT id, val FROM test_schema.mv_nodata_base WHERE amount > 0
WITH LOCAL CHECK OPTION;

-- =============================================================================
-- Regression: reloptions on partitions of a table recreated as partitioned (#216)
-- =============================================================================
-- Turning a regular table into a partitioned one has to go through DROP + CREATE,
-- and each partition is then created with CREATE TABLE ... PARTITION OF. That
-- statement takes WITH (...) like any other CREATE TABLE; emitting it without the
-- clause leaves every partition with no reloptions, so the migration only reaches
-- the target schema on a second pass.
--
-- FROM: plain regular table, no reloptions.
-- TO:   partitioned, with reloptions on both partitions. The partitioned parent
--       carries none of its own: PostgreSQL rejects storage parameters on a
--       partitioned table ("cannot specify storage parameters for a partitioned
--       table"), so the reloptions live only on the leaf partitions.
CREATE TABLE test_schema.reloptions_to_partitioned (
    id INTEGER,
    created_at TIMESTAMPTZ NOT NULL
);

-- The inverse direction, which already worked: partitioned -> regular, where the
-- reloptions ride along in the plain CREATE TABLE's WITH (...) clause.
CREATE TABLE test_schema.reloptions_to_regular (
    id INTEGER,
    created_at TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE test_schema.reloptions_to_regular_default
    PARTITION OF test_schema.reloptions_to_regular DEFAULT;

-- =============================================================================
-- Regression: SET config values must round-trip (issue #217)
-- =============================================================================
-- A GUC_LIST_QUOTE parameter (search_path, temp_tablespaces) stores its value in
-- proconfig as a plain comma-separated list. Emitting it as a single-quoted literal
-- turns `test_schema, pg_temp` into one schema literally named "test_schema, pg_temp",
-- which re-stores differently from the source, so every compare re-emits the routine
-- forever. The existing get_session_user_safe / secure_lookup fixtures use a QUOTED
-- single element ('public, pg_temp'), which round-trips either way and so never
-- exercised this — the unquoted multi-element list below is what does.
--
-- FROM: procedure with no SET config.
-- TO:   gains SECURITY DEFINER and three SET clauses, one of them an unquoted list.
CREATE OR REPLACE PROCEDURE test_schema.set_config_roundtrip(p_id integer)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'id=%', p_id;
END;
$$;

-- =============================================================================
-- Regression: inline NOT NULL must not become a named ADD/DROP CONSTRAINT (#218)
-- =============================================================================
-- PostgreSQL 18 records a column's inline NOT NULL in pg_constraint under the
-- auto-generated name {table}_{column}_not_null (contype='n'); PG14–17 keep it only
-- in pg_attribute.attnotnull. When a NOT NULL column is dropped, or a nullable column
-- gains NOT NULL, the diff must use only the column-level `set/drop not null` (or the
-- inline `not null` of a dropped column), never `ADD/DROP CONSTRAINT` for the invented
-- name: the DROP errors on PG14–17 ("constraint does not exist") and the ADD is
-- PG18-only syntax that also leaves a constraint the source never declared, looping.
--
-- FROM: nn_col has inline NOT NULL (dropped in TO); null_col is nullable (NOT NULL in TO).
CREATE TABLE test_schema.notnull_recreate (
    id       integer,
    nn_col   varchar(10) NOT NULL,
    null_col varchar(10)
);

-- =============================================================================
-- Regression: index on a partitioned parent must be created valid (#223)
-- =============================================================================
-- pg_get_indexdef renders a partitioned parent's index with `ON ONLY`, which builds
-- only the metadata index on the parent and leaves it indisvalid=false until each
-- partition's index is attached. The default (non-production) output has no attach
-- step, so it must emit a plain `CREATE INDEX ... ON` — PostgreSQL then builds and
-- attaches the partition indexes itself. The parent table is identical in both
-- schemas; only the index is added in TO. (The round-2 diff cannot catch this on its
-- own — an invalid index still round-trips empty — so the integration job also
-- asserts indisvalid after applying the migration.)
--
-- FROM: partitioned table with two partitions, no index on partidx_value.
CREATE TABLE test_schema.partidx (
    id      integer,
    value   text,
    bucket  date
) PARTITION BY RANGE (bucket);

CREATE TABLE test_schema.partidx_2024 PARTITION OF test_schema.partidx
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE test_schema.partidx_2025 PARTITION OF test_schema.partidx
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- =============================================================================
-- Regression: non-idempotent IN-list deparsing must not loop (issue #226)
-- =============================================================================
-- PostgreSQL deparses `col IN ('FOO','BAR')` as an array-level cast
-- `(ARRAY[...])::text[]` on first render, then as an element-level cast
-- `ARRAY[(...)::text, ...]` when that form is re-parsed. Both are equivalent but
-- differ textually, so a matview/partial-index/generated-column expression carrying
-- an IN-list re-emits DROP+CREATE on every run unless the definitions are
-- canonicalized before comparison.
--
-- The base table is present in both schemas; the matview and partial index that
-- carry the IN-list exist only in TO, so the diff creates them, applying re-parses
-- the expression into the element-level form, and the second diff must still be
-- empty.
CREATE TABLE test_schema.innorm (
    id          integer PRIMARY KEY,
    label       character varying(50),
    action_type character varying(50),
    device      jsonb
);

-- Companion to the innorm case (#226): a CHECK IN-list whose literals carry an
-- explicit varchar typmod. The typmod spelling survives both pretty
-- pg_get_constraintdef renderings and flips between the array-level and
-- element-level cast forms exactly like the bare-varchar IN-list, so the
-- canonicalizer must converge it too or this constraint re-emits DROP+ADD forever.
-- FROM: table only; TO adds the constraint, so the migration creates it and the
-- re-parsed rendering must still compare equal.
CREATE TABLE test_schema.innorm_typmod (
    id   integer PRIMARY KEY,
    code character varying(10)
);

-- =============================================================================
-- Regression: incompatible view column change needs DROP+CREATE (issue #227)
-- =============================================================================
-- CREATE OR REPLACE VIEW only allows appending columns at the end; inserting a
-- column in the middle (as TO does with `kind`) is rejected with "cannot change
-- name of view column". The diff must drop and recreate the view instead — and
-- because DROP VIEW runs without CASCADE, the unchanged dependent view
-- v227_dep must be dropped first and recreated after. The base table is
-- identical in both schemas so only the view definitions drive the diff.
CREATE TABLE test_schema.v227_item (
    id         integer PRIMARY KEY,
    status     text,
    kind       text,
    profile_id integer
);

CREATE VIEW test_schema.v227_base AS
SELECT id, status, profile_id FROM test_schema.v227_item;

CREATE VIEW test_schema.v227_dep AS
SELECT id, status FROM test_schema.v227_base;

-- =============================================================================
-- CASCADE recreation of an index on a partitioned parent (PR #234 review)
-- =============================================================================
-- cascade_part_fn's return type changes in TO, so its drop CASCADEs through the
-- functional index on the partitioned parent; Phase 7 must recreate that index
-- valid. In production mode the recreate must use the ON ONLY + per-partition
-- CONCURRENTLY/ATTACH sequence rather than a blocking plain CREATE INDEX; in
-- default mode a plain CREATE INDEX builds and attaches everything itself. The
-- integration job's indisvalid assertion guards the result.
CREATE FUNCTION test_schema.cascade_part_fn(text)
RETURNS integer IMMUTABLE LANGUAGE sql AS $$ SELECT length($1) $$;

CREATE TABLE test_schema.cascade_part (
    id integer,
    s  text,
    d  date NOT NULL
) PARTITION BY RANGE (d);

CREATE TABLE test_schema.cascade_part_2024 PARTITION OF test_schema.cascade_part
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE test_schema.cascade_part_2025 PARTITION OF test_schema.cascade_part
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX ix_cascade_part_fn ON test_schema.cascade_part (test_schema.cascade_part_fn(s));

-- =============================================================================
-- Regression: indexes on a materialized view (issue #235)
-- =============================================================================
-- A materialized view can be indexed, but `pg_indexes` rows for one were only
-- ever distributed into the dump's *table* list, so they matched nothing and
-- were dropped on the floor. Nothing then re-emitted them: a materialized view
-- is always dropped and recreated when its definition changes, and
-- DROP MATERIALIZED VIEW takes the indexes with it, so the migration silently
-- left the view unindexed. Neither dump carried the indexes, so the second diff
-- was empty and the loss went unreported.
--
-- mv235_recreated: definition changes in TO, so the view is dropped and rebuilt
-- and every index has to be recreated with it.
-- mv235_stable:    definition identical in both, so the view survives and its
--                  indexes are reconciled in place (added / removed / redefined
--                  / re-commented) — index changes stay out of the view hash so
--                  that adding an index never forces a full rebuild.
-- mv235_new:       TO-only, so its index rides along with the initial CREATE.
CREATE TABLE test_schema.mv235_base (
    id     integer PRIMARY KEY,
    label  text,
    amount numeric,
    active boolean
);

CREATE MATERIALIZED VIEW test_schema.mv235_recreated AS
SELECT id, label FROM test_schema.mv235_base;

CREATE UNIQUE INDEX ix_mv235_recreated_id ON test_schema.mv235_recreated (id);
CREATE INDEX ix_mv235_recreated_partial ON test_schema.mv235_recreated (label)
    WHERE label IS NOT NULL;
COMMENT ON INDEX test_schema.ix_mv235_recreated_partial IS 'partial index on a matview';

CREATE MATERIALIZED VIEW test_schema.mv235_stable AS
SELECT id, label, amount FROM test_schema.mv235_base;

-- Dropped in TO.
CREATE INDEX ix_mv235_stable_drop ON test_schema.mv235_stable (amount);
-- Definition changes in TO (ASC -> DESC): drop + rebuild, not a comment-only edit.
CREATE INDEX ix_mv235_stable_redef ON test_schema.mv235_stable (label);
-- Only the comment changes in TO: the index itself must be left alone.
CREATE INDEX ix_mv235_stable_cmt ON test_schema.mv235_stable (id, amount);
COMMENT ON INDEX test_schema.ix_mv235_stable_cmt IS 'comment before';

-- ─────────────────────────────────────────────────────────────────────
-- Issue #242: a table change must only drag in the views that actually
-- read the affected columns.
--
-- `i242_orders` gains an unrelated column in TO. Neither view below reads
-- it, so PostgreSQL is happy to add it with both in place and the diff
-- must leave them alone — no DROP MATERIALIZED VIEW, no rebuild, no index
-- rebuild. `i242_mv_touched` is the control: it reads `status`, which is
-- retyped in TO, so it must still be dropped and recreated.
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE test_schema.i242_orders (
    id         integer PRIMARY KEY,
    status     text NOT NULL,
    unrelated  integer
);

CREATE MATERIALIZED VIEW test_schema.i242_mv_untouched AS
SELECT id, count(*) AS cnt FROM test_schema.i242_orders GROUP BY id;

CREATE INDEX ix_i242_mv_untouched_id ON test_schema.i242_mv_untouched (id);

CREATE VIEW test_schema.i242_v_untouched AS
SELECT id FROM test_schema.i242_orders;

CREATE MATERIALIZED VIEW test_schema.i242_mv_touched AS
SELECT status, count(*) AS cnt FROM test_schema.i242_orders GROUP BY status;

-- ─────────────────────────────────────────────────────────────────────
-- Issue #243: the identity drop has to lead the column's other ALTERs.
--
-- An identity column is held NOT NULL, is forbidden a default, and is
-- restricted to an integer type, and PostgreSQL enforces each of those at
-- the moment of the ALTER. So all three columns below drop their identity
-- in TO while also relaxing one of the three, and each fails if the
-- statements come out in source order instead of identity-first.
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE test_schema.i243_identity_drop (
    goes_nullable   bigint GENERATED BY DEFAULT AS IDENTITY,
    gains_default   bigint GENERATED BY DEFAULT AS IDENTITY,
    leaves_integer  bigint GENERATED BY DEFAULT AS IDENTITY,
    label           text
);

-- ═════════════════════════════════════════════════════════════════════
-- PostgreSQL 19 feature tests (pgc v1.1.0), FROM side.
-- PG19-only statements fail harmlessly on PG14-18: CI applies this file
-- with ON_ERROR_STOP=0 and the round trip must still close there.
-- Keep every statement parenthesis-balanced — psql does not end a
-- statement at ';' inside an open parenthesis, so one broken statement
-- would swallow everything after it.
-- ═════════════════════════════════════════════════════════════════════

-- ==== BEGIN pg19:publications — publications: FOR ALL SEQUENCES, FOR ALL TABLES EXCEPT, TABLES IN SCHEMA, row filters, column lists, options ====
-- =============================================================================
-- PG19 feature test: publication membership and options (FROM side)
-- =============================================================================
-- Requires wal_level = logical. Statements a server cannot parse fail
-- harmlessly under ON_ERROR_STOP=0 and leave that publication out of this
-- side: column lists, row filters and TABLES IN SCHEMA need PG15+,
-- publish_generated_columns PG18+, FOR ALL SEQUENCES and EXCEPT PG19+.
-- Every case is described next to it; data/test/README.md section 44 has
-- the matrix. The tables are named tbl_pg19_pub_*, not pg19_pub_*: pgc's
-- table query filters `tablename not like 'pg_%'`, which (`_` being a
-- wildcard) hides every table whose name starts with "pg".
CREATE TABLE test_schema.tbl_pg19_pub_items (id integer PRIMARY KEY, qty integer, note text);
CREATE TABLE test_schema.tbl_pg19_pub_orders (id integer PRIMARY KEY, amount numeric);
CREATE TABLE test_schema."tbl_pg19_pub_Mixed" (id integer PRIMARY KEY);
CREATE TABLE test_schema.tbl_pg19_pub_events (id integer, kind text NOT NULL) PARTITION BY LIST (kind);
CREATE TABLE test_schema.tbl_pg19_pub_events_a PARTITION OF test_schema.tbl_pg19_pub_events FOR VALUES IN ('a');
-- The partition key changes in TO, so the migration drops and recreates this
-- table, and its publication rows (member and EXCEPT) go with it.
CREATE TABLE test_schema.tbl_pg19_pub_rebuilt (id integer NOT NULL) PARTITION BY RANGE (id);
-- Absent from TO: the migration drops it while it is still a member.
CREATE TABLE test_schema.tbl_pg19_pub_dropped (id integer PRIMARY KEY);
-- Classical inheritance. Each member is one pg_publication_rel row, so the
-- parent is written ONLY: SET TABLE without it would add the child as well.
CREATE TABLE test_schema.tbl_pg19_pub_parent (id integer);
CREATE TABLE test_schema.tbl_pg19_pub_child () INHERITS (test_schema.tbl_pg19_pub_parent);

-- All versions. No ALTER leaves FOR ALL TABLES: drop+recreate (these used
-- to emit a failing SET TABLE on every run, or nothing).
CREATE PUBLICATION pg19_pub_all_to_list FOR ALL TABLES;
CREATE PUBLICATION pg19_pub_all_to_empty FOR ALL TABLES;
-- List -> empty: DROP TABLE ONLY for the members still there (not _dropped).
CREATE PUBLICATION pg19_pub_list_to_empty FOR TABLE test_schema.tbl_pg19_pub_items, test_schema.tbl_pg19_pub_dropped;
-- Into FOR ALL TABLES: drop+recreate before 19, in place (SET ALL TABLES) on 19.
CREATE PUBLICATION pg19_pub_empty_to_all;
CREATE PUBLICATION pg19_pub_list_to_all FOR TABLE test_schema.tbl_pg19_pub_items, test_schema.tbl_pg19_pub_dropped;
-- Unchanged, but _rebuilt is recreated: SET TABLE puts its row back.
CREATE PUBLICATION pg19_pub_rebuilt_member FOR TABLE test_schema.tbl_pg19_pub_rebuilt, test_schema.tbl_pg19_pub_orders;
-- Gains the inheritance parent alone (TABLE ONLY): SET TABLE ONLY ..._parent.
CREATE PUBLICATION pg19_pub_only_parent_list FOR TABLE test_schema.tbl_pg19_pub_orders;

-- PG15+: row filters, column lists, TABLES IN SCHEMA, publish_via_partition_root.
CREATE PUBLICATION pg19_pub_filter_change FOR TABLE test_schema.tbl_pg19_pub_items (id, qty) WHERE (qty > 0);
CREATE PUBLICATION pg19_pub_filter_drop FOR TABLE test_schema.tbl_pg19_pub_items WHERE (qty > 0), test_schema."tbl_pg19_pub_Mixed";
CREATE PUBLICATION pg19_pub_list_to_schema FOR TABLE test_schema.tbl_pg19_pub_items;
CREATE PUBLICATION pg19_pub_schema_to_mixed FOR TABLES IN SCHEMA shared_schema;
CREATE PUBLICATION pg19_pub_schema_to_empty FOR TABLES IN SCHEMA shared_schema;
-- A row filter on a partitioned table needs publish_via_partition_root:
-- turned on before the filter is set, turned off after it is removed.
CREATE PUBLICATION pg19_pub_viaroot_on FOR TABLE test_schema.tbl_pg19_pub_events;
CREATE PUBLICATION pg19_pub_viaroot_off FOR TABLE test_schema.tbl_pg19_pub_events WHERE (kind <> 'x') WITH (publish_via_partition_root = true);
-- The inheritance parent alone gains a row filter: SET TABLE ONLY ... WHERE.
-- (TO also has pg19_pub_parent_filter_to_only, created recursively: rows for
-- the parent and the child, both filtered, which only CREATE ... ONLY can
-- reproduce.)
CREATE PUBLICATION pg19_pub_only_parent_filter FOR TABLE ONLY test_schema.tbl_pg19_pub_parent;

-- PG18+: publish_generated_columns.
CREATE PUBLICATION pg19_pub_gencols_on FOR TABLE test_schema.tbl_pg19_pub_items;
CREATE PUBLICATION pg19_pub_gencols_off FOR TABLE test_schema.tbl_pg19_pub_items WITH (publish_generated_columns = stored);

-- PG19+: FOR ALL SEQUENCES and FOR ALL TABLES EXCEPT, changed in place with
-- ALTER PUBLICATION ... SET ALL ... wherever PostgreSQL allows it.
CREATE PUBLICATION pg19_pub_seq_from_only FOR ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_seq_unchanged FOR ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_seq_add FOR ALL TABLES;
CREATE PUBLICATION pg19_pub_seq_remove FOR ALL TABLES, ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_seq_to_tables FOR ALL SEQUENCES WITH (publish = 'insert');
CREATE PUBLICATION pg19_pub_seq_to_list FOR ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_list_to_seq FOR TABLE test_schema.tbl_pg19_pub_items, test_schema.tbl_pg19_pub_dropped;
CREATE PUBLICATION pg19_pub_except_unchanged FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_items);
CREATE PUBLICATION pg19_pub_except_change FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_items);
CREATE PUBLICATION pg19_pub_except_clear FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_orders), ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_except_add FOR ALL TABLES;
CREATE PUBLICATION pg19_pub_except_rebuilt FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_rebuilt);
CREATE PUBLICATION pg19_pub_mixed_to_except FOR TABLE test_schema.tbl_pg19_pub_items WHERE (qty > 0), TABLES IN SCHEMA shared_schema;

-- reg-misc: a partition listed by name while its parent's column changes
-- type (integer -> bigint in TO). Only the parent is altered; the partition
-- is never dropped and keeps its pg_publication_rel row, so it must be
-- dropped like any other member before the list empties (every version) or
-- becomes FOR ALL TABLES / ALL TABLES EXCEPT (in place on 19; SET ALL is
-- refused while a member is left). The unchanged publication is left alone.
CREATE TABLE test_schema.rv_regmisc_pubtbl_pt (id integer NOT NULL, v integer) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_regmisc_pubtbl_pt1 PARTITION OF test_schema.rv_regmisc_pubtbl_pt FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_regmisc_pubtbl_other (id integer PRIMARY KEY);
CREATE PUBLICATION rv_regmisc_pub_list_to_empty FOR TABLE test_schema.rv_regmisc_pubtbl_pt1, test_schema.rv_regmisc_pubtbl_other;
CREATE PUBLICATION rv_regmisc_pub_list_to_all FOR TABLE test_schema.rv_regmisc_pubtbl_pt1;
CREATE PUBLICATION rv_regmisc_pub_list_to_except FOR TABLE test_schema.rv_regmisc_pubtbl_pt1;
CREATE PUBLICATION rv_regmisc_pub_unchanged FOR TABLE test_schema.rv_regmisc_pubtbl_pt1, test_schema.rv_regmisc_pubtbl_other;

-- pre-grants (rv_pgrants_*): publication rows that block a table change are
-- released ahead of the tables and put back after them.
--   rv_pgrants_pub_collist      : column list (id, code, qty) -> (id, code);
--                                 TO drops qty and retypes code (PG15+)
--   rv_pgrants_pub_collist_same : column list (id, code) on the same table,
--                                 unchanged: released and restored (PG15+)
--   rv_pgrants_pub_filter       : row filter on qty, unchanged; qty becomes
--                                 bigint (PG15+)
--   rv_pgrants_pub_unlogged     : TO makes a member UNLOGGED and leaves it
--                                 out (unlogged tables cannot be published)
--                                 (every version)
--   rv_pgrants_pub_gen          : column list with a stored generated column
--                                 whose type and expression change (drop +
--                                 add) (PG18+)
--   rv_pgrants_pub_except       : EXCEPT entries that TO makes UNLOGGED and
--                                 attaches as a partition (PG19+)
-- Subscriptions (every version), disabled like the others (connect = false):
--   rv_pgrants_sub_setpub       : publications change while disabled: SET
--                                 PUBLICATION ... WITH (refresh = false)
--   rv_pgrants_sub_quoted       : TO subscribes to names that need quotes
CREATE TABLE test_schema.rv_pgrants_pubtbl (id integer PRIMARY KEY, code text, qty integer, note text);
CREATE TABLE test_schema.rv_pgrants_pubfilter (id integer PRIMARY KEY, qty integer);
CREATE TABLE test_schema.rv_pgrants_publog (id integer PRIMARY KEY);
CREATE TABLE test_schema.rv_pgrants_pubgen (id integer PRIMARY KEY, qty integer, total integer GENERATED ALWAYS AS (qty * 2) STORED);
CREATE TABLE test_schema.rv_pgrants_pubexc_log (id integer PRIMARY KEY);
CREATE TABLE test_schema.rv_pgrants_pubexc_parent (id integer NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pgrants_pubexc_part (id integer NOT NULL);
CREATE PUBLICATION rv_pgrants_pub_collist FOR TABLE test_schema.rv_pgrants_pubtbl (id, code, qty);
CREATE PUBLICATION rv_pgrants_pub_collist_same FOR TABLE test_schema.rv_pgrants_pubtbl (id, code);
CREATE PUBLICATION rv_pgrants_pub_filter FOR TABLE test_schema.rv_pgrants_pubfilter WHERE (qty > 0);
CREATE PUBLICATION rv_pgrants_pub_unlogged FOR TABLE test_schema.rv_pgrants_publog, test_schema.rv_pgrants_pubfilter;
CREATE PUBLICATION rv_pgrants_pub_gen FOR TABLE test_schema.rv_pgrants_pubgen (id, total);
CREATE PUBLICATION rv_pgrants_pub_except FOR ALL TABLES EXCEPT (TABLE test_schema.rv_pgrants_pubexc_log, TABLE test_schema.rv_pgrants_pubexc_part);
CREATE SUBSCRIPTION rv_pgrants_sub_setpub
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_pgrants_remote_p1
    WITH (connect = false, slot_name = NONE);
CREATE SUBSCRIPTION rv_pgrants_sub_quoted
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_pgrants_remote_p1
    WITH (connect = false, slot_name = NONE);

-- ==== END pg19:publications ====

-- ==== BEGIN pg19:subscriptions-fdw — subscriptions (SERVER, retain_dead_tuples, max_retention_duration, wal_receiver_timeout) and FDW CONNECTION ====

-- =============================================================================
-- PG19 feature test: subscriptions (SERVER source, retain_dead_tuples,
-- max_retention_duration, wal_receiver_timeout) and FDW CONNECTION functions
-- (FROM side)
-- =============================================================================
-- Note: Requires PostgreSQL 19+ (postgres_fdw 1.3 ships postgres_fdw_connection).
-- The PG19-only statements are syntax errors / 'unrecognized subscription
-- parameter' on PG14-18 and fail on BOTH sides alike under ON_ERROR_STOP=0.
-- Relies on the postgres_fdw extension created for the foreign table tests.
--
-- The pg19_subfdw_* subscriptions exist on both sides except the FROM-only
-- pg19_subfdw_sub_from_only and, on PG14-18 only, pg19_subfdw_sub_to_server.
-- They and the rv_pgrants_* ones use connect = false / slot_name = NONE:
-- disabled, no slot, droppable without a publisher. The rv_r4s_* ones at the
-- end of this block are disabled too, but most record a replication slot:
-- a plain DROP SUBSCRIPTION of those contacts the publisher to drop the slot
-- and fails, so detach them first (ALTER SUBSCRIPTION ... DISABLE, then
-- SET (slot_name = NONE)). The TO-only rv_r4s_sub_to_only_* are created by the
-- migration, which writes connect = false for a disabled subscription and so
-- never contacts their publisher. pg_subscription is a shared catalog; these
-- rows only stay apart in the a and b dumps thanks to pgc's subdbid filter.

-- FDW CONNECTION functions. CONNECTION is set by a separate ALTER, so PG14-18
-- keep identical connection-less wrappers on both sides.
-- pg19_subfdw_fdw_same:      same CONNECTION on both sides (no diff)
-- pg19_subfdw_fdw_added:     none in FROM; TO adds one (ALTER ... CONNECTION),
--                            and TO's pg19_subfdw_srv_b needs it before
--                            ALTER SUBSCRIPTION ... SERVER pg19_subfdw_srv_b
-- pg19_subfdw_fdw_removed:   CONNECTION in FROM; TO removes it (NO CONNECTION)
-- pg19_subfdw_fdw_from_only: FROM-only, carries the FROM-only server, so its
--                            DROP must wait until no subscription uses it
CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_same
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
ALTER FOREIGN DATA WRAPPER pg19_subfdw_fdw_same CONNECTION public.postgres_fdw_connection;

CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_added
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;

CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_removed
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
ALTER FOREIGN DATA WRAPPER pg19_subfdw_fdw_removed CONNECTION public.postgres_fdw_connection;

CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_from_only
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator
    CONNECTION public.postgres_fdw_connection;

-- Servers and user mappings for the SERVER subscriptions (a PUBLIC mapping
-- satisfies ALTER SUBSCRIPTION ... SERVER's user-mapping check).
CREATE SERVER pg19_subfdw_srv_a
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_a');
CREATE USER MAPPING FOR PUBLIC SERVER pg19_subfdw_srv_a OPTIONS (user 'pgc_repl');

-- pg19_subfdw_srv_from_only: FROM-only, on the FROM-only wrapper. A
-- subscription blocks DROP SERVER / DROP FOREIGN DATA WRAPPER even with
-- CASCADE, so both drops must follow the subscription changes.
CREATE SERVER pg19_subfdw_srv_from_only
    FOREIGN DATA WRAPPER pg19_subfdw_fdw_from_only
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_legacy');
CREATE USER MAPPING FOR PUBLIC SERVER pg19_subfdw_srv_from_only OPTIONS (user 'pgc_repl');

-- pg19_subfdw_sub_to_server: CONNECTION in FROM, SERVER pg19_subfdw_srv_a in TO
--   (ALTER SUBSCRIPTION ... SERVER). On PG14-18 TO cannot create it, so the
--   diff there is DROP SUBSCRIPTION.
CREATE SUBSCRIPTION pg19_subfdw_sub_to_server
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);

-- pg19_subfdw_sub_switch: SERVER pg19_subfdw_srv_from_only in FROM, SERVER
--   pg19_subfdw_srv_b in TO. The ALTER must run after pg19_subfdw_srv_b, its
--   user mapping and its wrapper's CONNECTION exist, and before the FROM-only
--   server and wrapper are dropped.
CREATE SUBSCRIPTION pg19_subfdw_sub_switch
    SERVER pg19_subfdw_srv_from_only
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);

-- pg19_subfdw_sub_to_conn: SERVER pg19_subfdw_srv_a in FROM, CONNECTION in TO.
--   Created with CONNECTION and switched by ALTER, so PG14-18 keep identical
--   CONNECTION subscriptions on both sides.
CREATE SUBSCRIPTION pg19_subfdw_sub_to_conn
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);
ALTER SUBSCRIPTION pg19_subfdw_sub_to_conn SERVER pg19_subfdw_srv_a;

-- pg19_subfdw_sub_from_only: FROM-only, on the FROM-only server; dropped
--   ahead of every other section.
CREATE SUBSCRIPTION pg19_subfdw_sub_from_only
    SERVER pg19_subfdw_srv_from_only
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);

-- pg19_subfdw_rdt_off: retain_dead_tuples true -> false, max_retention_duration
--   60000 -> 5000, and the conninfo moves from this server to an unreachable
--   host. While retain_dead_tuples is on, a CONNECTION change contacts the new
--   publisher, so the SET has to run first.
CREATE SUBSCRIPTION pg19_subfdw_rdt_off
    CONNECTION 'host=127.0.0.1 port=5432 dbname=postgres user=postgres'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, retain_dead_tuples = true, max_retention_duration = 60000);

-- pg19_subfdw_rdt_on: retain_dead_tuples false -> true, and the conninfo moves
--   from an unreachable host to this server (port 5432 inside the container,
--   trust-authenticated on 127.0.0.1). Turning it on always contacts the
--   publisher, which must be PG19+, so the SET has to run after the CONNECTION
--   change. The explicit max_retention_duration = 0 (the default) makes PG14-18
--   reject this side too.
CREATE SUBSCRIPTION pg19_subfdw_rdt_on
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, max_retention_duration = 0);

-- wal_receiver_timeout (stored verbatim; '-1' = inherit the GUC):
-- pg19_subfdw_wrt_changed: '30s' -> '10s'
-- pg19_subfdw_wrt_reset:   '45s' -> '-1' (no RESET form: SET ... = '-1')
-- pg19_subfdw_wrt_set:     '-1' -> '2min', with max_retention_duration
--                          0 -> 10000 in the same SET
-- pg19_subfdw_wrt_same:    '1min' on both sides (no diff)
CREATE SUBSCRIPTION pg19_subfdw_wrt_changed
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '30s');
CREATE SUBSCRIPTION pg19_subfdw_wrt_reset
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '45s');
CREATE SUBSCRIPTION pg19_subfdw_wrt_set
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '-1', max_retention_duration = 0);
CREATE SUBSCRIPTION pg19_subfdw_wrt_same
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '1min');

-- pg19_subfdw_sub_owner_switch: SERVER pg19_subfdw_srv_a owned by postgres in
--   FROM; in TO it is owned by pgc_pg19_subfdw_owner and on
--   pg19_subfdw_srv_owner, whose only user mapping is that role's.
--   ALTER SUBSCRIPTION ... SERVER checks the owner's user mapping, USAGE and
--   (non-superuser) password, so OWNER TO has to run first. The server, the
--   mapping (with a password) and the USAGE grant exist on both sides: pgc
--   does not compare server privileges. Created with CONNECTION and switched
--   by ALTER, so PG14-18 keep CONNECTION subscriptions on both sides; there
--   the diff is the OWNER TO alone (PG16+), or nothing on PG14/15, which
--   reject a non-superuser subscription owner in TO as well.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_subfdw_owner') THEN
        CREATE ROLE pgc_pg19_subfdw_owner;
    END IF;
END
$$;
CREATE SERVER pg19_subfdw_srv_owner
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_owner');
CREATE USER MAPPING FOR pgc_pg19_subfdw_owner SERVER pg19_subfdw_srv_owner
    OPTIONS (user 'pgc_repl', password 'pgc_not_a_secret');
GRANT USAGE ON FOREIGN SERVER pg19_subfdw_srv_owner TO pgc_pg19_subfdw_owner;
CREATE SUBSCRIPTION pg19_subfdw_sub_owner_switch
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);
ALTER SUBSCRIPTION pg19_subfdw_sub_owner_switch SERVER pg19_subfdw_srv_a;

-- r3-constraints (rv_r3c_*): a foreign server that changes its wrapper (every
-- version). ALTER SERVER cannot change it, so it is dropped and created again
-- — without CASCADE, which silently took its user mapping, the foreign table
-- on it (with its grant and comment) and the view over that. The migration
-- drops the view and the foreign table first, and creates the server, its
-- user mapping, the foreign table and the view again.
-- FROM: rv_r3c_srv on rv_r3c_fdw_a. TO: the same server on rv_r3c_fdw_b.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_r3c_reader') THEN
        CREATE ROLE pgc_pg19_r3c_reader;
    END IF;
END
$$;
CREATE FOREIGN DATA WRAPPER rv_r3c_fdw_a HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
CREATE FOREIGN DATA WRAPPER rv_r3c_fdw_b HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
CREATE SERVER rv_r3c_srv FOREIGN DATA WRAPPER rv_r3c_fdw_a
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_r3c');
COMMENT ON SERVER rv_r3c_srv IS 'rv_r3c server';
CREATE USER MAPPING FOR PUBLIC SERVER rv_r3c_srv OPTIONS (user 'pgc_repl');
CREATE FOREIGN TABLE test_schema.rv_r3c_ft (id integer, label text)
    SERVER rv_r3c_srv OPTIONS (table_name 'rv_r3c_remote');
COMMENT ON FOREIGN TABLE test_schema.rv_r3c_ft IS 'rv_r3c foreign table';
GRANT SELECT ON test_schema.rv_r3c_ft TO pgc_pg19_r3c_reader;
CREATE VIEW test_schema.rv_r3c_ft_view AS SELECT id, label FROM test_schema.rv_r3c_ft;
-- Review fixes: what else the server's re-create has to bring back — its own
-- USAGE grant, a column-level grant on the foreign table, and a foreign table
-- that is a partition of a local partitioned table (re-attached).
GRANT USAGE ON FOREIGN SERVER rv_r3c_srv TO pgc_pg19_r3c_reader;
GRANT UPDATE (label) ON test_schema.rv_r3c_ft TO pgc_pg19_r3c_reader;
CREATE TABLE test_schema.rv_r3c_fp (id integer, label text) PARTITION BY RANGE (id);
CREATE FOREIGN TABLE test_schema.rv_r3c_fp_remote PARTITION OF test_schema.rv_r3c_fp
    FOR VALUES FROM (0) TO (100) SERVER rv_r3c_srv OPTIONS (table_name 'rv_r3c_fp_remote');

-- r4-sub (rv_r4s_*): replication slots (pg_subscription.subslotname), every
-- version. Every subscription here is disabled and points at an unreachable
-- publisher, and no statement the migration runs contacts it: a TO-only
-- disabled subscription is created WITH (connect = false, ...), and a slot
-- change is ALTER SUBSCRIPTION ... SET (slot_name = ...).
--   rv_r4s_sub_own_to_none   : its own slot -> NONE (SET (slot_name = NONE))
--   rv_r4s_sub_none_to_named : NONE -> rv_r4s_named_slot
--   rv_r4s_sub_named_to_own  : rv_r4s_old_slot -> its own slot
--   rv_r4s_sub_same_named    : rv_r4s_kept_slot on both sides (no diff)
--   rv_r4s_sub_to_only_*     : TO-only, created by the migration (schema_b.sql)
CREATE SUBSCRIPTION rv_r4s_sub_own_to_none
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false);
CREATE SUBSCRIPTION rv_r4s_sub_none_to_named
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = NONE);
CREATE SUBSCRIPTION rv_r4s_sub_named_to_own
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = 'rv_r4s_old_slot');
CREATE SUBSCRIPTION rv_r4s_sub_same_named
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = 'rv_r4s_kept_slot');
-- r4-sub review: TO slots PostgreSQL refuses as a slot_name. CREATE
-- SUBSCRIPTION ... WITH (connect = false) records the subscription's own
-- name as its slot unchecked, but SET (slot_name = ...) only takes 1-63
-- lower-case letters, digits and underscores, and reads 'none' as NONE. The
-- migration leaves both slots out (a note, and a warning at compare time), so
-- a keeps its slot here:
--   "rv_r4s_Sub_Upper"    : rv_r4s_upper_slot -> its own slot rv_r4s_Sub_Upper
--   rv_r4s_sub_none_alias : NONE -> a slot named none
CREATE SUBSCRIPTION "rv_r4s_Sub_Upper"
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = 'rv_r4s_upper_slot');
CREATE SUBSCRIPTION rv_r4s_sub_none_alias
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = NONE);

-- ==== END pg19:subscriptions-fdw ====

-- ==== BEGIN pg19:routines — routines: aggregate SUPPORT / PARALLEL / negative SSPACE, SET <list GUC> TO NULL ====

-- -----------------------------------------------------------------------------
-- Aggregate modify path (all versions): pgc emitted a plain CREATE AGGREGATE, so
-- every in-place change failed with "already exists"; it now emits CREATE OR
-- REPLACE AGGREGATE. PARALLEL was hashed but never emitted (a loop), and a
-- negative SSPACE / MSSPACE was dropped and invisible to the hash. PG14-18 accept
-- and store negative state sizes too; PG19's planner reads them as "unbounded".
-- -----------------------------------------------------------------------------
-- FROM: PARALLEL UNSAFE (default) -> TO: PARALLEL = SAFE.
CREATE AGGREGATE test_schema.pg19_routines_agg_parallel(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    COMBINEFUNC = int8pl,
    INITCOND = '0'
);

-- FROM: INITCOND '0' -> TO: '1'. The view must survive the in-place change.
CREATE AGGREGATE test_schema.pg19_routines_agg_initcond(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0'
);
CREATE VIEW test_schema.pg19_routines_agg_initcond_v AS
SELECT test_schema.pg19_routines_agg_initcond(v.x) AS n
FROM (VALUES (1::bigint), (2::bigint)) AS v(x);

-- FROM: default SSPACE (0) -> TO: SSPACE = -1.
CREATE AGGREGATE test_schema.pg19_routines_agg_sspace(integer) (
    SFUNC = int4pl,
    STYPE = integer
);

-- FROM: SSPACE = -1 -> TO: back to the default.
CREATE AGGREGATE test_schema.pg19_routines_agg_sspace_reset(integer) (
    SFUNC = int4pl,
    STYPE = integer,
    SSPACE = -1
);

-- FROM: moving aggregate with default MSSPACE -> TO: MSSPACE = -1.
CREATE AGGREGATE test_schema.pg19_routines_agg_msspace(integer) (
    SFUNC = int4pl,
    STYPE = integer,
    MSFUNC = int4pl,
    MINVFUNC = int4mi,
    MSTYPE = integer
);

-- FROM: ordered-set -> TO: hypothetical-set, same identity arguments. CREATE OR
-- REPLACE cannot change the aggregate kind, so pgc must drop first.
CREATE AGGREGATE test_schema.pg19_routines_agg_kind(VARIADIC "any" ORDER BY VARIADIC "any") (
    SFUNC = ordered_set_transition_multi,
    STYPE = internal,
    FINALFUNC = rank_final,
    FINALFUNC_EXTRA
);

-- FROM: plain function -> TO: aggregate with the same signature ("cannot change
-- routine kind" unless dropped first).
CREATE FUNCTION test_schema.pg19_routines_fn_to_agg(bigint)
RETURNS bigint
LANGUAGE sql
AS $$ SELECT $1 $$;

-- -----------------------------------------------------------------------------
-- Aggregate SUPPORT function (PG19+). CREATE AGGREGATE ... SUPPORT = fn is new in
-- PostgreSQL 19 (stored in the aggregate's pg_proc.prosupport; needs superuser).
-- PG14-18 accept the statement with 'WARNING: aggregate attribute "support" not
-- recognized' and create the aggregate without it, so there only the FROM-only /
-- TO-only aggregates differ. int8inc_support is count()'s own support function.
-- -----------------------------------------------------------------------------
-- FROM: no SUPPORT -> TO adds it.
CREATE AGGREGATE test_schema.pg19_routines_support_add(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0'
);
-- FROM: SUPPORT -> TO removes it.
CREATE AGGREGATE test_schema.pg19_routines_support_drop(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0',
    SUPPORT = int8inc_support
);
-- Same SUPPORT on both sides (count(*)-shaped) -> no diff.
CREATE AGGREGATE test_schema.pg19_routines_support_same(*) (
    SFUNC = int8inc,
    STYPE = bigint,
    INITCOND = '0',
    SUPPORT = int8inc_support
);
-- FROM only -> dropped.
CREATE AGGREGATE test_schema.pg19_routines_support_old(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0',
    SUPPORT = int8inc_support
);

-- -----------------------------------------------------------------------------
-- Routine SET clauses for GUC_LIST_QUOTE parameters. `SET search_path = ''` (a
-- common hardening pattern) stores search_path="" in proconfig, which pgc pasted
-- back as `SET search_path = ""` — rejected on every version ("zero-length
-- delimited identifier"). Elements longer than 63 bytes were truncated the same
-- way. Both are now emitted element-wise as literals, as pg_get_functiondef does.
-- PG19 adds `SET <list GUC> TO NULL`, stored as a bare search_path=, which only
-- `TO NULL` can reproduce (a syntax error on PG14-18, where those statements fail
-- and the routine exists on one side only).
-- -----------------------------------------------------------------------------
-- FROM: search_path = public -> TO: search_path = '' (stored "").
CREATE FUNCTION test_schema.pg19_routines_guc_empty_string()
RETURNS integer
LANGUAGE sql
SET search_path = public
AS $$ SELECT 1 $$;

-- FROM: search_path = public -> TO: '', public (an empty element in a list).
CREATE FUNCTION test_schema.pg19_routines_guc_empty_element()
RETURNS integer
LANGUAGE sql
SET search_path = public
AS $$ SELECT 1 $$;

-- FROM: search_path = public -> TO: a 70-byte element plus public.
CREATE FUNCTION test_schema.pg19_routines_guc_long_element()
RETURNS integer
LANGUAGE sql
SET search_path = public
AS $$ SELECT 1 $$;

-- FROM: output_plugin_libraries = pgoutput -> TO: an unquoted two-element list.
-- output_plugin_libraries is GUC_LIST_QUOTE, so wrapping the list in one literal
-- would re-store it as the single element "pgoutput, test_decoding".
CREATE FUNCTION test_schema.pg19_routines_guc_output_plugins()
RETURNS integer
LANGUAGE sql
SET output_plugin_libraries = pgoutput
AS $$ SELECT 1 $$;

-- PG19: FROM search_path = '' (stored "") -> TO search_path TO NULL (bare empty).
CREATE FUNCTION test_schema.pg19_routines_guc_to_null()
RETURNS integer
LANGUAGE sql
SET search_path = ''
AS $$ SELECT 1 $$;

-- PG19: FROM search_path TO NULL -> TO search_path = public.
CREATE FUNCTION test_schema.pg19_routines_guc_from_null()
RETURNS integer
LANGUAGE sql
SET search_path TO NULL
AS $$ SELECT 1 $$;

-- -----------------------------------------------------------------------------
-- Aggregate SORTOP (all versions). The sort operator was emitted as the raw
-- regoper text, `SORTOP = pg_catalog.>`, a syntax error (the grammar takes a
-- qualified operator only as OPERATOR(pg_catalog.>), which pg_dump writes too).
-- -----------------------------------------------------------------------------
-- FROM: no sort operator -> TO: SORTOP = > (a MAX-like aggregate), in place.
CREATE AGGREGATE test_schema.pg19_routines_agg_sortop(integer) (
    SFUNC = int4larger,
    STYPE = integer
);

-- -----------------------------------------------------------------------------
-- Numeric elements of a list GUC (all versions). `SET search_path = public, 42`
-- stores the number bare (search_path=public, 42); as a literal it would be
-- stored back as "42" and the routine re-emitted on every compare.
-- -----------------------------------------------------------------------------
-- FROM: search_path = public -> TO: public, 42 (verbatim form).
CREATE FUNCTION test_schema.pg19_routines_guc_numeric()
RETURNS integer
LANGUAGE sql
SET search_path = public
AS $$ SELECT 1 $$;

-- FROM: search_path = public -> TO: '', 7 (literal form, the number stays bare).
CREATE FUNCTION test_schema.pg19_routines_guc_numeric_mixed()
RETURNS integer
LANGUAGE sql
SET search_path = public
AS $$ SELECT 1 $$;

-- -----------------------------------------------------------------------------
-- Privileges of a routine the migration drops and recreates (all versions). The
-- DROP ... CASCADE takes its grants with it, so they must be re-applied in the
-- same run instead of the next one (CI runs GRANTS_MODE=full).
-- -----------------------------------------------------------------------------
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_routines_role') THEN
          CREATE ROLE pgc_pg19_routines_role;
     END IF;
END;
$$;

-- FROM: function granted to pgc_pg19_routines_role -> TO: an aggregate of the same
-- signature with the same grant (routine kind change).
CREATE FUNCTION test_schema.pg19_routines_grant_kind(bigint)
RETURNS bigint
LANGUAGE sql
AS $$ SELECT $1 $$;
GRANT EXECUTE ON FUNCTION test_schema.pg19_routines_grant_kind(bigint) TO pgc_pg19_routines_role;

-- FROM: returns bigint with EXECUTE revoked from PUBLIC -> TO: returns integer,
-- still revoked. The recreated function starts with PUBLIC's EXECUTE again.
CREATE FUNCTION test_schema.pg19_routines_grant_revoked(bigint)
RETURNS bigint
LANGUAGE sql
AS $$ SELECT $1 $$;
REVOKE EXECUTE ON FUNCTION test_schema.pg19_routines_grant_revoked(bigint) FROM PUBLIC;

-- ==== END pg19:routines ====

-- ==== BEGIN pg19:tables — tables: CHECK ALTER CONSTRAINT [NOT] ENFORCED, generated column SET EXPRESSION ====
-- Object names use the wp19_tables_ prefix rather than pg19_: pgc's table
-- query filters `tablename NOT LIKE 'pg_%'` with an unescaped `_`, which
-- silently drops every table whose name starts with "pg" plus any character.
-- -----------------------------------------------------------------------------
-- CHECK constraints: ALTER CONSTRAINT ... [NOT] ENFORCED (PG19+)
-- -----------------------------------------------------------------------------
-- FROM states. Every NOT ENFORCED state is reached with the PG19-only
-- ALTER CONSTRAINT form, which PG14-18 reject, so there FROM and TO stay
-- identical and this part produces no diff. On PG19 pgc must flip each one
-- with an in-place ALTER CONSTRAINT instead of a drop + re-add.
CREATE TABLE test_schema.wp19_tables_ck (
    id    integer PRIMARY KEY,
    qty   integer,
    price numeric
);
ALTER TABLE test_schema.wp19_tables_ck
    ADD CONSTRAINT wp19_tables_ck_relax CHECK (qty >= 0);
ALTER TABLE test_schema.wp19_tables_ck
    ADD CONSTRAINT wp19_tables_ck_tighten CHECK (price > 0);
ALTER TABLE test_schema.wp19_tables_ck
    ALTER CONSTRAINT wp19_tables_ck_tighten NOT ENFORCED;
ALTER TABLE test_schema.wp19_tables_ck
    ADD CONSTRAINT wp19_tables_ck_notvalid CHECK (qty < 1000000) NOT VALID;
INSERT INTO test_schema.wp19_tables_ck VALUES (1, 5, 10), (2, 7, 20);

-- Classical inheritance: the child's copy of the constraint is merged
-- (conislocal = true, coninhcount = 1). The child sorts before its parent
-- on purpose: PG19 rejects NOT ENFORCED on the child while the parent's copy
-- is still enforced, so pgc must emit the parent's (recursing) ALTER first.
CREATE TABLE test_schema.wp19_tables_ck_parent (
    id     integer,
    amount integer,
    CONSTRAINT wp19_tables_ck_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.wp19_tables_ck_child (
    CONSTRAINT wp19_tables_ck_amount CHECK (amount > 0)
) INHERITS (test_schema.wp19_tables_ck_parent);

-- Partitioned: NOT ENFORCED in FROM (recursing to the partition), ENFORCED
-- in TO. The partition's copy is inherited and managed by the parent.
CREATE TABLE test_schema.wp19_tables_ck_part (
    id     integer NOT NULL,
    amount integer,
    CONSTRAINT wp19_tables_ck_part_amount CHECK (amount >= 0)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.wp19_tables_ck_part_1
    PARTITION OF test_schema.wp19_tables_ck_part FOR VALUES FROM (0) TO (1000);
ALTER TABLE test_schema.wp19_tables_ck_part
    ALTER CONSTRAINT wp19_tables_ck_part_amount NOT ENFORCED;
INSERT INTO test_schema.wp19_tables_ck_part VALUES (1, 3);

-- -----------------------------------------------------------------------------
-- Generated columns: ALTER COLUMN ... SET EXPRESSION (STORED PG17+, VIRTUAL PG19+)
-- -----------------------------------------------------------------------------
-- FROM expressions use (qty * 2); TO (schema_b.sql) uses (qty * 3).
-- STORED, every version: SET EXPRESSION on PG17+, DROP/ADD COLUMN on PG14-16
-- (with the view dropped first and the index restored). pgc used to emit the
-- invalid `ALTER COLUMN total ADD GENERATED ALWAYS AS (...) STORED` here.
CREATE TABLE test_schema.wp19_tables_gen_stored (
    id    integer PRIMARY KEY,
    qty   integer NOT NULL,
    total integer GENERATED ALWAYS AS (qty * 2) STORED
);
CREATE INDEX wp19_tables_gen_stored_total_idx
    ON test_schema.wp19_tables_gen_stored (total);
CREATE VIEW test_schema.wp19_tables_gen_stored_v AS
    SELECT id, total FROM test_schema.wp19_tables_gen_stored;
INSERT INTO test_schema.wp19_tables_gen_stored (id, qty) VALUES (1, 4), (2, 6);

-- VIRTUAL with CHECK constraints (PG18+; fails harmlessly on PG14-17). PG18
-- rejects SET EXPRESSION here, so pgc drops the view, drops and re-adds the
-- column and restores its CHECK; PG19 uses SET EXPRESSION and keeps the view.
CREATE TABLE test_schema.wp19_tables_gen_virtual (
    id      integer PRIMARY KEY,
    qty     integer NOT NULL CONSTRAINT wp19_tables_gen_virtual_qty_check CHECK (qty >= 0),
    doubled integer GENERATED ALWAYS AS (qty * 2) VIRTUAL
        CONSTRAINT wp19_tables_gen_virtual_doubled_check CHECK (doubled < 1000000)
);
CREATE VIEW test_schema.wp19_tables_gen_virtual_v AS
    SELECT id, doubled FROM test_schema.wp19_tables_gen_virtual;
INSERT INTO test_schema.wp19_tables_gen_virtual (id, qty) VALUES (1, 4), (2, 6);

-- VIRTUAL without CHECK constraints (PG18+): SET EXPRESSION on 18 and 19.
CREATE TABLE test_schema.wp19_tables_gen_virtual_nochk (
    id      integer PRIMARY KEY,
    qty     integer NOT NULL,
    doubled integer GENERATED ALWAYS AS (qty * 2) VIRTUAL
);

-- VIRTUAL in an explicit FOR TABLE publication (PG18+): SET EXPRESSION is
-- rejected on 18 and 19 alike, so pgc must drop and re-add the column.
CREATE TABLE test_schema.wp19_tables_gen_published (
    id      integer PRIMARY KEY,
    qty     integer NOT NULL,
    doubled integer GENERATED ALWAYS AS (qty * 2) VIRTUAL
);
CREATE PUBLICATION wp19_tables_pub FOR TABLE test_schema.wp19_tables_gen_published;

-- -----------------------------------------------------------------------------
-- Classical inheritance ordering (every version)
-- -----------------------------------------------------------------------------
-- FROM-only family whose parent sorts first: DROP TABLE on the parent fails
-- while the child exists, so pgc must drop the child first.
CREATE TABLE test_schema.wp19_tables_inh_drop_a_parent (id integer);
CREATE TABLE test_schema.wp19_tables_inh_drop_b_child ()
    INHERITS (test_schema.wp19_tables_inh_drop_a_parent);

-- -----------------------------------------------------------------------------
-- Generated-column changes that interact with the rest of the table
-- -----------------------------------------------------------------------------
-- plain -> STORED generated next to a CHECK on the same column whose
-- enforceability flips on PG19 (TO). The DROP COLUMN takes the CHECK with it,
-- so the flip must stay a drop + re-add that runs first, not an in-place
-- ALTER CONSTRAINT after the column is gone. Every version: DROP/ADD COLUMN.
CREATE TABLE test_schema.wp19_tables_rc (
    id integer PRIMARY KEY,
    g  integer,
    CONSTRAINT wp19_tables_rc_g_check CHECK (g > 0)
);
INSERT INTO test_schema.wp19_tables_rc VALUES (1, 5), (2, 9);

-- Type and expression change together: ALTER COLUMN ... TYPE would cast the
-- old expression (int -> date cannot), so this is a DROP/ADD COLUMN on every
-- version, never TYPE + SET EXPRESSION.
CREATE TABLE test_schema.wp19_tables_gen_retype (
    id    integer PRIMARY KEY,
    stamp integer GENERATED ALWAYS AS (id * 2) STORED
);
INSERT INTO test_schema.wp19_tables_gen_retype (id) VALUES (1), (2);

-- The new expression reads a column the migration adds (bonus), and the old
-- one read a column it drops (legacy): SET EXPRESSION (PG17+) or the
-- DROP/ADD COLUMN (PG14-16) must run after ADD COLUMN bonus and before
-- DROP COLUMN legacy. The TO expression, `(qty * 2) + (bonus * 3)`, deparses
-- as `((qty * 2) + (bonus * 3))`, whose outer pair is the only redundant one.
CREATE TABLE test_schema.wp19_tables_gen_newcol (
    id     integer PRIMARY KEY,
    qty    integer,
    legacy integer,
    total  integer GENERATED ALWAYS AS (qty * 2 + legacy) STORED
);
INSERT INTO test_schema.wp19_tables_gen_newcol (id, qty, legacy) VALUES (1, 4, 1);
-- plain -> generated reading a new column: DROP/ADD COLUMN after the ADD.
CREATE TABLE test_schema.wp19_tables_gen_newcol_p (
    id    integer PRIMARY KEY,
    qty   integer,
    total integer
);

-- PG19: the parent's CHECK goes NOT ENFORCED while the child's merged copy
-- stays enforced (TO). ALTER CONSTRAINT ... NOT ENFORCED cannot say ONLY and
-- would flip the child too, so pgc keeps the drop + re-add on the parent,
-- which leaves the child's local copy alone.
CREATE TABLE test_schema.wp19_tables_keep_parent (
    id     integer,
    amount integer,
    CONSTRAINT wp19_tables_keep_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.wp19_tables_keep_child (
    CONSTRAINT wp19_tables_keep_amount CHECK (amount > 0)
) INHERITS (test_schema.wp19_tables_keep_parent);

-- PG18+: a VIRTUAL expression change on a partitioned table that gains a
-- partition with a CHECK (TO). The new partition exists before the parent is
-- altered, so PG18 (which rejects SET EXPRESSION in tables with check
-- constraints, recursion included) needs the DROP/ADD COLUMN; PG19 uses SET
-- EXPRESSION, recursing to the partitions.
CREATE TABLE test_schema.wp19_tables_gen_vpart (
    id integer NOT NULL,
    v  integer GENERATED ALWAYS AS (id * 2) VIRTUAL
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.wp19_tables_gen_vpart_1
    PARTITION OF test_schema.wp19_tables_gen_vpart FOR VALUES FROM (0) TO (100);

-- A STORED expression change on a partitioned table whose partition has a
-- local CHECK on the column that changes too. On PG14-16 the parent's
-- recursing DROP COLUMN takes the partition's CHECK first, so the
-- partition's drop of it says IF EXISTS.
CREATE TABLE test_schema.wp19_tables_gen_part (
    id    integer NOT NULL,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 2) STORED
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.wp19_tables_gen_part_1
    PARTITION OF test_schema.wp19_tables_gen_part (
        CONSTRAINT wp19_tables_gen_part_1_total_check CHECK (total >= 0)
    ) FOR VALUES FROM (0) TO (100);
INSERT INTO test_schema.wp19_tables_gen_part (id, qty) VALUES (1, 2);

-- Classical inheritance and a STORED expression change. PG17+: SET
-- EXPRESSION with ONLY on the parent and on each child that follows it, so
-- a child with an expression of its own keeps it. PG14-16: the parent drops
-- the column, drops the followers' copies, and re-adds it once; the
-- followers emit no drop/add of their own, and their index, view and CHECK
-- are restored.
CREATE TABLE test_schema.wp19_tables_gen_inh (
    id    integer,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 2) STORED
);
-- Follows the parent (inherited-only copy), with an index, a view and a
-- local CHECK of its own; the CHECK changes too.
CREATE TABLE test_schema.wp19_tables_gen_inh_follow (
    CONSTRAINT wp19_tables_gen_inh_follow_check CHECK (total >= 0)
) INHERITS (test_schema.wp19_tables_gen_inh);
CREATE INDEX wp19_tables_gen_inh_follow_total_idx
    ON test_schema.wp19_tables_gen_inh_follow (total);
CREATE VIEW test_schema.wp19_tables_gen_inh_follow_v AS
    SELECT id, total FROM test_schema.wp19_tables_gen_inh_follow;
INSERT INTO test_schema.wp19_tables_gen_inh_follow (id, qty) VALUES (1, 3);
-- PG16+ (PG14/15 reject a child's own generation expression): a merged copy
-- with the parent's expression, which follows it...
CREATE TABLE test_schema.wp19_tables_gen_inh_merged (
    id    integer,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 2) STORED
) INHERITS (test_schema.wp19_tables_gen_inh);
-- ...and a merged copy with an expression of its own, which must survive.
CREATE TABLE test_schema.wp19_tables_gen_inh_own (
    id    integer,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 5) STORED
) INHERITS (test_schema.wp19_tables_gen_inh);
INSERT INTO test_schema.wp19_tables_gen_inh_own (id, qty) VALUES (1, 2);

-- PG18+: VIRTUAL, classical inheritance, no CHECK: SET EXPRESSION with ONLY
-- on 18 and 19 for the parent and the child that follows it.
CREATE TABLE test_schema.wp19_tables_gen_vinh (
    id      integer,
    qty     integer,
    doubled integer GENERATED ALWAYS AS (qty * 2) VIRTUAL
);
CREATE TABLE test_schema.wp19_tables_gen_vinh_c ()
    INHERITS (test_schema.wp19_tables_gen_vinh);

-- -----------------------------------------------------------------------------
-- reg-misc: a CHECK enforceability flip next to a rewrite of the column it
-- reads (ALTER COLUMN ... TYPE, SET EXPRESSION)
-- -----------------------------------------------------------------------------
-- The rewrite checks every row against the CHECK constraints still enforced
-- at that moment. On PG19 (in place) a CHECK going NOT ENFORCED must be
-- relaxed before the rewrite, whose new values it rejects, and one going
-- ENFORCED must wait for the rewritten values, which it accepts while the
-- current ones violate it. PG18 drops the CHECK first and re-adds it last.
-- PG14-17 cannot parse NOT ENFORCED: the relaxing tables lose the CHECK in
-- TO, the tightening ones gain it, and every rewrite still applies.
-- Relaxed, STORED expression change: the new g (-6) violates the old CHECK.
CREATE TABLE test_schema.rv_regmisc_ck_expr (
    id integer PRIMARY KEY,
    c  integer,
    g  integer GENERATED ALWAYS AS (c * 2) STORED,
    CONSTRAINT rv_regmisc_ck_expr_g CHECK (g > -5)
);
INSERT INTO test_schema.rv_regmisc_ck_expr (id, c) VALUES (1, -2);
-- Relaxed, type change: 0.6 rounds to 1, which the old CHECK rejects.
CREATE TABLE test_schema.rv_regmisc_ck_type (
    id integer PRIMARY KEY,
    c  numeric(6,2),
    CONSTRAINT rv_regmisc_ck_type_c CHECK (c <> 1)
);
INSERT INTO test_schema.rv_regmisc_ck_type VALUES (1, 0.6);
-- Relaxed, VIRTUAL expression change (PG18+; PG18 drops and re-adds g).
CREATE TABLE test_schema.rv_regmisc_ck_virt (
    id integer PRIMARY KEY,
    c  integer,
    g  integer GENERATED ALWAYS AS (c * 2) VIRTUAL,
    CONSTRAINT rv_regmisc_ck_virt_g CHECK (g > -5)
);
INSERT INTO test_schema.rv_regmisc_ck_virt (id, c) VALUES (1, -2);
-- Tightened, STORED expression change: the current g (-6) violates the
-- CHECK, the new one (-2) does not.
CREATE TABLE test_schema.rv_regmisc_ck_tight_expr (
    id integer PRIMARY KEY,
    c  integer,
    g  integer GENERATED ALWAYS AS (c * 2) STORED
);
INSERT INTO test_schema.rv_regmisc_ck_tight_expr (id, c) VALUES (1, -3);
ALTER TABLE test_schema.rv_regmisc_ck_tight_expr
    ADD CONSTRAINT rv_regmisc_ck_tight_expr_g CHECK (g > -5) NOT ENFORCED;
-- Tightened, type change: 0.6 violates the CHECK, the rounded 1 does not.
CREATE TABLE test_schema.rv_regmisc_ck_tight_type (
    id integer PRIMARY KEY,
    c  numeric(6,2)
);
INSERT INTO test_schema.rv_regmisc_ck_tight_type VALUES (1, 0.6);
ALTER TABLE test_schema.rv_regmisc_ck_tight_type
    ADD CONSTRAINT rv_regmisc_ck_tight_type_c CHECK (c <> 0.6) NOT ENFORCED;
-- The same under a parent's rewrite: a partition's own CHECK is checked by
-- the parent's recursing ALTER COLUMN ... TYPE, which runs before the
-- partition's alter. The relax (PG19) or the drop (PG18, and PG14-17 where
-- TO has no CHECK) of rv_regmisc_ckfam_p1_c must run ahead of every table
-- alter; the tightened rv_regmisc_ckfam_p2_c is validated after it.
CREATE TABLE test_schema.rv_regmisc_ckfam_p (
    id integer NOT NULL,
    c  numeric(6,2)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_regmisc_ckfam_p1 PARTITION OF test_schema.rv_regmisc_ckfam_p
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_regmisc_ckfam_p2 PARTITION OF test_schema.rv_regmisc_ckfam_p
    FOR VALUES FROM (100) TO (200);
INSERT INTO test_schema.rv_regmisc_ckfam_p VALUES (1, 0.6), (101, 0.6);
ALTER TABLE test_schema.rv_regmisc_ckfam_p1
    ADD CONSTRAINT rv_regmisc_ckfam_p1_c CHECK (c <> 1);
ALTER TABLE test_schema.rv_regmisc_ckfam_p2
    ADD CONSTRAINT rv_regmisc_ckfam_p2_c CHECK (c <> 0.6) NOT ENFORCED;

-- -----------------------------------------------------------------------------
-- pre-recreate: a table dropped and recreated (partition-key change, or a
-- plain table turned into a partitioned one) takes along everything that
-- went with its DROP TABLE, and the migration puts all of it back in the
-- same pass: every partition at every depth (also the ones whose own
-- definition is unchanged) with its index, grant, comment, statistics,
-- trigger and the view reading it; the sequence of its serial column (kept,
-- with its value and grant, by OWNED BY NONE before the drop); its own
-- foreign keys; and the foreign keys other tables hold against it, which
-- are dropped before and added back after. Every PostgreSQL version from 14
-- runs all of it. Rows sit in every partition.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_rv_recreate_reader') THEN
          CREATE ROLE pgc_pg19_rv_recreate_reader;
     END IF;
END;
$$;
CREATE FUNCTION test_schema.rv_recreate_touch() RETURNS trigger
    LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
CREATE TABLE test_schema.rv_recreate_regions (id integer PRIMARY KEY);
INSERT INTO test_schema.rv_recreate_regions VALUES (1), (2), (3);
-- RANGE (id) becomes RANGE (region_id). _p1 and _def keep their bound text
-- (their hash does not change), _p2 changes its bound and keeps its own
-- LIST (kind) sub-partitioning with the unchanged _p2a; TO adds _p2b.
CREATE TABLE test_schema.rv_recreate_orders (
    id        serial  NOT NULL,
    region_id integer NOT NULL
        CONSTRAINT rv_recreate_orders_region_fk REFERENCES test_schema.rv_recreate_regions (id),
    kind      integer NOT NULL,
    amount    numeric(10,2),
    CONSTRAINT rv_recreate_orders_pkey PRIMARY KEY (id, region_id, kind)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_recreate_orders_p1 PARTITION OF test_schema.rv_recreate_orders
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_recreate_orders_p2 PARTITION OF test_schema.rv_recreate_orders
    FOR VALUES FROM (100) TO (200) PARTITION BY LIST (kind);
CREATE TABLE test_schema.rv_recreate_orders_p2a PARTITION OF test_schema.rv_recreate_orders_p2
    FOR VALUES IN (1);
CREATE TABLE test_schema.rv_recreate_orders_def PARTITION OF test_schema.rv_recreate_orders DEFAULT;
CREATE INDEX rv_recreate_orders_p1_amount_idx ON test_schema.rv_recreate_orders_p1 (amount);
COMMENT ON TABLE test_schema.rv_recreate_orders_p1 IS 'first hundred orders';
GRANT SELECT ON test_schema.rv_recreate_orders_p1 TO pgc_pg19_rv_recreate_reader;
GRANT USAGE ON SEQUENCE test_schema.rv_recreate_orders_id_seq TO pgc_pg19_rv_recreate_reader;
CREATE STATISTICS test_schema.rv_recreate_orders_p1_st ON id, amount FROM test_schema.rv_recreate_orders_p1;
-- A row trigger on the parent (PostgreSQL 15+ lists its clone on every
-- partition) and one of the unchanged partition's own.
CREATE TRIGGER rv_recreate_orders_touch BEFORE INSERT ON test_schema.rv_recreate_orders
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE TRIGGER rv_recreate_orders_p1_touch BEFORE UPDATE ON test_schema.rv_recreate_orders_p1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE VIEW test_schema.rv_recreate_orders_p1_v AS
    SELECT id, amount FROM test_schema.rv_recreate_orders_p1;
INSERT INTO test_schema.rv_recreate_orders (id, region_id, kind, amount)
    VALUES (1, 1, 1, 10.00), (150, 1, 1, 20.00), (500, 2, 1, 30.00);
SELECT setval('test_schema.rv_recreate_orders_id_seq', 500);
-- Foreign keys held against the recreated table: one unchanged, one that TO
-- turns ON DELETE CASCADE, and one against the unchanged partition itself.
-- Their rows leave the key NULL, so adding them back succeeds.
CREATE TABLE test_schema.rv_recreate_order_lines (
    id           integer PRIMARY KEY,
    order_id     integer,
    order_region integer,
    order_kind   integer,
    CONSTRAINT rv_recreate_order_lines_order_fk FOREIGN KEY (order_id, order_region, order_kind)
        REFERENCES test_schema.rv_recreate_orders (id, region_id, kind)
);
CREATE TABLE test_schema.rv_recreate_order_refunds (
    id           integer PRIMARY KEY,
    order_id     integer,
    order_region integer,
    order_kind   integer,
    CONSTRAINT rv_recreate_order_refunds_order_fk FOREIGN KEY (order_id, order_region, order_kind)
        REFERENCES test_schema.rv_recreate_orders (id, region_id, kind)
);
CREATE TABLE test_schema.rv_recreate_p1_notes (
    id           integer PRIMARY KEY,
    order_id     integer,
    order_region integer,
    order_kind   integer,
    CONSTRAINT rv_recreate_p1_notes_order_fk FOREIGN KEY (order_id, order_region, order_kind)
        REFERENCES test_schema.rv_recreate_orders_p1 (id, region_id, kind)
);
INSERT INTO test_schema.rv_recreate_order_lines VALUES (1, NULL, NULL, NULL);
INSERT INTO test_schema.rv_recreate_order_refunds VALUES (1, NULL, NULL, NULL);
INSERT INTO test_schema.rv_recreate_p1_notes VALUES (1, NULL, NULL, NULL);
-- A plain table with a bigserial key that TO partitions, referenced by
-- another table.
CREATE TABLE test_schema.rv_recreate_events (
    id         bigserial NOT NULL,
    created_on date      NOT NULL,
    payload    text,
    CONSTRAINT rv_recreate_events_pkey PRIMARY KEY (id, created_on)
);
CREATE TABLE test_schema.rv_recreate_event_tags (
    id         integer PRIMARY KEY,
    event_id   bigint,
    event_date date,
    CONSTRAINT rv_recreate_event_tags_event_fk FOREIGN KEY (event_id, event_date)
        REFERENCES test_schema.rv_recreate_events (id, created_on)
);
INSERT INTO test_schema.rv_recreate_events (created_on, payload)
    VALUES ('2024-03-01', 'a'), ('2024-09-01', 'b');
INSERT INTO test_schema.rv_recreate_event_tags VALUES (1, NULL, NULL);
-- Review fixes. A plain table named before the table it joins: the ATTACH
-- waits until rv_recreate_events is back, instead of running first and going
-- with its DROP TABLE, row and all.
CREATE TABLE test_schema.rv_recreate_aa_event_archive (
    id         bigint NOT NULL,
    created_on date   NOT NULL,
    payload    text
);
INSERT INTO test_schema.rv_recreate_aa_event_archive VALUES (1, '2023-06-01', 'archived');
-- A statement trigger on the parent and a row trigger of the unchanged
-- partition's own with the same name: PostgreSQL clones only row triggers,
-- so the partition's is created with it.
CREATE TRIGGER rv_recreate_orders_audit AFTER INSERT ON test_schema.rv_recreate_orders
    FOR EACH STATEMENT EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE TRIGGER rv_recreate_orders_audit AFTER INSERT ON test_schema.rv_recreate_orders_p1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
-- A public table that TO partitions, referenced from test_schema: the dump's
-- search_path reaches public, so the foreign key names it unqualified.
CREATE TABLE public.rv_recreate_pub_orders (
    id integer NOT NULL,
    k  integer NOT NULL,
    CONSTRAINT rv_recreate_pub_orders_pkey PRIMARY KEY (id, k)
);
INSERT INTO public.rv_recreate_pub_orders VALUES (1, 1);
CREATE TABLE test_schema.rv_recreate_pub_lines (
    id       integer PRIMARY KEY,
    order_id integer,
    order_k  integer,
    CONSTRAINT rv_recreate_pub_lines_order_fk FOREIGN KEY (order_id, order_k)
        REFERENCES public.rv_recreate_pub_orders (id, k)
);
INSERT INTO test_schema.rv_recreate_pub_lines VALUES (1, NULL, NULL);
-- A sub-partition that changes its own key under a parent that stays, and a
-- foreign key referencing that parent: PostgreSQL copies it onto the
-- sub-partition, where it blocks the DROP TABLE.
CREATE TABLE test_schema.rv_recreate_tree (
    id integer NOT NULL,
    k  integer NOT NULL,
    CONSTRAINT rv_recreate_tree_pkey PRIMARY KEY (id, k)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_recreate_tree_c PARTITION OF test_schema.rv_recreate_tree
    FOR VALUES FROM (0) TO (100) PARTITION BY LIST (id);
CREATE TABLE test_schema.rv_recreate_tree_c1 PARTITION OF test_schema.rv_recreate_tree_c
    FOR VALUES IN (1);
INSERT INTO test_schema.rv_recreate_tree VALUES (1, 1);
CREATE TABLE test_schema.rv_recreate_tree_refs (
    id  integer PRIMARY KEY,
    tid integer,
    tk  integer,
    CONSTRAINT rv_recreate_tree_refs_fk FOREIGN KEY (tid, tk)
        REFERENCES test_schema.rv_recreate_tree (id, k)
);
INSERT INTO test_schema.rv_recreate_tree_refs VALUES (1, NULL, NULL);
-- Mixed-case names: the foreign key held against the recreated table is
-- added back with its quoted identifiers as they are, the partition only TO
-- has waits for its quoted parent, and the parent's row trigger is not
-- created a second time on the partitions.
CREATE TABLE test_schema."rv_recreate_Cased" (
    "Id" integer NOT NULL,
    "K"  integer NOT NULL,
    CONSTRAINT "rv_recreate_Cased_pkey" PRIMARY KEY ("Id", "K")
) PARTITION BY RANGE ("Id");
CREATE TABLE test_schema."rv_recreate_Cased_P1" PARTITION OF test_schema."rv_recreate_Cased"
    FOR VALUES FROM (0) TO (100);
CREATE TRIGGER "rv_recreate_Cased_Trg" BEFORE INSERT ON test_schema."rv_recreate_Cased"
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
INSERT INTO test_schema."rv_recreate_Cased" VALUES (1, 1);
CREATE TABLE test_schema."rv_recreate_CasedRefs" (
    "Id"      integer PRIMARY KEY,
    "OrderId" integer,
    "K"       integer,
    CONSTRAINT "rv_recreate_Cased_FK" FOREIGN KEY ("OrderId", "K")
        REFERENCES test_schema."rv_recreate_Cased" ("Id", "K")
);
INSERT INTO test_schema."rv_recreate_CasedRefs" VALUES (1, NULL, NULL);

-- pre-columns (rv_pcols_*): see data/test/README.md.
-- (1) A column a generated column reads changes type. PostgreSQL refuses
-- ALTER COLUMN ... TYPE under a generated column (STORED or VIRTUAL), so the
-- generated column is dropped first and re-added after, with its index, its
-- CHECK, the statistics and the view on it restored.
CREATE TABLE test_schema.rv_pcols_gen (
    id   integer PRIMARY KEY,
    a    integer NOT NULL,
    g    integer GENERATED ALWAYS AS (a * 2) STORED,
    flag integer,
    CONSTRAINT rv_pcols_gen_g_check CHECK (g >= 0)
);
CREATE INDEX rv_pcols_gen_g_idx ON test_schema.rv_pcols_gen (g);
CREATE STATISTICS test_schema.rv_pcols_gen_st (ndistinct) ON g, flag FROM test_schema.rv_pcols_gen;
CREATE VIEW test_schema.rv_pcols_gen_v AS SELECT id, g FROM test_schema.rv_pcols_gen;
INSERT INTO test_schema.rv_pcols_gen (id, a, flag) VALUES (1, 5, 1), (2, 7, 2);
-- ... whose expression changes as well.
CREATE TABLE test_schema.rv_pcols_gen_expr (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 2) STORED
);
INSERT INTO test_schema.rv_pcols_gen_expr (id, a) VALUES (1, 5);
-- ... that becomes a plain column and keeps its values (DROP EXPRESSION).
CREATE TABLE test_schema.rv_pcols_gen_plain (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 2) STORED
);
INSERT INTO test_schema.rv_pcols_gen_plain (id, a) VALUES (1, 5);
-- ... that goes away.
CREATE TABLE test_schema.rv_pcols_gen_gone (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 2) STORED
);
-- Both the column and the generated column reading it go away: the
-- generated column is dropped first.
CREATE TABLE test_schema.rv_pcols_gen_both (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 2) STORED
);
-- A partitioned table: the parent's drop, retype and add recurse.
CREATE TABLE test_schema.rv_pcols_gen_part (
    id integer NOT NULL,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 10) STORED
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pcols_gen_part_1 PARTITION OF test_schema.rv_pcols_gen_part
    FOR VALUES FROM (0) TO (100);
CREATE INDEX rv_pcols_gen_part_g_idx ON test_schema.rv_pcols_gen_part (g);
INSERT INTO test_schema.rv_pcols_gen_part (id, a) VALUES (1, 2);
-- VIRTUAL (PostgreSQL 18+; the table fails on both sides before).
CREATE TABLE test_schema.rv_pcols_gen_virt (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 2) VIRTUAL
);
INSERT INTO test_schema.rv_pcols_gen_virt (id, a) VALUES (1, 5);

-- (2) Extended statistics print their plain columns in attnum order. Here
-- `b` was dropped and re-added, so it sits after `c`: the same objects print
-- `ON c, b` here and `ON b, c` in TO, which is not a change. A real change
-- of the column set is still a drop + create.
CREATE TABLE test_schema.rv_pcols_st (a integer, b integer, c integer);
ALTER TABLE test_schema.rv_pcols_st DROP COLUMN b;
ALTER TABLE test_schema.rv_pcols_st ADD COLUMN b integer;
CREATE STATISTICS test_schema.rv_pcols_st_same (ndistinct) ON b, c FROM test_schema.rv_pcols_st;
CREATE STATISTICS test_schema.rv_pcols_st_expr (ndistinct, mcv)
    ON b, lower(c::text), c FROM test_schema.rv_pcols_st;
CREATE STATISTICS test_schema.rv_pcols_st_changed (ndistinct) ON b, c FROM test_schema.rv_pcols_st;

-- (3) CHECK drop + re-add in an inheritance family with merged child copies
-- (local and inherited): every copy is dropped, parents first, before any is
-- re-added. The expression change runs on every version; the enforceability
-- flips need PostgreSQL 18 (NOT ENFORCED at CREATE), where they are a drop +
-- re-add, and are in place on 19.
CREATE TABLE test_schema.rv_pcols_ckx_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckx_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.rv_pcols_ckx_ch (
    CONSTRAINT rv_pcols_ckx_amount CHECK (amount > 0)
) INHERITS (test_schema.rv_pcols_ckx_par);
CREATE TABLE test_schema.rv_pcols_ckx_ch2 () INHERITS (test_schema.rv_pcols_ckx_par);
CREATE TABLE test_schema.rv_pcols_ckx_gch (
    CONSTRAINT rv_pcols_ckx_amount CHECK (amount > 0)
) INHERITS (test_schema.rv_pcols_ckx_ch);
-- A merged copy that TO only inherits.
CREATE TABLE test_schema.rv_pcols_cku_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_cku_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.rv_pcols_cku_ch (
    CONSTRAINT rv_pcols_cku_amount CHECK (amount > 0)
) INHERITS (test_schema.rv_pcols_cku_par);
-- Relaxed on parent and child.
CREATE TABLE test_schema.rv_pcols_ckr_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckr_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.rv_pcols_ckr_ch (
    CONSTRAINT rv_pcols_ckr_amount CHECK (amount > 0)
) INHERITS (test_schema.rv_pcols_ckr_par);
-- Tightened on parent and child (PostgreSQL 18+ here).
CREATE TABLE test_schema.rv_pcols_ckt_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckt_amount CHECK (amount > 0) NOT ENFORCED
);
CREATE TABLE test_schema.rv_pcols_ckt_ch (
    CONSTRAINT rv_pcols_ckt_amount CHECK (amount > 0) NOT ENFORCED
) INHERITS (test_schema.rv_pcols_ckt_par);
-- Only the child's copy is relaxed; the parent's stays NOT ENFORCED
-- (PostgreSQL 18+ here).
CREATE TABLE test_schema.rv_pcols_ckc_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckc_amount CHECK (amount > 0) NOT ENFORCED
);
CREATE TABLE test_schema.rv_pcols_ckc_ch (
    CONSTRAINT rv_pcols_ckc_amount CHECK (amount > 0)
) INHERITS (test_schema.rv_pcols_ckc_par);

-- (4) Partition clones of a parent's row trigger belong to the parent's
-- trigger: never created or dropped on their own, only their own enabled
-- state and comment are set.
CREATE FUNCTION test_schema.rv_pcols_trg_fn() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
-- Exists on both sides: TO adds a row trigger, changes another one's
-- definition, and gives a clone a state and a comment of its own.
CREATE TABLE test_schema.rv_pcols_trg_ex (
    id integer NOT NULL,
    k  integer NOT NULL
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pcols_trg_ex_1 PARTITION OF test_schema.rv_pcols_trg_ex
    FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (k);
CREATE TABLE test_schema.rv_pcols_trg_ex_1a PARTITION OF test_schema.rv_pcols_trg_ex_1
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_pcols_trg_ex_2 PARTITION OF test_schema.rv_pcols_trg_ex
    FOR VALUES FROM (100) TO (200);
CREATE TRIGGER rv_pcols_trg_changed BEFORE INSERT ON test_schema.rv_pcols_trg_ex
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TRIGGER rv_pcols_trg_kept BEFORE UPDATE ON test_schema.rv_pcols_trg_ex
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
ALTER TABLE test_schema.rv_pcols_trg_ex_2 DISABLE TRIGGER rv_pcols_trg_changed;
COMMENT ON TRIGGER rv_pcols_trg_kept ON test_schema.rv_pcols_trg_ex_2 IS 'rv_pcols: old clone comment';
-- Goes away; its partition sorts before it.
CREATE TABLE test_schema.rv_pcols_trg_zgone (id integer NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pcols_trg_agone PARTITION OF test_schema.rv_pcols_trg_zgone
    FOR VALUES FROM (0) TO (100);
CREATE TRIGGER rv_pcols_trg_gone AFTER INSERT ON test_schema.rv_pcols_trg_zgone
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();

-- pre-columns review fixes (rv_pcols_*): see data/test/README.md.
-- (5) INHERITS: the parent retypes a column its children inherit. A child
-- cannot retype an inherited column itself ("cannot alter inherited
-- column"), and the parent's recursing ALTER COLUMN ... TYPE is refused while
-- a child's own generated column reads it: that column is dropped ahead of
-- every table alter and re-added in the child's alter.
CREATE TABLE test_schema.rv_pcols_ihp (id integer, a integer);
CREATE TABLE test_schema.rv_pcols_ihc (
    g integer GENERATED ALWAYS AS (a * 2) STORED
) INHERITS (test_schema.rv_pcols_ihp);
CREATE TABLE test_schema.rv_pcols_ihgc (
    h integer GENERATED ALWAYS AS (a + 1) STORED
) INHERITS (test_schema.rv_pcols_ihc);
CREATE TABLE test_schema.rv_pcols_ihc2 (x integer) INHERITS (test_schema.rv_pcols_ihp);
CREATE INDEX rv_pcols_ihc_g_idx ON test_schema.rv_pcols_ihc (g);
INSERT INTO test_schema.rv_pcols_ihc (id, a) VALUES (1, 4);
INSERT INTO test_schema.rv_pcols_ihgc (id, a) VALUES (2, 5);
-- (6) What blocks the drop of a generated column re-created around a retype:
-- a policy, UPDATE OF / WHEN triggers, another table's foreign key on a
-- mixed-case unique key, and (PostgreSQL 18+) a named NOT NULL constraint.
CREATE TABLE test_schema.rv_pcols_blk (
    id    integer PRIMARY KEY,
    a     integer,
    "G x" integer GENERATED ALWAYS AS (a * 2) STORED CONSTRAINT rv_pcols_blk_gx_nn NOT NULL,
    CONSTRAINT "rv_pcols_blk_G_key" UNIQUE ("G x")
);
ALTER TABLE test_schema.rv_pcols_blk ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_pcols_blk_pol ON test_schema.rv_pcols_blk USING ("G x" > 0);
CREATE TRIGGER rv_pcols_blk_upd AFTER UPDATE OF "G x" ON test_schema.rv_pcols_blk
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TRIGGER rv_pcols_blk_when AFTER UPDATE ON test_schema.rv_pcols_blk
    FOR EACH ROW WHEN (old."G x" IS DISTINCT FROM new."G x")
    EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
ALTER TABLE test_schema.rv_pcols_blk DISABLE TRIGGER rv_pcols_blk_when;
CREATE TABLE test_schema.rv_pcols_blk_ref (
    id integer,
    gx integer REFERENCES test_schema.rv_pcols_blk ("G x")
);
INSERT INTO test_schema.rv_pcols_blk (id, a) VALUES (1, 3);
INSERT INTO test_schema.rv_pcols_blk_ref VALUES (1, 6);
-- A partitioned table's UPDATE OF trigger, its clones' own states, and a
-- partition's own trigger on the column.
CREATE TABLE test_schema.rv_pcols_blkp (
    id integer,
    k  integer,
    a  integer,
    g  integer GENERATED ALWAYS AS (a * 2) STORED
) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_pcols_blkp_1 PARTITION OF test_schema.rv_pcols_blkp FOR VALUES IN (1);
CREATE TABLE test_schema.rv_pcols_blkp_2 PARTITION OF test_schema.rv_pcols_blkp FOR VALUES IN (2);
CREATE TRIGGER rv_pcols_blkp_upd AFTER UPDATE OF g ON test_schema.rv_pcols_blkp
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
ALTER TABLE test_schema.rv_pcols_blkp_2 DISABLE TRIGGER rv_pcols_blkp_upd;
COMMENT ON TRIGGER rv_pcols_blkp_upd ON test_schema.rv_pcols_blkp_1 IS 'rv_pcols: blkp clone comment';
CREATE TRIGGER rv_pcols_blkp_own AFTER UPDATE OF g ON test_schema.rv_pcols_blkp_1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
-- (7) Partitions whose parent is outside the dumped schemas: every clone is
-- dumped, and compared with the other side's.
CREATE SCHEMA IF NOT EXISTS rv_pcols_outside;
CREATE FUNCTION rv_pcols_outside.rv_pcols_out_fn() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TABLE rv_pcols_outside.rv_pcols_out (id integer NOT NULL, k integer NOT NULL)
    PARTITION BY LIST (k);
CREATE TRIGGER rv_pcols_out_ins AFTER INSERT ON rv_pcols_outside.rv_pcols_out
    FOR EACH ROW EXECUTE FUNCTION rv_pcols_outside.rv_pcols_out_fn();
CREATE TRIGGER rv_pcols_out_upd AFTER UPDATE ON rv_pcols_outside.rv_pcols_out
    FOR EACH ROW EXECUTE FUNCTION rv_pcols_outside.rv_pcols_out_fn();
CREATE TABLE test_schema.rv_pcols_out_1 PARTITION OF rv_pcols_outside.rv_pcols_out
    FOR VALUES IN (1, 11) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_pcols_out_1a PARTITION OF test_schema.rv_pcols_out_1 FOR VALUES IN (1);
CREATE TABLE test_schema.rv_pcols_out_2 PARTITION OF rv_pcols_outside.rv_pcols_out FOR VALUES IN (2);
CREATE TABLE test_schema.rv_pcols_out_gone PARTITION OF rv_pcols_outside.rv_pcols_out FOR VALUES IN (5);
ALTER TABLE test_schema.rv_pcols_out_1 DISABLE TRIGGER rv_pcols_out_ins;
COMMENT ON TRIGGER rv_pcols_out_upd ON test_schema.rv_pcols_out_2 IS 'rv_pcols: old orphan clone comment';
-- (8) A table with a trigger of its own named like the row trigger of the
-- partitioned table it becomes a partition of: its trigger is dropped before
-- the ATTACH PARTITION, which clones the parent's.
CREATE TABLE test_schema.rv_pcols_att (id integer NOT NULL, k integer NOT NULL) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_pcols_att_1 PARTITION OF test_schema.rv_pcols_att FOR VALUES IN (1);
CREATE TRIGGER rv_pcols_att_trg AFTER INSERT ON test_schema.rv_pcols_att
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TABLE test_schema.rv_pcols_att_2 (id integer NOT NULL, k integer NOT NULL);
CREATE TRIGGER rv_pcols_att_trg AFTER INSERT ON test_schema.rv_pcols_att_2
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();

-- r3-gencol review fixes (rv_r3g_*): see data/test/README.md.
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_r3g_reader') THEN
          CREATE ROLE pgc_pg19_r3g_reader;
     END IF;
END;
$$;
CREATE FUNCTION test_schema.rv_r3g_trg() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
-- (1) A routine whose DROP ... CASCADE takes a stored generated column
-- calling it: TO changes rv_r3g_fn's return type (and the column's type with
-- it) and renames rv_r3g_ren_fn's parameter. The CASCADE takes the views over
-- the column (and a view over one of them), its privileges, its UPDATE OF /
-- WHEN triggers, a SQL-standard routine reading it, the publication rows
-- whose row filter (PostgreSQL 15+) or column list (18+) uses it, and on a
-- partition its own index, policy, trigger and privileges on its copy.
CREATE FUNCTION test_schema.rv_r3g_fn(x integer) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT $1 * 2 $$;
CREATE TABLE test_schema.rv_r3g_items (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_fn(a)) STORED
);
INSERT INTO test_schema.rv_r3g_items (id, a) VALUES (1, 3);
CREATE VIEW test_schema.rv_r3g_items_v AS SELECT id, g FROM test_schema.rv_r3g_items;
CREATE VIEW test_schema.rv_r3g_items_vv AS SELECT id FROM test_schema.rv_r3g_items_v;
GRANT SELECT ON test_schema.rv_r3g_items_v TO pgc_pg19_r3g_reader;
GRANT SELECT (g) ON test_schema.rv_r3g_items TO pgc_pg19_r3g_reader;
CREATE TRIGGER rv_r3g_items_upd AFTER UPDATE OF g ON test_schema.rv_r3g_items
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE TRIGGER rv_r3g_items_when AFTER UPDATE ON test_schema.rv_r3g_items
    FOR EACH ROW WHEN (old.g IS DISTINCT FROM new.g)
    EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE FUNCTION test_schema.rv_r3g_items_sum() RETURNS numeric
    LANGUAGE sql BEGIN ATOMIC SELECT sum(g) FROM test_schema.rv_r3g_items; END;
CREATE PUBLICATION rv_r3g_pub_filter FOR TABLE test_schema.rv_r3g_items WHERE (g > 0);
CREATE PUBLICATION rv_r3g_pub_cols FOR TABLE test_schema.rv_r3g_items (id, g)
    WITH (publish_generated_columns = stored);
CREATE TABLE test_schema.rv_r3g_part (
    id integer NOT NULL,
    k  integer NOT NULL,
    a  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_fn(a)) STORED
) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_r3g_part_1 PARTITION OF test_schema.rv_r3g_part FOR VALUES IN (1);
INSERT INTO test_schema.rv_r3g_part (id, k, a) VALUES (1, 1, 4);
CREATE INDEX rv_r3g_part_1_g ON test_schema.rv_r3g_part_1 (g);
ALTER TABLE test_schema.rv_r3g_part_1 ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_part_1_pol ON test_schema.rv_r3g_part_1 USING (g > 0);
CREATE TRIGGER rv_r3g_part_1_upd AFTER UPDATE OF g ON test_schema.rv_r3g_part_1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
GRANT SELECT (g) ON test_schema.rv_r3g_part_1 TO pgc_pg19_r3g_reader;
CREATE VIEW test_schema.rv_r3g_part_1_v AS SELECT id, g FROM test_schema.rv_r3g_part_1;
CREATE PUBLICATION rv_r3g_pub_part FOR TABLE test_schema.rv_r3g_part_1 WHERE (g > 0);
CREATE FUNCTION test_schema.rv_r3g_ren_fn(x integer) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT $1 + 1 $$;
CREATE TABLE test_schema.rv_r3g_ren (
    id integer PRIMARY KEY,
    a  integer,
    h  integer GENERATED ALWAYS AS (test_schema.rv_r3g_ren_fn(a)) STORED
);
CREATE VIEW test_schema.rv_r3g_ren_v AS SELECT id, h FROM test_schema.rv_r3g_ren;
GRANT SELECT (h) ON test_schema.rv_r3g_ren TO pgc_pg19_r3g_reader;
CREATE TRIGGER rv_r3g_ren_upd AFTER UPDATE OF h ON test_schema.rv_r3g_ren
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
-- The same through a SQL-standard routine drop_views drops ahead of the
-- tables (it reads a column TO retypes).
CREATE TABLE test_schema.rv_r3g_cfg (id integer PRIMARY KEY, n integer);
CREATE FUNCTION test_schema.rv_r3g_k(x integer) RETURNS integer
    LANGUAGE sql IMMUTABLE
    BEGIN ATOMIC SELECT x + coalesce((SELECT max(n) FROM test_schema.rv_r3g_cfg), 0)::integer; END;
CREATE TABLE test_schema.rv_r3g_kt (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_k(a)) STORED
);
CREATE VIEW test_schema.rv_r3g_kt_v AS SELECT id, g FROM test_schema.rv_r3g_kt;
GRANT SELECT (g) ON test_schema.rv_r3g_kt TO pgc_pg19_r3g_reader;
CREATE TRIGGER rv_r3g_kt_upd AFTER UPDATE OF g ON test_schema.rv_r3g_kt
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE PUBLICATION rv_r3g_pub_kt FOR TABLE test_schema.rv_r3g_kt WHERE (g > 0);
-- And with a retype of the column the generated column reads in the same
-- migration: the view is dropped for the retype too, and re-created only
-- once the column is back.
CREATE FUNCTION test_schema.rv_r3g_f16(x bigint) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT (x * 2)::integer $$;
CREATE TABLE test_schema.rv_r3g_c16 (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_f16(a)) STORED
);
INSERT INTO test_schema.rv_r3g_c16 (id, a) VALUES (1, 5);
CREATE VIEW test_schema.rv_r3g_c16_v AS SELECT id, g FROM test_schema.rv_r3g_c16;
CREATE PUBLICATION rv_r3g_pub_c16 FOR TABLE test_schema.rv_r3g_c16 WHERE (g > 0);
-- (2) Publication rows using a generated column that is dropped and re-added
-- because TO retypes a column it reads, on a table and on a partition.
CREATE TABLE test_schema.rv_r3g_pubrt (
    id integer PRIMARY KEY,
    a  integer,
    v  integer GENERATED ALWAYS AS (a * 2) STORED
);
CREATE PUBLICATION rv_r3g_pub_rt_filter FOR TABLE test_schema.rv_r3g_pubrt WHERE (v > 0);
CREATE PUBLICATION rv_r3g_pub_rt_cols FOR TABLE test_schema.rv_r3g_pubrt (id, v)
    WITH (publish_generated_columns = stored);
CREATE TABLE test_schema.rv_r3g_pubrtp (
    id integer NOT NULL,
    k  integer NOT NULL,
    a  integer,
    b  integer,
    g  integer GENERATED ALWAYS AS (a + b) STORED,
    PRIMARY KEY (id, k)
) PARTITION BY RANGE (k);
CREATE TABLE test_schema.rv_r3g_pubrtp_1 PARTITION OF test_schema.rv_r3g_pubrtp
    FOR VALUES FROM (0) TO (10);
CREATE PUBLICATION rv_r3g_pub_rtp FOR TABLE test_schema.rv_r3g_pubrtp_1 WHERE (g > 0);
-- (3) A partition's own index, CHECK, UNIQUE (with a foreign key referencing
-- it), statistics and policy on the copy of a generated column its parent
-- drops and re-adds around the retype of a column it reads.
CREATE TABLE test_schema.rv_r3g_pdep (
    id integer NOT NULL,
    k  integer NOT NULL,
    a  integer,
    b  integer,
    g  integer GENERATED ALWAYS AS (a + b) STORED,
    PRIMARY KEY (id, k)
) PARTITION BY RANGE (k);
CREATE TABLE test_schema.rv_r3g_pdep_1 PARTITION OF test_schema.rv_r3g_pdep
    FOR VALUES FROM (0) TO (10);
INSERT INTO test_schema.rv_r3g_pdep (id, k, a, b) VALUES (1, 1, 2, 3);
CREATE INDEX rv_r3g_pdep_1_g ON test_schema.rv_r3g_pdep_1 (g);
ALTER TABLE test_schema.rv_r3g_pdep_1 ADD CONSTRAINT rv_r3g_pdep_1_chk CHECK (g > 0);
ALTER TABLE test_schema.rv_r3g_pdep_1 ADD CONSTRAINT rv_r3g_pdep_1_uk UNIQUE (id, g);
CREATE STATISTICS test_schema.rv_r3g_pdep_1_st ON g, id FROM test_schema.rv_r3g_pdep_1;
ALTER TABLE test_schema.rv_r3g_pdep_1 ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_pdep_1_pol ON test_schema.rv_r3g_pdep_1 AS RESTRICTIVE USING (g > 0);
CREATE TABLE test_schema.rv_r3g_pdep_ref (
    id integer,
    g  integer,
    FOREIGN KEY (id, g) REFERENCES test_schema.rv_r3g_pdep_1 (id, g)
);
-- (4) PostgreSQL 14: a generated column over a function (and an operator)
-- that stays, whose type TO changes. PostgreSQL 14 refuses ALTER COLUMN ...
-- TYPE on it, so the column is dropped and re-added there, with its index,
-- view, trigger and privileges; later servers retype it in place.
CREATE FUNCTION test_schema.rv_r3g_keep_fn(x integer) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT x * 3 $$;
CREATE FUNCTION test_schema.rv_r3g_plus2(a integer, b integer) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 2 $$;
CREATE OPERATOR test_schema.#@# (LEFTARG = integer, RIGHTARG = integer,
    FUNCTION = test_schema.rv_r3g_plus2);
CREATE TABLE test_schema.rv_r3g_pg14 (
    id integer PRIMARY KEY,
    v  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_keep_fn(v)) STORED,
    o  integer GENERATED ALWAYS AS (v OPERATOR(test_schema.#@#) 1) STORED,
    b  integer GENERATED ALWAYS AS (abs(v)) STORED
);
INSERT INTO test_schema.rv_r3g_pg14 (id, v) VALUES (1, 5);
CREATE INDEX rv_r3g_pg14_g ON test_schema.rv_r3g_pg14 (g);
CREATE VIEW test_schema.rv_r3g_pg14_v AS SELECT id, g, o FROM test_schema.rv_r3g_pg14;
GRANT SELECT (g) ON test_schema.rv_r3g_pg14 TO pgc_pg19_r3g_reader;
-- (5) ALTER COLUMN ... TYPE of a column an RLS policy or a trigger (UPDATE
-- OF, WHEN) uses: they are dropped ahead of the retype and created again.
CREATE TABLE test_schema.rv_r3g_rt_docs (id integer PRIMARY KEY, tenant_id integer, body text);
ALTER TABLE test_schema.rv_r3g_rt_docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_docs_pol ON test_schema.rv_r3g_rt_docs
    USING (tenant_id = current_setting('pgc.rv_r3g_tenant', true)::integer);
CREATE TABLE test_schema.rv_r3g_rt_orders (id integer PRIMARY KEY, status text);
CREATE TRIGGER rv_r3g_rt_orders_upd BEFORE UPDATE OF status ON test_schema.rv_r3g_rt_orders
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE TABLE test_schema.rv_r3g_rt_ev (id integer PRIMARY KEY, kind text);
CREATE TRIGGER rv_r3g_rt_ev_when BEFORE UPDATE ON test_schema.rv_r3g_rt_ev
    FOR EACH ROW WHEN (new.kind IS DISTINCT FROM old.kind)
    EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE TABLE test_schema.rv_r3g_rt_part (id integer NOT NULL, k integer NOT NULL, tenant_id integer)
    PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_r3g_rt_part_1 PARTITION OF test_schema.rv_r3g_rt_part FOR VALUES IN (1);
ALTER TABLE test_schema.rv_r3g_rt_part ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_part_pol ON test_schema.rv_r3g_rt_part USING (tenant_id > 0);
ALTER TABLE test_schema.rv_r3g_rt_part_1 ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_part_1_pol ON test_schema.rv_r3g_rt_part_1 USING (tenant_id <> 7);
CREATE TRIGGER rv_r3g_rt_part_upd AFTER UPDATE OF tenant_id ON test_schema.rv_r3g_rt_part
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE TRIGGER rv_r3g_rt_part_1_upd AFTER UPDATE OF tenant_id ON test_schema.rv_r3g_rt_part_1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
-- (6) Review fixes. What calls a SQL-standard routine that reads
-- rv_r3g_items.g (taken with the column by rv_r3g_fn's CASCADE, re-created
-- after the re-add) — a column default, a CHECK and a policy — comes back
-- only once that routine is back; so does a CHECK calling a string-body
-- reader whose return type changes, which is dropped with CASCADE only after
-- the re-add.
CREATE FUNCTION test_schema.rv_r3g_items_max() RETURNS integer
    LANGUAGE sql STABLE
    BEGIN ATOMIC SELECT coalesce(max(g), 0)::integer FROM test_schema.rv_r3g_items; END;
CREATE TABLE test_schema.rv_r3g_use (
    id integer PRIMARY KEY,
    v  integer DEFAULT test_schema.rv_r3g_items_max(),
    w  integer,
    CONSTRAINT rv_r3g_use_chk CHECK (w <= test_schema.rv_r3g_items_max() + 1000)
);
INSERT INTO test_schema.rv_r3g_use (id, w) VALUES (1, 5);
ALTER TABLE test_schema.rv_r3g_use ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_use_pol ON test_schema.rv_r3g_use
    USING (w <= test_schema.rv_r3g_items_max() + 1000);
CREATE FUNCTION test_schema.rv_r3g_items_top() RETURNS integer
    LANGUAGE sql STABLE
    AS $$ SELECT coalesce(max(g), 0)::integer FROM test_schema.rv_r3g_items $$;
CREATE TABLE test_schema.rv_r3g_use2 (
    id integer PRIMARY KEY,
    w  bigint,
    CONSTRAINT rv_r3g_use2_chk CHECK (w <= test_schema.rv_r3g_items_top() + 1000)
);
-- A materialized view reading the re-added column, with extended
-- statistics: TO also adds an operator and a SQL-standard routine using it,
-- which holds the routines back until after the operators, the view with
-- them; its statistics follow it there.
CREATE MATERIALIZED VIEW test_schema.rv_r3g_items_mv AS
    SELECT id, g FROM test_schema.rv_r3g_items;
CREATE STATISTICS test_schema.rv_r3g_items_mv_st ON id, g FROM test_schema.rv_r3g_items_mv;
-- What only TO has on a generated column rv_r3g_fn's CASCADE takes: the
-- table phase creates it first, the CASCADE takes it again, and it comes back
-- with the re-add.
CREATE TABLE test_schema.rv_r3g_newdep (
    id integer PRIMARY KEY,
    a  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_fn(a)) STORED
);
INSERT INTO test_schema.rv_r3g_newdep (id, a) VALUES (1, 3);
-- A policy of another table reading rv_r3g_rt_docs.tenant_id through a
-- subquery, which TO changes; the table is altered before rv_r3g_rt_docs, so
-- its alter re-created the policy ahead of the retype.
CREATE TABLE test_schema.rv_r3g_rt_a_reader (id integer PRIMARY KEY, x integer);
ALTER TABLE test_schema.rv_r3g_rt_a_reader ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_a_reader_pol ON test_schema.rv_r3g_rt_a_reader
    USING (EXISTS (SELECT 1 FROM test_schema.rv_r3g_rt_docs d WHERE d.tenant_id = x));
-- PostgreSQL 14: generated columns over a function made plain. There the
-- column keeps depending on the function after DROP EXPRESSION, so a type
-- change (g) still fails, and dropping the function (rv_r3g_gone_fn, h) with
-- CASCADE takes the column and its data. The migration rewrites such a
-- column, keeping its values; later servers use ALTER TYPE / DROP
-- EXPRESSION.
CREATE FUNCTION test_schema.rv_r3g_gone_fn(x integer) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT x * 5 $$;
CREATE TABLE test_schema.rv_r3g_plain (
    id integer PRIMARY KEY,
    v  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_keep_fn(v)) STORED,
    h  integer GENERATED ALWAYS AS (test_schema.rv_r3g_gone_fn(v)) STORED
);
INSERT INTO test_schema.rv_r3g_plain (id, v) VALUES (1, 5);
CREATE INDEX rv_r3g_plain_g ON test_schema.rv_r3g_plain (g);
CREATE VIEW test_schema.rv_r3g_plain_v AS SELECT id, g, h FROM test_schema.rv_r3g_plain;
GRANT SELECT (g, h) ON test_schema.rv_r3g_plain TO pgc_pg19_r3g_reader;
COMMENT ON COLUMN test_schema.rv_r3g_plain.h IS 'rv_r3g kept';
CREATE TABLE test_schema.rv_r3g_plainp (
    id integer NOT NULL,
    k  integer NOT NULL,
    v  integer,
    g  integer GENERATED ALWAYS AS (test_schema.rv_r3g_keep_fn(v)) STORED
) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_r3g_plainp_1 PARTITION OF test_schema.rv_r3g_plainp FOR VALUES IN (1);
INSERT INTO test_schema.rv_r3g_plainp (id, k, v) VALUES (1, 1, 2);
CREATE INDEX rv_r3g_plainp_1_g ON test_schema.rv_r3g_plainp_1 (g);

-- -----------------------------------------------------------------------------
-- r3-constraints (rv_r3c_*): CHECK copies a table only inherits, ATTACH /
-- DETACH PARTITION with the parents' CHECKs, and NOT NULL name drift (FROM side)
-- -----------------------------------------------------------------------------
-- (1) Enforceability of CHECK copies a table only inherits (conislocal =
-- false). PostgreSQL 19 lets such a copy stay ENFORCED under a NOT ENFORCED
-- parent (ALTER TABLE ONLY ... ENFORCED); pgc dumps that state and puts it
-- in place after every parent's statement. The NOT ENFORCED CHECKs are added
-- by a separate ALTER, so on PG14-17 only that statement fails (on both
-- sides) and the tables stay alike; on PG18 the ENFORCED flips fail on both
-- sides, and only the parent's NOT ENFORCED change remains.
-- rv_r3c_ie_p:  enforced here; NOT ENFORCED in TO, whose INHERITS child
--               rv_r3c_ie_c keeps an enforced copy (in-place parent flip,
--               then ALTER TABLE ... ENFORCED on the child).
-- rv_r3c_ie_pt: NOT ENFORCED on both sides. Partition _1 follows it here and
--               keeps an enforced copy in TO; _2 keeps one here and follows it
--               in TO (ALTER TABLE ... NOT ENFORCED); TO-only _3 keeps one.
CREATE TABLE test_schema.rv_r3c_ie_p (id integer, a integer, CONSTRAINT rv_r3c_ie_p_ck CHECK (a > 0));
CREATE TABLE test_schema.rv_r3c_ie_c () INHERITS (test_schema.rv_r3c_ie_p);
CREATE TABLE test_schema.rv_r3c_ie_pt (id integer, a integer) PARTITION BY RANGE (id);
ALTER TABLE test_schema.rv_r3c_ie_pt ADD CONSTRAINT rv_r3c_ie_pt_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie_pt_1 PARTITION OF test_schema.rv_r3c_ie_pt FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_ie_pt_2 PARTITION OF test_schema.rv_r3c_ie_pt FOR VALUES FROM (10) TO (20);
ALTER TABLE ONLY test_schema.rv_r3c_ie_pt_2 ALTER CONSTRAINT rv_r3c_ie_pt_ck ENFORCED;
INSERT INTO test_schema.rv_r3c_ie_c VALUES (1, 1);
INSERT INTO test_schema.rv_r3c_ie_pt VALUES (1, 1), (11, 2);
-- (2) Tables TO attaches to a partitioned parent with a CHECK. ATTACH
-- PARTITION needs the table to have the parent's CHECK already, under its
-- name and with its definition, and merges it into the copy the partition
-- inherits, which cannot be dropped afterwards.
-- rv_r3c_at_same: has the CHECK (merged as it is: no DROP after the attach)
-- rv_r3c_at_miss: lacks it (added before the attach)
-- rv_r3c_at_diff: has it with another definition (dropped and re-added first)
-- rv_r3c_at_like: LIKE ... INCLUDING CONSTRAINTS: the CHECK, and on PG18+ the
--                 parent-named NOT NULL rv_r3c_at_id_not_null, which the
--                 attach merges too
-- rv_r3c_atn_x:   an enforced copy of rv_r3c_atn's NOT ENFORCED CHECK
--                 (PG18+), NOT ENFORCED in TO: altered (PG19) or dropped and
--                 re-added (PG18) before the attach
-- rv_r3c_mv_x:    moves from rv_r3c_mv_op to rv_r3c_mv_np; the detach leaves
--                 rv_r3c_mv_op's CHECKs on it as local ones: the one
--                 rv_r3c_mv_np has too merges, the other is dropped
-- rv_r3c_mv_dt:   detached for good, without CHECKs in TO; its PG18+ NOT
--                 NULL constraints keep rv_r3c_mv_op's names (a name drift)
CREATE TABLE test_schema.rv_r3c_at (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_at_ck CHECK (a > 0))
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_at_1 PARTITION OF test_schema.rv_r3c_at FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_at_same (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_at_ck CHECK (a > 0));
CREATE TABLE test_schema.rv_r3c_at_miss (id integer NOT NULL, a integer);
CREATE TABLE test_schema.rv_r3c_at_diff (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_at_ck CHECK (a > 1));
CREATE TABLE test_schema.rv_r3c_at_like (LIKE test_schema.rv_r3c_at INCLUDING CONSTRAINTS);
INSERT INTO test_schema.rv_r3c_at_same VALUES (10, 5);
INSERT INTO test_schema.rv_r3c_at_miss VALUES (20, 5);
INSERT INTO test_schema.rv_r3c_at_diff VALUES (30, 5);
INSERT INTO test_schema.rv_r3c_at_like VALUES (40, 5);
CREATE TABLE test_schema.rv_r3c_atn (id integer NOT NULL, a integer) PARTITION BY RANGE (id);
ALTER TABLE test_schema.rv_r3c_atn ADD CONSTRAINT rv_r3c_atn_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_atn_x (id integer NOT NULL, a integer);
ALTER TABLE test_schema.rv_r3c_atn_x ADD CONSTRAINT rv_r3c_atn_ck CHECK (a > 0);
CREATE TABLE test_schema.rv_r3c_mv_op (
    id integer NOT NULL,
    a  integer,
    CONSTRAINT rv_r3c_mv_ck CHECK (a > 0),
    CONSTRAINT rv_r3c_mv_old CHECK (a < 1000)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_mv_np (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_mv_ck CHECK (a > 0))
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_mv_x PARTITION OF test_schema.rv_r3c_mv_op FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_mv_dt PARTITION OF test_schema.rv_r3c_mv_op FOR VALUES FROM (10) TO (20);
INSERT INTO test_schema.rv_r3c_mv_op VALUES (1, 5), (11, 5);
-- (3) NOT NULL constraints (PG18+) whose name no longer follows
-- {table}_{column}_not_null: PostgreSQL keeps it through a column or table
-- rename. The TO column is NOT NULL under PostgreSQL's own name, so nothing
-- changes — a DROP CONSTRAINT would clear NOT NULL (and fails outright for a
-- primary-key or identity column). A name TO chose is renamed to.
CREATE TABLE test_schema.rv_r3c_nn_col (a integer NOT NULL, v text);
ALTER TABLE test_schema.rv_r3c_nn_col RENAME COLUMN a TO b;
CREATE TABLE test_schema.rv_r3c_nn_old (id integer CONSTRAINT rv_r3c_nn_tab_pk PRIMARY KEY, a integer NOT NULL);
ALTER TABLE test_schema.rv_r3c_nn_old RENAME TO rv_r3c_nn_tab;
CREATE TABLE test_schema.rv_r3c_nn_ident (x integer GENERATED ALWAYS AS IDENTITY, v text);
ALTER TABLE test_schema.rv_r3c_nn_ident RENAME COLUMN x TO y;
CREATE TABLE test_schema.rv_r3c_nn_named (b integer NOT NULL);
CREATE TABLE test_schema.rv_r3c_nn_custom (b integer CONSTRAINT rv_r3c_nn_custom_nn NOT NULL);
-- Review fixes (rv_r3c_*), FROM side.
-- (4) A NOT ENFORCED statement of the inherited-copy pass recurses into a
-- merged local copy (PG19): rv_r3c_ie3_c1 only inherits rv_r3c_ie3_p's NOT
-- ENFORCED CHECK and keeps an enforced copy here; TO lets it follow the
-- parent. Its child rv_r3c_ie3_c2 holds a merged, enforced copy on both
-- sides, which has to be enforced again after rv_r3c_ie3_c1's statement.
CREATE TABLE test_schema.rv_r3c_ie3_p (a integer);
ALTER TABLE test_schema.rv_r3c_ie3_p ADD CONSTRAINT rv_r3c_ie3_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie3_c1 () INHERITS (test_schema.rv_r3c_ie3_p);
ALTER TABLE ONLY test_schema.rv_r3c_ie3_c1 ALTER CONSTRAINT rv_r3c_ie3_ck ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie3_c2 (a integer) INHERITS (test_schema.rv_r3c_ie3_c1);
ALTER TABLE test_schema.rv_r3c_ie3_c2 ADD CONSTRAINT rv_r3c_ie3_ck CHECK (a > 0);
-- (5) Classical inheritance joined (INHERIT) and left (NO INHERIT), every
-- version. rv_r3c_inh_p (a CHECK) inherits rv_r3c_inh_g (a CHECK and a NOT
-- NULL column), and rv_r3c_inh_s only inherits both CHECKs.
-- rv_r3c_inh_merge: standalone here with both CHECKs; TO: an INHERITS child
--                   keeping them as merged copies
-- rv_r3c_inh_join:  standalone here without the NOT NULL, column b or the
--                   CHECKs; TO: an INHERITS child that only inherits them
--                   (the holders drop and re-add theirs around the INHERIT)
-- rv_r3c_inh_keep:  an INHERITS child here; TO: standalone with both CHECKs
-- rv_r3c_inh_drop:  an INHERITS child here; TO: standalone, no CHECKs, id
--                   nullable
-- rv_r3c_inh_leave: an INHERITS child here with a merged copy of
--                   rv_r3c_inh_p_ck; TO: standalone without CHECKs (the
--                   copy is dropped after the NO INHERIT, on its own)
-- rv_r3c_inh_r:     INHERITS (q1, q2) here, (q2, q1) in TO
CREATE TABLE test_schema.rv_r3c_inh_g (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_inh_g_ck CHECK (a > 0));
CREATE TABLE test_schema.rv_r3c_inh_p (b integer, CONSTRAINT rv_r3c_inh_p_ck CHECK (b > 0))
    INHERITS (test_schema.rv_r3c_inh_g);
CREATE TABLE test_schema.rv_r3c_inh_s () INHERITS (test_schema.rv_r3c_inh_p);
INSERT INTO test_schema.rv_r3c_inh_s VALUES (1, 1, 1);
CREATE TABLE test_schema.rv_r3c_inh_merge (
    id integer NOT NULL,
    a  integer,
    b  integer,
    CONSTRAINT rv_r3c_inh_g_ck CHECK (a > 0),
    CONSTRAINT rv_r3c_inh_p_ck CHECK (b > 0)
);
CREATE TABLE test_schema.rv_r3c_inh_join (id integer, a integer);
INSERT INTO test_schema.rv_r3c_inh_join VALUES (2, 2);
CREATE TABLE test_schema.rv_r3c_inh_keep () INHERITS (test_schema.rv_r3c_inh_p);
CREATE TABLE test_schema.rv_r3c_inh_drop () INHERITS (test_schema.rv_r3c_inh_p);
CREATE TABLE test_schema.rv_r3c_inh_leave (b integer, CONSTRAINT rv_r3c_inh_p_ck CHECK (b > 0))
    INHERITS (test_schema.rv_r3c_inh_p);
CREATE TABLE test_schema.rv_r3c_inh_q1 (k integer, CONSTRAINT rv_r3c_inh_q1_ck CHECK (k > 0));
CREATE TABLE test_schema.rv_r3c_inh_q2 (k integer, CONSTRAINT rv_r3c_inh_q2_ck CHECK (k < 100));
CREATE TABLE test_schema.rv_r3c_inh_r () INHERITS (test_schema.rv_r3c_inh_q1, test_schema.rv_r3c_inh_q2);
-- (6) Tables TO attaches that need column changes first (every version):
-- rv_r3c_atc_x lacks the NOT NULL of the parent's key column;
-- rv_r3c_atc_y has an extra column and lacks one of the parent's; the
-- partitioned rv_r3c_atc_sub lacks one too, and its partition
-- rv_r3c_atc_sub_1 gets an index and a CHECK on that column in TO, so its
-- alter has to follow its parent's.
CREATE TABLE test_schema.rv_r3c_atc (id integer NOT NULL, a integer, c integer, CONSTRAINT rv_r3c_atc_ck CHECK (a > 0))
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_atc_x (id integer, a integer, c integer);
CREATE TABLE test_schema.rv_r3c_atc_y (id integer NOT NULL, a integer, extra text);
CREATE TABLE test_schema.rv_r3c_atc_sub (id integer NOT NULL, a integer) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_atc_sub_1 PARTITION OF test_schema.rv_r3c_atc_sub FOR VALUES FROM (20) TO (25);
INSERT INTO test_schema.rv_r3c_atc_x VALUES (1, 1, 1);
INSERT INTO test_schema.rv_r3c_atc_y VALUES (11, 1, 'x');
INSERT INTO test_schema.rv_r3c_atc_sub VALUES (21, 1);
-- (7) NOT NULL constraint names (PG18+) that move to another column
-- (rv_r3c_nn_move) or swap between two (rv_r3c_nn_swap): renamed, never
-- dropped (a drop clears the column's NOT NULL, and the re-add fails while
-- the other column still has one).
CREATE TABLE test_schema.rv_r3c_nn_move (a integer CONSTRAINT rv_r3c_nn_move_x NOT NULL, b integer NOT NULL);
CREATE TABLE test_schema.rv_r3c_nn_swap (
    a integer CONSTRAINT rv_r3c_nn_swap_p NOT NULL,
    b integer CONSTRAINT rv_r3c_nn_swap_q NOT NULL
);

-- ==== END pg19:tables ====

-- ==== BEGIN pg19:statistics — extended statistics: virtual generated columns, expression statistics ====

-- Tables carry a `t_` in front of the pg19_ prefix: pgc's table query skips
-- every name LIKE 'pg_%' (an unescaped `_`), which would hide a table named
-- pg19_... from both dumps.

-- -----------------------------------------------------------------------------
-- Expression statistics (PG14+), FROM side.
-- -----------------------------------------------------------------------------
-- pg19_stat_expr_kinds:     (ndistinct) -> (mcv) in TO => drop + recreate; the
--                           CREATE must not name the "expressions" kind.
-- pg19_stat_expr_onlist:    the ON list changes inside EXTRACT(... FROM ...),
--                           which the old ',' split saw as unchanged.
-- pg19_stat_expr_same:      unchanged, with a ',' inside a literal; must stay
--                           silent.
-- pg19_stat_expr_from_only: univariate expression statistics, dropped in TO.
CREATE TABLE test_schema.t_pg19_stat_expr (
    id         integer PRIMARY KEY,
    code       text,
    qty        integer,
    created_on date,
    shipped_on date
);

CREATE STATISTICS test_schema.pg19_stat_expr_kinds (ndistinct)
    ON qty, (lower(code)) FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_expr_onlist
    ON qty, (extract(year from created_on)) FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_expr_same (mcv)
    ON qty, (code || ',') FROM test_schema.t_pg19_stat_expr;

-- Statistics targets. stxstattarget is a nullable smallint from PG17 on,
-- and pgc used to lose every explicit target there.
-- pg19_stat_expr_target: 100 -> 250; pg19_stat_expr_reset: 300 -> default.
CREATE STATISTICS test_schema.pg19_stat_expr_target (ndistinct)
    ON qty, (lower(code)) FROM test_schema.t_pg19_stat_expr;
ALTER STATISTICS test_schema.pg19_stat_expr_target SET STATISTICS 100;

CREATE STATISTICS test_schema.pg19_stat_expr_reset (ndistinct)
    ON qty, (upper(code)) FROM test_schema.t_pg19_stat_expr;
ALTER STATISTICS test_schema.pg19_stat_expr_reset SET STATISTICS 300;

CREATE STATISTICS test_schema.pg19_stat_expr_from_only
    ON (qty + 1) FROM test_schema.t_pg19_stat_expr;

-- -----------------------------------------------------------------------------
-- Statistics CASCADE-dropped with a column or a table (PG14+), FROM side.
-- -----------------------------------------------------------------------------
-- Each statistics object here carries a statistics target, which its
-- restore must bring back too.
-- t_pg19_stat_conv: `dbl` becomes a STORED generated column in TO, which pgc
--   rewrites as DROP COLUMN + ADD COLUMN; PostgreSQL CASCADE-drops
--   pg19_stat_conv_st with the column, and pgc must restore it.
--   `tripled` changes its STORED expression: a drop + add before PG17, which
--   takes pg19_stat_conv_setexpr along (restored); SET EXPRESSION from 17 on,
--   which keeps it but rebuilds it without its statistics target (set again).
CREATE TABLE test_schema.t_pg19_stat_conv (
    id      integer PRIMARY KEY,
    qty     integer,
    dbl     integer,
    tripled integer GENERATED ALWAYS AS (qty * 3) STORED
);

CREATE STATISTICS test_schema.pg19_stat_conv_st (ndistinct)
    ON qty, dbl FROM test_schema.t_pg19_stat_conv;
ALTER STATISTICS test_schema.pg19_stat_conv_st SET STATISTICS 200;

CREATE STATISTICS test_schema.pg19_stat_conv_setexpr (ndistinct)
    ON id, tripled FROM test_schema.t_pg19_stat_conv;
ALTER STATISTICS test_schema.pg19_stat_conv_setexpr SET STATISTICS 250;

-- t_pg19_stat_part: the partition key changes in TO, so pgc drops and recreates
--   the table, which drops pg19_stat_part_st with it.
CREATE TABLE test_schema.t_pg19_stat_part (
    a integer,
    b integer,
    c integer
) PARTITION BY RANGE (a);

CREATE STATISTICS test_schema.pg19_stat_part_st (ndistinct)
    ON a, b FROM test_schema.t_pg19_stat_part;
ALTER STATISTICS test_schema.pg19_stat_part_st SET STATISTICS 400;

-- t_pg19_stat_fn_items: the return type of pg19_stat_fn_compute changes in TO;
--   DROP FUNCTION ... CASCADE drops gen_total and with it the statistics on
--   it (Phase 7 restoration via the pg_depend column graph).
CREATE FUNCTION test_schema.pg19_stat_fn_compute(x integer)
RETURNS integer LANGUAGE sql IMMUTABLE AS $$
    SELECT x + 1;
$$;

CREATE TABLE test_schema.t_pg19_stat_fn_items (
    id        integer PRIMARY KEY,
    value     integer NOT NULL,
    gen_total integer GENERATED ALWAYS AS (test_schema.pg19_stat_fn_compute(value)) STORED
);

CREATE STATISTICS test_schema.pg19_stat_fn_items_st (ndistinct)
    ON value, gen_total FROM test_schema.t_pg19_stat_fn_items;
ALTER STATISTICS test_schema.pg19_stat_fn_items_st SET STATISTICS 600;

-- t_pg19_stat_fn_part: the same generated column on a partitioned table. Its
--   partition carries statistics of its own (extended statistics are not
--   inherited), anchored on the partition's column; Phase 7 re-adds the
--   column on the parent only and must restore them too.
CREATE TABLE test_schema.t_pg19_stat_fn_part (
    id        integer,
    value     integer NOT NULL,
    gen_total integer GENERATED ALWAYS AS (test_schema.pg19_stat_fn_compute(value)) STORED
) PARTITION BY RANGE (id);

CREATE TABLE test_schema.t_pg19_stat_fn_part1 PARTITION OF test_schema.t_pg19_stat_fn_part
    FOR VALUES FROM (0) TO (1000);

CREATE STATISTICS test_schema.pg19_stat_fn_part1_st (ndistinct)
    ON value, gen_total FROM test_schema.t_pg19_stat_fn_part1;
ALTER STATISTICS test_schema.pg19_stat_fn_part1_st SET STATISTICS 700;

-- pg19_stat_fn_items_new exists only in TO, on gen_total: created before
--   Phase 7's DROP FUNCTION ... CASCADE, dropped by it, restored after.

-- t_pg19_stat_pc: `v` changes type in TO. The parent's ALTER COLUMN reaches
--   the partition, which is not dropped: pg19_stat_pc_keep must not be
--   created again, and pg19_stat_pc_kind ((ndistinct) -> (mcv)) is altered
--   normally. ALTER COLUMN ... TYPE rebuilds the statistics on the column,
--   on the parent (pg19_stat_pc_parent) and on the partition alike, without
--   their statistics targets, which pgc sets again.
CREATE TABLE test_schema.t_pg19_stat_pc (
    id integer,
    v  integer,
    w  integer
) PARTITION BY RANGE (id);

CREATE TABLE test_schema.t_pg19_stat_pc1 PARTITION OF test_schema.t_pg19_stat_pc
    FOR VALUES FROM (0) TO (1000);

CREATE STATISTICS test_schema.pg19_stat_pc_keep (ndistinct)
    ON v, w FROM test_schema.t_pg19_stat_pc1;
ALTER STATISTICS test_schema.pg19_stat_pc_keep SET STATISTICS 450;

CREATE STATISTICS test_schema.pg19_stat_pc_kind (ndistinct)
    ON w, v FROM test_schema.t_pg19_stat_pc1;

CREATE STATISTICS test_schema.pg19_stat_pc_parent (ndistinct)
    ON v, w FROM test_schema.t_pg19_stat_pc;
ALTER STATISTICS test_schema.pg19_stat_pc_parent SET STATISTICS 350;

-- mv_pg19_stat_rebuilt: the materialized view changes its definition in TO,
--   so pgc drops it before the tables and creates it again later;
--   pg19_stat_mv_rebuilt (unchanged, target 800) went with the drop and must
--   be created after the view. mv_pg19_stat_new and pg19_stat_mv_new exist
--   only in TO.
CREATE MATERIALIZED VIEW test_schema.mv_pg19_stat_rebuilt AS
    SELECT id, qty FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_mv_rebuilt (ndistinct)
    ON id, qty FROM test_schema.mv_pg19_stat_rebuilt;
ALTER STATISTICS test_schema.pg19_stat_mv_rebuilt SET STATISTICS 800;

-- -----------------------------------------------------------------------------
-- PG19 feature test: extended statistics on virtual generated columns (PG19+)
-- -----------------------------------------------------------------------------
-- PG14-17 reject the VIRTUAL table; PG18 creates it but rejects every
-- statistics object below ("statistics creation on virtual generated columns
-- is not supported"), leaving only the column rewrite to round-trip.
-- pg19_stat_vcol_rewrite:   unchanged, but `doubled` changes its expression in
--                           TO. pgc alters it with SET EXPRESSION, which keeps
--                           the statistics but rebuilds them without their
--                           statistics target; pgc sets it again.
-- pg19_stat_vcol_expr:      univariate expression over `doubled`; kept.
-- pg19_stat_vcol_readd:     unchanged, but `readded` changes type and
--                           expression, which SET EXPRESSION cannot do: pgc
--                           drops + re-adds the column, the statistics go
--                           with it and must be restored.
-- public.pg19_stat_vcol_xschema: on `readded` too, and lives in another schema
--                           than its table; same story.
-- pg19_stat_vcol_kinds:     (ndistinct) -> default kinds in TO.
-- pg19_stat_vcol_from_only: univariate on a virtual column, dropped in TO.
CREATE TABLE test_schema.t_pg19_stat_vcol (
    id      integer PRIMARY KEY,
    qty     integer,
    price   integer,
    total   integer GENERATED ALWAYS AS (qty * price) VIRTUAL,
    doubled integer GENERATED ALWAYS AS (qty * 2) VIRTUAL,
    readded integer GENERATED ALWAYS AS (qty + 1) VIRTUAL
);

CREATE STATISTICS test_schema.pg19_stat_vcol_rewrite (ndistinct)
    ON qty, doubled FROM test_schema.t_pg19_stat_vcol;
ALTER STATISTICS test_schema.pg19_stat_vcol_rewrite SET STATISTICS 500;

CREATE STATISTICS test_schema.pg19_stat_vcol_expr
    ON (doubled + 1) FROM test_schema.t_pg19_stat_vcol;

CREATE STATISTICS test_schema.pg19_stat_vcol_readd (ndistinct)
    ON qty, readded FROM test_schema.t_pg19_stat_vcol;
ALTER STATISTICS test_schema.pg19_stat_vcol_readd SET STATISTICS 550;

CREATE STATISTICS public.pg19_stat_vcol_xschema (mcv)
    ON price, readded FROM test_schema.t_pg19_stat_vcol;

CREATE STATISTICS test_schema.pg19_stat_vcol_kinds (ndistinct)
    ON qty, total FROM test_schema.t_pg19_stat_vcol;

CREATE STATISTICS test_schema.pg19_stat_vcol_from_only
    ON total FROM test_schema.t_pg19_stat_vcol;

-- ==== END pg19:statistics ====

-- ==== BEGIN pg19:misc — misc: text search dictionaries, extensions, new PG19 syntax coverage ====
-- Naming: non-table objects are pg19_misc_*; TABLES are misc_pg19_*, because
-- the tables query filters `tablename not like 'pg_%'` with an unescaped `_`
-- (a LIKE wildcard), so a table named pg19_* would silently vanish from both
-- dumps and the test would pass vacuously.

-- -----------------------------------------------------------------------------
-- PG19 misc 1/5: Snowball Dutch stemmer rewritten, old one kept as 'dutch_porter'
-- -----------------------------------------------------------------------------
-- FROM: pg19_misc_dict_nl uses language 'dutch' (valid on every version). The
--       configuration pg19_misc_tsconfig_nl maps it, and a STORED generated
--       tsvector column and a GIN expression index use that configuration.
-- TO:   on PG19 the dictionary moves to 'dutch_porter' (keeps the pre-19
--       stems): an options-only change that must be an in-place
--       ALTER TEXT SEARCH DICTIONARY, because DROP ... CASCADE would also
--       drop the configuration, the generated column and the index. On
--       PG14-18 schema_b keeps 'dutch' (no diff).
CREATE TEXT SEARCH DICTIONARY test_schema.pg19_misc_dict_nl (
    TEMPLATE = pg_catalog.snowball,
    language = 'dutch',
    stopwords = 'dutch'
);
CREATE TEXT SEARCH CONFIGURATION test_schema.pg19_misc_tsconfig_nl (
    PARSER = pg_catalog."default"
);
ALTER TEXT SEARCH CONFIGURATION test_schema.pg19_misc_tsconfig_nl
    ADD MAPPING FOR asciiword, word WITH test_schema.pg19_misc_dict_nl, pg_catalog.simple;
CREATE TABLE test_schema.misc_pg19_ts_docs (
    id       integer PRIMARY KEY,
    body     text,
    body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('test_schema.pg19_misc_tsconfig_nl'::regconfig, body)) STORED
);
CREATE INDEX misc_pg19_ts_docs_fts ON test_schema.misc_pg19_ts_docs
    USING gin (to_tsvector('test_schema.pg19_misc_tsconfig_nl'::regconfig, body));
-- misc_pg19_ts_notes already holds rows here, and TO adds a STORED generated
-- column over the same configuration. PostgreSQL computes it for every row
-- when the column is added and an ALTER of the dictionary never re-processes
-- it, so on PG19 the in-place dictionary ALTER must run before the tables
-- phase or these rows keep the new 'dutch' stemmer's output (the three words
-- stem differently under 'dutch_porter'). The CI assertion recomputes them.
CREATE TABLE test_schema.misc_pg19_ts_notes (
    id   integer PRIMARY KEY,
    note text
);
INSERT INTO test_schema.misc_pg19_ts_notes (id, note)
VALUES (1, 'mogelijkheden'), (2, 'gebruikt'), (3, 'bedrijven');

-- -----------------------------------------------------------------------------
-- PG19 misc 2/5: oid8 data type (PG19+)
-- -----------------------------------------------------------------------------
-- FROM: plain bigint / oid columns, plus back_ref already oid8 (PG19 only)
--       that goes back to bigint in TO. On PG14-18 the ALTER fails and the
--       table is identical to the TO side there.
CREATE TABLE test_schema.misc_pg19_oid8_refs (
    id          bigint PRIMARY KEY,
    legacy_ref  oid,
    big_ref     bigint,
    back_ref    bigint
);
ALTER TABLE test_schema.misc_pg19_oid8_refs ALTER COLUMN back_ref TYPE oid8;

-- -----------------------------------------------------------------------------
-- PG19 misc 3/5: window-function null treatment IGNORE NULLS / RESPECT NULLS
-- -----------------------------------------------------------------------------
-- pg_get_viewdef prints IGNORE NULLS and drops RESPECT NULLS (the default).
--   pg19_misc_v_null_treatment      FROM plain lag          TO lag ... IGNORE NULLS   (replace)
--   pg19_misc_v_null_treatment_off  FROM last_value IGNORE  TO plain last_value       (replace)
--   pg19_misc_v_respect_nulls       FROM plain first_value  TO RESPECT NULLS          (no diff)
--   pg19_misc_mv_null_treatment     FROM plain lead         TO lead ... IGNORE NULLS  (matview drop+create)
--   pg19_misc_v_ignore_nulls_new    TO only, named WINDOW                             (create)
-- Before PG19 IGNORE/RESPECT NULLS is a syntax error: such a view then exists
-- on one side only and is created or dropped instead.
CREATE TABLE test_schema.misc_pg19_readings (
    id      integer PRIMARY KEY,
    sensor  integer NOT NULL,
    reading numeric
);
CREATE VIEW test_schema.pg19_misc_v_null_treatment AS
SELECT id, sensor,
       lag(reading) OVER (PARTITION BY sensor ORDER BY id) AS prev_reading
FROM test_schema.misc_pg19_readings;
CREATE VIEW test_schema.pg19_misc_v_null_treatment_off AS
SELECT id,
       last_value(reading) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known
FROM test_schema.misc_pg19_readings;
CREATE VIEW test_schema.pg19_misc_v_respect_nulls AS
SELECT id,
       first_value(reading) OVER (ORDER BY id) AS first_reading
FROM test_schema.misc_pg19_readings;
CREATE MATERIALIZED VIEW test_schema.pg19_misc_mv_null_treatment AS
SELECT sensor, id,
       lead(reading, 1, 0) OVER (PARTITION BY sensor ORDER BY id) AS next_reading
FROM test_schema.misc_pg19_readings;

-- -----------------------------------------------------------------------------
-- PG19 misc 4/5: JSON_ARRAY(query) deparse and empty-result semantics
-- -----------------------------------------------------------------------------
-- PG16-18 rewrite JSON_ARRAY(SELECT ...) at parse time into a JSON_ARRAYAGG
-- scalar subquery (NULL for an empty result); PG19 keeps JSON_ARRAY(query)
-- as written ('[]' for an empty result). FROM spells pg19_misc_ja_view and
-- pg19_misc_ja_mv in the pre-19 form (what a pg_dump from 16-18 carries into
-- 19); TO spells them JSON_ARRAY(SELECT ...). pg19_misc_ja_stable is the same
-- on both sides. PG19: CREATE OR REPLACE VIEW + matview recreate; PG16-18: no
-- diff (identical deparse); PG14-15: no JSON_ARRAY, the views fail on both.
CREATE TABLE test_schema.misc_pg19_ja_items (
    id    integer PRIMARY KEY,
    label text
);
CREATE VIEW test_schema.pg19_misc_ja_view AS
SELECT (SELECT JSON_ARRAYAGG(q.a RETURNING json)
          FROM (SELECT label FROM test_schema.misc_pg19_ja_items ORDER BY id) q(a)) AS labels;
CREATE MATERIALIZED VIEW test_schema.pg19_misc_ja_mv AS
SELECT (SELECT JSON_ARRAYAGG(q.a RETURNING jsonb)
          FROM (SELECT id FROM test_schema.misc_pg19_ja_items) q(a)) AS ids;
CREATE VIEW test_schema.pg19_misc_ja_stable AS
SELECT JSON_ARRAY(SELECT id FROM test_schema.misc_pg19_ja_items ORDER BY id) AS ids;

-- -----------------------------------------------------------------------------
-- PG19 misc 5/5: heap reloptions autovacuum_parallel_workers and
-- log_autoanalyze_min_duration (PG19+)
-- -----------------------------------------------------------------------------
-- Options are set by a separate ALTER so that on PG14-18 only the ALTER fails
-- ("unrecognized parameter") and the objects still exist on both sides.
-- FROM: fillfactor=80 (all versions) + autovacuum_parallel_workers=2,
--       log_autovacuum_min_duration=500 (PG19)
-- TO:   fillfactor=80 + autovacuum_parallel_workers=4, log_autoanalyze_min_duration=1000
CREATE TABLE test_schema.misc_pg19_reloptions_test (
    id    serial PRIMARY KEY,
    value text
) WITH (fillfactor = 80);
ALTER TABLE test_schema.misc_pg19_reloptions_test SET (
    autovacuum_parallel_workers = 2,
    log_autovacuum_min_duration = 500
);
-- FROM: leaf partition without reloptions; TO adds autovacuum_parallel_workers=1
CREATE TABLE test_schema.misc_pg19_reloptions_part (
    id    integer,
    value text
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.misc_pg19_reloptions_part_1
    PARTITION OF test_schema.misc_pg19_reloptions_part FOR VALUES FROM (0) TO (100);
-- FROM: materialized view without reloptions; TO adds log_autoanalyze_min_duration
CREATE MATERIALIZED VIEW test_schema.pg19_misc_reloptions_mv AS SELECT 1 AS x;

-- -----------------------------------------------------------------------------
-- r3-ddl: catalog names in emitted DDL (all supported versions)
-- -----------------------------------------------------------------------------
-- Collations used by column COLLATE clauses, on both sides. One name holds a
-- `"` followed by SQL: pgc wrote `collate "<name>"` without doubling the
-- quote, so the rest of the name ran when the migration was applied (the
-- CI assertion checks public.rv_r3d_pwned never appears in FROM). A
-- same-named collation in public (a different locale) is what an
-- unqualified `collate "rv_r3d_ci"` silently bound to.
CREATE COLLATION test_schema.rv_r3d_ci (provider = libc, locale = 'POSIX');
CREATE COLLATION public.rv_r3d_ci (provider = libc, locale = 'C');
CREATE COLLATION test_schema."rv_r3d_ci""; create table public.rv_r3d_pwned(x int); --" (provider = libc, locale = 'C');

-- Event triggers whose names need quoting. The dump stores quote_ident(evtname)
-- and pgc quoted it again, so "rv_r3d_Keep Trg" came out as
-- """rv_r3d_Keep Trg""": the create made a misnamed trigger and every drop,
-- alter and comment missed the real one.
CREATE OR REPLACE FUNCTION test_schema.rv_r3d_evt_fn() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    NULL;
END;
$$;
CREATE EVENT TRIGGER "rv_r3d_Keep Trg" ON ddl_command_start
    EXECUTE FUNCTION test_schema.rv_r3d_evt_fn();

-- Foreign keys next to a dropped table. The FK pre-drop pass matched every
-- FROM foreign key whose definition merely CONTAINED the dropped table's
-- name: rv_r3d_account is a prefix of rv_r3d_accounts, and rv_r3d_log
-- appears in the column rv_r3d_log_catalog_id. An unchanged key was dropped
-- and never added back; a changed one was dropped twice ("does not exist").
CREATE TABLE test_schema.rv_r3d_accounts (id integer PRIMARY KEY);
CREATE TABLE test_schema.rv_r3d_payments (
    id         integer PRIMARY KEY,
    account_id integer CONSTRAINT rv_r3d_payments_account_fk
               REFERENCES test_schema.rv_r3d_accounts(id)
);
CREATE TABLE test_schema.rv_r3d_catalogs (id integer PRIMARY KEY);
-- A partitioned table referenced by a foreign key: PostgreSQL copies the key
-- onto every partition, so dropping a partition drops the key first, and it
-- has to come back.
CREATE TABLE test_schema.rv_r3d_parted (id integer PRIMARY KEY) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3d_parted_1
    PARTITION OF test_schema.rv_r3d_parted FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_r3d_parted_ref (
    id  integer PRIMARY KEY,
    pid integer CONSTRAINT rv_r3d_parted_ref_fk REFERENCES test_schema.rv_r3d_parted(id)
);

-- FROM side.
-- Collations that TO changes: "C" -> test_schema.rv_r3d_ci (a collation-only
-- change, which left the table hash as it was and was never migrated), and
-- public.rv_r3d_ci -> test_schema.rv_r3d_ci (the schema alone differs).
CREATE TABLE test_schema.rv_r3d_coll_alter (
    id         integer PRIMARY KEY,
    c_retag    text COLLATE "C",
    c_reschema text COLLATE public.rv_r3d_ci,
    c_kept     text COLLATE "POSIX"
);
-- Event triggers: "rv_r3d_Keep Trg" is disabled and commented in TO;
-- "rv_r3d_From ""Only""" is FROM-only (dropped).
CREATE EVENT TRIGGER "rv_r3d_From ""Only""" ON sql_drop
    EXECUTE FUNCTION test_schema.rv_r3d_evt_fn();
-- FROM-only tables, and the keys around them.
CREATE TABLE test_schema.rv_r3d_account (id integer PRIMARY KEY);
CREATE TABLE test_schema.rv_r3d_log (id integer PRIMARY KEY);
CREATE TABLE test_schema.rv_r3d_products (
    id                    integer PRIMARY KEY,
    rv_r3d_log_catalog_id integer,
    CONSTRAINT rv_r3d_products_catalog_fk FOREIGN KEY (rv_r3d_log_catalog_id)
        REFERENCES test_schema.rv_r3d_catalogs(id) ON DELETE CASCADE
);
CREATE TABLE test_schema.rv_r3d_legacy_ref (id integer PRIMARY KEY);
CREATE TABLE test_schema.rv_r3d_orders (
    id        integer PRIMARY KEY,
    legacy_id integer CONSTRAINT rv_r3d_orders_legacy_fk
              REFERENCES test_schema.rv_r3d_legacy_ref(id)
);
CREATE TABLE test_schema.rv_r3d_parted_2
    PARTITION OF test_schema.rv_r3d_parted FOR VALUES FROM (100) TO (200);

-- r3-ddl review. Both sides: the trigger function, and a table whose policy
-- reads a column of rv_r3d_retype in a subquery (PostgreSQL records that as
-- a dependency on the column too).
CREATE OR REPLACE FUNCTION test_schema.rv_r3d_trg_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RETURN NEW;
END;
$$;

-- FROM side.
-- rv_r3d_to_new_coll.c moves, in TO, to a collation only TO has; it was
-- created after the tables, so the ALTER COLUMN ... TYPE ... COLLATE (and a
-- CREATE TABLE or CREATE DOMAIN using it) failed with "does not exist".
CREATE TABLE test_schema.rv_r3d_to_new_coll (
    id integer PRIMARY KEY,
    c  text COLLATE "C"
);
-- Retyped in place in TO: owner and status change collation only, code
-- becomes varchar(20). A policy and two triggers (UPDATE OF, WHEN) depend on
-- them, and PostgreSQL refuses ALTER COLUMN ... TYPE while they are there.
CREATE TABLE test_schema.rv_r3d_retype (
    id     integer PRIMARY KEY,
    owner  text COLLATE "C",
    code   varchar(10),
    status text COLLATE "C"
);
ALTER TABLE test_schema.rv_r3d_retype ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3d_retype_owner ON test_schema.rv_r3d_retype
    USING (owner = current_user);
CREATE POLICY rv_r3d_retype_code ON test_schema.rv_r3d_retype FOR SELECT
    USING (code <> 'x');
CREATE POLICY rv_r3d_retype_gone ON test_schema.rv_r3d_retype FOR DELETE
    USING (status <> 'locked');
CREATE TRIGGER rv_r3d_retype_when BEFORE UPDATE ON test_schema.rv_r3d_retype
    FOR EACH ROW WHEN (old.status IS DISTINCT FROM new.status)
    EXECUTE FUNCTION test_schema.rv_r3d_trg_fn();
CREATE TRIGGER rv_r3d_retype_upd_of BEFORE UPDATE OF owner ON test_schema.rv_r3d_retype
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3d_trg_fn();
CREATE TABLE test_schema.rv_r3d_retype_ref (id integer PRIMARY KEY, rid integer);
ALTER TABLE test_schema.rv_r3d_retype_ref ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3d_retype_ref_pol ON test_schema.rv_r3d_retype_ref
    USING (EXISTS (SELECT 1 FROM test_schema.rv_r3d_retype r
                    WHERE r.id = rid AND r.code = 'a'));
-- A partition-key column that changes collation only: PostgreSQL retypes no
-- partition-key column, so the table is dropped and recreated.
CREATE TABLE test_schema.rv_r3d_pkey_coll (id integer, c text COLLATE "C")
    PARTITION BY LIST (c);
CREATE TABLE test_schema.rv_r3d_pkey_coll_a
    PARTITION OF test_schema.rv_r3d_pkey_coll FOR VALUES IN ('a');

-- -----------------------------------------------------------------------------
-- r3:types — composite type attributes, sequence data types, owners of
-- serial / identity sequences, multirange schema (rv_r3t_*; all versions)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_r3t_owner') THEN
        CREATE ROLE pgc_pg19_r3t_owner;
    END IF;
END $$;

-- Composite attributes used to reach only an "Alter script not implemented"
-- comment. FROM: a retype (a), a rename in the middle (b -> bb, which takes c
-- out of place: nothing stores the type, so c is dropped and re-added), a
-- dropped attribute (gone). TO adds `added` at the end.
CREATE TYPE test_schema.rv_r3t_comp AS (a integer, b integer, c text, gone text);
-- Stored in a column: an added attribute is appended (ADD ATTRIBUTE works
-- with stored values; the row keeps its data and reads z as NULL).
CREATE TYPE test_schema.rv_r3t_used AS (x integer, y text);
CREATE TABLE test_schema.rv_r3t_used_tbl (id integer PRIMARY KEY, v test_schema.rv_r3t_used);
INSERT INTO test_schema.rv_r3t_used_tbl VALUES (1, ROW(1, 'one'));
-- Typed table: its type retypes id, drops dropme and adds extra, all with
-- CASCADE; ALTER TABLE could do none of them ("cannot add column to typed
-- table"). TO gives extra a default and NOT NULL of the table's own.
CREATE TYPE test_schema.rv_r3t_typed_t AS (id integer, dropme text, keep text);
CREATE TABLE test_schema.rv_r3t_typed OF test_schema.rv_r3t_typed_t (PRIMARY KEY (id));
-- A row: ADD ATTRIBUTE leaves extra NULL in it, which TO's NOT NULL refused
-- on every run; the row now gets extra's default first.
INSERT INTO test_schema.rv_r3t_typed (id, dropme, keep) VALUES (1, 'gone', 'kept');

-- Sequence data types: serial -> bigserial, bigserial -> serial, a standalone
-- integer -> bigint sequence and an identity integer -> bigint. The ALTER
-- SEQUENCE had no AS clause: the first failed every run (MAXVALUE out of
-- range for integer), the second silently kept bigint.
CREATE TABLE test_schema.rv_r3t_ser (id serial PRIMARY KEY, v text);
CREATE TABLE test_schema.rv_r3t_bigser (id bigserial PRIMARY KEY, v text);
INSERT INTO test_schema.rv_r3t_ser (v) VALUES ('one');
CREATE SEQUENCE test_schema.rv_r3t_seq AS integer;
CREATE TABLE test_schema.rv_r3t_ident (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY);

-- Owner change of tables with a serial and an identity sequence (TO: owned by
-- pgc_pg19_r3t_owner). A direct ALTER SEQUENCE ... OWNER TO of a linked
-- sequence is refused; the table's OWNER TO carries it.
CREATE TABLE test_schema.rv_r3t_own_ser (id serial PRIMARY KEY, v text);
CREATE TABLE test_schema.rv_r3t_own_ident (id integer GENERATED ALWAYS AS IDENTITY, v text);

-- A multirange in the wrong schema, where pgc before 1.1.0 created it (an
-- unqualified multirange_type_name lands in the search_path schema): moved
-- with ALTER TYPE ... SET SCHEMA, without recreating the range.
CREATE TYPE test_schema.rv_r3t_mis AS RANGE (subtype = integer, multirange_type_name = public.rv_r3t_mis_multirange);

-- Review fixes. A typed table whose retyped column a view reads: the
-- ALTER ATTRIBUTE ... TYPE ... CASCADE ran before the view was dropped, and
-- PostgreSQL refused it on every run ("cannot alter type of a column used by
-- a view or rule").
CREATE TYPE test_schema.rv_r3t_tv_t AS (id integer, name text);
CREATE TABLE test_schema.rv_r3t_tv OF test_schema.rv_r3t_tv_t;
INSERT INTO test_schema.rv_r3t_tv VALUES (1, 'one');
CREATE VIEW test_schema.rv_r3t_tv_v AS SELECT id, name FROM test_schema.rv_r3t_tv;
-- A sequence OWNED BY a column of rv_r3t_mv_a, moved to rv_r3t_mv_b while
-- both get a new owner: a direct OWNER TO is refused while it is linked to
-- rv_r3t_mv_a, and OWNED BY rv_r3t_mv_b while the owners differ.
CREATE TABLE test_schema.rv_r3t_mv_a (id bigint);
CREATE TABLE test_schema.rv_r3t_mv_b (id bigint);
CREATE SEQUENCE test_schema.rv_r3t_mv_seq OWNED BY test_schema.rv_r3t_mv_a.id;


-- -----------------------------------------------------------------------------
-- r3-constraints (rv_r3c_*): a text search dictionary that changes its
-- template (every version)
-- -----------------------------------------------------------------------------
-- ALTER cannot change a template, and DROP TEXT SEARCH DICTIONARY ... CASCADE
-- silently took the configuration mapping it, and through that the STORED
-- generated tsvector column and the expression index (the next compare then
-- failed to put them back, as configurations come after tables). The new
-- dictionary is created under a temporary name before the tables, the
-- configuration moves to it, the old one is dropped without CASCADE and the
-- new one takes its name.
-- FROM: rv_r3c_dict on the simple template. TO: on snowball.
CREATE TEXT SEARCH DICTIONARY test_schema.rv_r3c_dict (TEMPLATE = pg_catalog.simple, stopwords = 'english');
COMMENT ON TEXT SEARCH DICTIONARY test_schema.rv_r3c_dict IS 'rv_r3c dictionary';
CREATE TEXT SEARCH CONFIGURATION test_schema.rv_r3c_tscfg (COPY = pg_catalog.simple);
ALTER TEXT SEARCH CONFIGURATION test_schema.rv_r3c_tscfg
    ALTER MAPPING FOR asciiword WITH test_schema.rv_r3c_dict;
CREATE TABLE test_schema.rv_r3c_ts_docs (
    id       integer PRIMARY KEY,
    body     text,
    body_tsv tsvector GENERATED ALWAYS AS (to_tsvector('test_schema.rv_r3c_tscfg'::regconfig, body)) STORED
);
CREATE INDEX rv_r3c_ts_docs_fts ON test_schema.rv_r3c_ts_docs
    USING gin (to_tsvector('test_schema.rv_r3c_tscfg'::regconfig, body));
INSERT INTO test_schema.rv_r3c_ts_docs (id, body) VALUES (1, 'The running dogs');

-- -----------------------------------------------------------------------------
-- r4-typed (rv_r4t_*): typed tables and composite types (every version)
-- -----------------------------------------------------------------------------
-- A composite type's ALTER ATTRIBUTE ... TYPE ... CASCADE runs before the
-- table phase and retypes the column of every typed table of the type, their
-- partitions and INHERITS descendants included, and of one the table phase
-- drops or recreates later: what is in its way (a policy, an UPDATE OF
-- trigger, a generated column reading the column, another table's policy
-- reading it through a subquery) is dropped ahead of it and restored after
-- every table alter.
-- rv_r4t_pt:  typed partitioned table rv_r4t_tp; its partition rv_r4t_tp1
--             has a policy and an UPDATE OF trigger on amount (int -> bigint)
-- rv_r4t_it:  typed table rv_r4t_tt; its INHERITS child rv_r4t_ti has a
--             policy on amount, and a generated column g reading it with a
--             policy of its own; rv_r4t_other's policy reads rv_r4t_tt.amount
-- rv_r4t_dt:  typed table rv_r4t_gone, dropped in TO, with a policy and a
--             trigger on amount
-- rv_r4t_rt:  typed partitioned table rv_r4t_rp, recreated in TO (LIST ->
--             RANGE); rv_r4t_reader's policy reads rv_r4t_rp.amount
-- rv_r4t_ct:  attribute collations change (note: default -> "C", memo: "C"
--             -> default) under typed table rv_r4t_coll's policy and trigger
-- rv_r4t_a_t / rv_r4t_z / rv_r4t_o: rv_r4t_o_pol reads a column rv_r4t_a_t
--             drops and re-adds (g) and one rv_r4t_z retypes in place (n)
-- TO only: rv_r4t_newc (an attribute with a collation), and the typed tables
--             rv_r4t_new (defaults, NOT NULL, a primary key and a CHECK),
--             rv_r4t_bare (nothing of its own) and rv_r4t_nn (a named NOT
--             NULL constraint).
-- FROM side.
CREATE FUNCTION test_schema.rv_r4t_trg() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
CREATE TYPE test_schema.rv_r4t_pt AS (id integer, amount integer, note text);
CREATE TABLE test_schema.rv_r4t_tp OF test_schema.rv_r4t_pt (id WITH OPTIONS NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r4t_tp1 PARTITION OF test_schema.rv_r4t_tp FOR VALUES FROM (0) TO (100);
ALTER TABLE test_schema.rv_r4t_tp1 ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_tp1_pol ON test_schema.rv_r4t_tp1 USING (amount > 0);
CREATE TRIGGER rv_r4t_tp1_trg BEFORE UPDATE OF amount ON test_schema.rv_r4t_tp1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r4t_trg();
INSERT INTO test_schema.rv_r4t_tp VALUES (1, 5, 'a');
CREATE TYPE test_schema.rv_r4t_it AS (id integer, amount integer, note text);
CREATE TABLE test_schema.rv_r4t_tt OF test_schema.rv_r4t_it (id WITH OPTIONS PRIMARY KEY);
CREATE TABLE test_schema.rv_r4t_ti (extra integer, g integer GENERATED ALWAYS AS (amount * 2) STORED)
    INHERITS (test_schema.rv_r4t_tt);
ALTER TABLE test_schema.rv_r4t_ti ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_ti_pol ON test_schema.rv_r4t_ti USING (amount > 0);
CREATE POLICY rv_r4t_ti_gpol ON test_schema.rv_r4t_ti USING (g > 0);
INSERT INTO test_schema.rv_r4t_ti (id, amount, note, extra) VALUES (1, 5, 'a', 3);
CREATE TABLE test_schema.rv_r4t_other (id integer, x bigint);
ALTER TABLE test_schema.rv_r4t_other ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_other_pol ON test_schema.rv_r4t_other
    USING (x IN (SELECT amount FROM test_schema.rv_r4t_tt));
CREATE TYPE test_schema.rv_r4t_dt AS (id integer, amount integer);
CREATE TABLE test_schema.rv_r4t_gone OF test_schema.rv_r4t_dt;
ALTER TABLE test_schema.rv_r4t_gone ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_gone_pol ON test_schema.rv_r4t_gone USING (amount > 0);
CREATE TRIGGER rv_r4t_gone_trg BEFORE UPDATE OF amount ON test_schema.rv_r4t_gone
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r4t_trg();
CREATE TYPE test_schema.rv_r4t_rt AS (id integer, amount integer, note text);
CREATE TABLE test_schema.rv_r4t_rp OF test_schema.rv_r4t_rt (id WITH OPTIONS NOT NULL) PARTITION BY LIST (id);
CREATE TABLE test_schema.rv_r4t_rp1 PARTITION OF test_schema.rv_r4t_rp FOR VALUES IN (1, 2);
ALTER TABLE test_schema.rv_r4t_rp ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_rp_pol ON test_schema.rv_r4t_rp USING (amount > 0);
CREATE TABLE test_schema.rv_r4t_reader (id integer, x bigint);
ALTER TABLE test_schema.rv_r4t_reader ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_reader_pol ON test_schema.rv_r4t_reader
    USING (x IN (SELECT amount FROM test_schema.rv_r4t_rp));
CREATE TYPE test_schema.rv_r4t_ct AS (id integer, note text, memo text COLLATE "C");
CREATE TABLE test_schema.rv_r4t_coll OF test_schema.rv_r4t_ct (id WITH OPTIONS PRIMARY KEY);
ALTER TABLE test_schema.rv_r4t_coll ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_coll_pol ON test_schema.rv_r4t_coll USING (note <> '' AND memo <> '');
CREATE TRIGGER rv_r4t_coll_trg BEFORE UPDATE OF note ON test_schema.rv_r4t_coll
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r4t_trg();
INSERT INTO test_schema.rv_r4t_coll VALUES (1, 'a', 'b');
CREATE TABLE test_schema.rv_r4t_a_t (id integer PRIMARY KEY, n integer, g integer GENERATED ALWAYS AS (n * 2) STORED);
CREATE TABLE test_schema.rv_r4t_z (id integer, n integer);
CREATE TABLE test_schema.rv_r4t_o (id integer, x bigint);
ALTER TABLE test_schema.rv_r4t_o ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_o_pol ON test_schema.rv_r4t_o
    USING (x IN (SELECT g FROM test_schema.rv_r4t_a_t) AND x IN (SELECT n FROM test_schema.rv_r4t_z));
INSERT INTO test_schema.rv_r4t_a_t (id, n) VALUES (1, 4);
INSERT INTO test_schema.rv_r4t_z VALUES (1, 4);
-- Review fixes (r4-typed):
-- rv_r4t_qt:  attributes that need quoting ("order", "Amt": int -> bigint)
--             under typed table rv_r4t_qtt's policy on "order", and a
--             generated column of its INHERITS child rv_r4t_qti reading "Amt"
-- rv_r4t_lt:  rv_r4t_lti leaves typed table rv_r4t_ltt's family (NO INHERIT)
--             and retypes amount itself (int -> bigint) under its own policy
-- rv_r4t_3t:  rv_r4t_3c1 (generated g) INHERITS typed table rv_r4t_3tt and
--             rv_r4t_3c2 INHERITS rv_r4t_3c1; TO drops both children while
--             the type retypes amount
-- rv_r4t_5t:  the same, but TO keeps rv_r4t_5c1 with g a plain column (DROP
--             EXPRESSION, which reaches rv_r4t_5c2's copy) and drops rv_r4t_5c2
-- rv_r4t_outside.rv_r4t_xt: a type outside the compared schemas whose
--             attribute differs: not migrated, so the view rv_r4t_xv and the
--             publication rv_r4t_xpub (PG15+) over typed table rv_r4t_xtt stay
CREATE TYPE test_schema.rv_r4t_qt AS (id integer, "order" integer, "Amt" integer);
CREATE TABLE test_schema.rv_r4t_qtt OF test_schema.rv_r4t_qt;
ALTER TABLE test_schema.rv_r4t_qtt ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_qtt_pol ON test_schema.rv_r4t_qtt USING ("order" > 0);
CREATE TABLE test_schema.rv_r4t_qti (g integer GENERATED ALWAYS AS ("Amt" * 2) STORED)
    INHERITS (test_schema.rv_r4t_qtt);
INSERT INTO test_schema.rv_r4t_qti (id, "order", "Amt") VALUES (1, 2, 3);
CREATE TYPE test_schema.rv_r4t_lt AS (id integer, amount integer);
CREATE TABLE test_schema.rv_r4t_ltt OF test_schema.rv_r4t_lt;
CREATE TABLE test_schema.rv_r4t_lti (extra integer) INHERITS (test_schema.rv_r4t_ltt);
ALTER TABLE test_schema.rv_r4t_lti ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_lti_pol ON test_schema.rv_r4t_lti USING (amount > 0);
INSERT INTO test_schema.rv_r4t_lti VALUES (1, 5, 3);
CREATE TYPE test_schema.rv_r4t_3t AS (id integer, amount integer);
CREATE TABLE test_schema.rv_r4t_3tt OF test_schema.rv_r4t_3t;
CREATE TABLE test_schema.rv_r4t_3c1 (g integer GENERATED ALWAYS AS (amount * 2) STORED)
    INHERITS (test_schema.rv_r4t_3tt);
CREATE TABLE test_schema.rv_r4t_3c2 (x integer) INHERITS (test_schema.rv_r4t_3c1);
INSERT INTO test_schema.rv_r4t_3c2 (id, amount, x) VALUES (1, 5, 3);
CREATE TYPE test_schema.rv_r4t_5t AS (id integer, amount integer);
CREATE TABLE test_schema.rv_r4t_5tt OF test_schema.rv_r4t_5t;
CREATE TABLE test_schema.rv_r4t_5c1 (g integer GENERATED ALWAYS AS (amount * 2) STORED)
    INHERITS (test_schema.rv_r4t_5tt);
CREATE TABLE test_schema.rv_r4t_5c2 (x integer) INHERITS (test_schema.rv_r4t_5c1);
INSERT INTO test_schema.rv_r4t_5c2 (id, amount, x) VALUES (1, 5, 3);
CREATE SCHEMA IF NOT EXISTS rv_r4t_outside;
CREATE TYPE rv_r4t_outside.rv_r4t_xt AS (id integer, amount integer);
CREATE TABLE test_schema.rv_r4t_xtt OF rv_r4t_outside.rv_r4t_xt;
CREATE VIEW test_schema.rv_r4t_xv AS SELECT id, amount FROM test_schema.rv_r4t_xtt;
CREATE PUBLICATION rv_r4t_xpub FOR TABLE test_schema.rv_r4t_xtt (id, amount) WHERE (amount > 0);

-- ==== END pg19:misc ====

-- ═════════════════════════════════════════════════════════════════════
-- Pre-existing bug fixes shipped with pgc v1.1.0 (all supported versions).
-- ═════════════════════════════════════════════════════════════════════

-- ==== BEGIN fix:sqlbody — SQL-standard routine bodies (BEGIN ATOMIC / RETURN) ====

-- SQL-standard routine bodies (BEGIN ATOMIC ... END / RETURN expr, PG14+) are
-- stored as a parsed query tree in pg_proc.prosqlbody and leave prosrc empty.
-- pgc 1.0.x read only prosrc: it dumped every such body as '' (so a body change
-- was never detected) and replayed it as `as $$$$`, which fails for a function
-- and silently turns a procedure into an empty no-op.
-- FROM: an unchanged body, a RETURN body and two BEGIN ATOMIC bodies that
-- change, a string body that becomes a SQL-standard one and the reverse, and a
-- FROM-only procedure.
CREATE TABLE test_schema.pg19_sqlbody_target (
    id  integer PRIMARY KEY,
    val text
);

-- Identical in TO: must not be re-emitted.
CREATE FUNCTION test_schema.pg19_sqlbody_unchanged(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT p + 1;
END;

-- RETURN-form body whose expression changes in TO.
CREATE FUNCTION test_schema.pg19_sqlbody_return(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN p + 1;

-- ON CONFLICT DO NOTHING here; DO SELECT FOR UPDATE ... WHERE in TO (PG19+).
CREATE FUNCTION test_schema.pg19_sqlbody_upsert(p integer)
RETURNS SETOF test_schema.pg19_sqlbody_target
LANGUAGE sql
BEGIN ATOMIC
    INSERT INTO test_schema.pg19_sqlbody_target VALUES (p, 'x')
    ON CONFLICT (id) DO NOTHING
    RETURNING *;
END;

-- Procedure whose multi-statement body changes in TO.
CREATE PROCEDURE test_schema.pg19_sqlbody_proc_changed(p integer)
LANGUAGE sql
BEGIN ATOMIC
    INSERT INTO test_schema.pg19_sqlbody_target VALUES (p, 'a')
    ON CONFLICT (id) DO NOTHING;
END;

-- String body here, SQL-standard body in TO.
CREATE FUNCTION test_schema.pg19_sqlbody_to_atomic(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
AS $$ SELECT p * 2 $$;

-- SQL-standard body here, string body in TO.
CREATE FUNCTION test_schema.pg19_sqlbody_to_string(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN p * 3;

-- FROM-only procedure: dropped.
CREATE PROCEDURE test_schema.pg19_sqlbody_proc_from_only(p integer)
LANGUAGE sql
BEGIN ATOMIC
    DELETE FROM test_schema.pg19_sqlbody_target WHERE id = p;
END;

-- ---- A SQL-standard body is a hard pg_depend dependent of every relation,
-- column, routine and operator it uses (a string body is not): PostgreSQL
-- refuses to drop or retype any of them under it, and a DROP ... CASCADE of
-- one takes the routine along. Each case below failed, needed a second run,
-- or never converged before pgc dropped such routines ahead of the change and
-- re-created them after it.

-- FROM-only table and sequence with FROM-only readers: the readers go first.
CREATE TABLE test_schema.pg19_sqlbody_legacy (id integer);
CREATE SEQUENCE test_schema.pg19_sqlbody_legacy_seq;
CREATE FUNCTION test_schema.pg19_sqlbody_legacy_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM test_schema.pg19_sqlbody_legacy);
CREATE FUNCTION test_schema.pg19_sqlbody_legacy_next() RETURNS bigint
LANGUAGE sql
RETURN nextval('test_schema.pg19_sqlbody_legacy_seq');

-- old_col is dropped (its changed reader goes first); v is retyped
-- integer -> bigint under an unchanged reader, which a view and a CHECK
-- constraint call and a role may execute: all of them come back.
CREATE TABLE test_schema.pg19_sqlbody_cols (id integer, old_col integer, v integer);
CREATE FUNCTION test_schema.pg19_sqlbody_cols_old() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(old_col) FROM test_schema.pg19_sqlbody_cols);
CREATE FUNCTION test_schema.pg19_sqlbody_cols_max() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT max(v) FROM test_schema.pg19_sqlbody_cols);
CREATE VIEW test_schema.pg19_sqlbody_cols_view AS
SELECT test_schema.pg19_sqlbody_cols_max() AS max_v;
CREATE TABLE test_schema.pg19_sqlbody_limits (
    lim integer CONSTRAINT pg19_sqlbody_limits_chk
        CHECK (test_schema.pg19_sqlbody_cols_max() IS NULL OR lim >= 0)
);
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_sqlbody_reader') THEN
        CREATE ROLE pgc_pg19_sqlbody_reader;
    END IF;
END
$$;
REVOKE EXECUTE ON FUNCTION test_schema.pg19_sqlbody_cols_max() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION test_schema.pg19_sqlbody_cols_max() TO pgc_pg19_sqlbody_reader;
-- A rule calling that unchanged reader: the reader's DROP ... CASCADE ahead
-- of the retype would take the rule along silently, so the rule is dropped
-- before it and recreated once the reader is back.
CREATE TABLE test_schema.pg19_sqlbody_rule_log (id integer);
CREATE RULE pg19_sqlbody_rule_max AS ON INSERT TO test_schema.pg19_sqlbody_rule_log
    DO ALSO SELECT test_schema.pg19_sqlbody_cols_max();

-- A view reshaped incompatibly (drop + create) under an unchanged reader.
CREATE TABLE test_schema.pg19_sqlbody_shape (id integer, a integer, b integer);
CREATE VIEW test_schema.pg19_sqlbody_shape_view AS
SELECT id, a, b FROM test_schema.pg19_sqlbody_shape;
CREATE FUNCTION test_schema.pg19_sqlbody_shape_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM test_schema.pg19_sqlbody_shape_view);

-- A callee whose return type changes (DROP ... CASCADE + CREATE) under an
-- unchanged SQL-standard caller.
CREATE FUNCTION test_schema.pg19_sqlbody_rt_callee(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN p + 1;
CREATE FUNCTION test_schema.pg19_sqlbody_rt_caller(p integer) RETURNS bigint
LANGUAGE sql IMMUTABLE
RETURN test_schema.pg19_sqlbody_rt_callee(p) * 2;

-- A table rebuilt by a partition-key change under an unchanged reader.
CREATE TABLE test_schema.pg19_sqlbody_parted (id integer, k integer) PARTITION BY RANGE (id);
CREATE FUNCTION test_schema.pg19_sqlbody_parted_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM test_schema.pg19_sqlbody_parted);

-- An operator rebuilt (its function changes) under an unchanged user, which
-- a column default calls.
CREATE FUNCTION test_schema.pg19_sqlbody_eq(integer, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN $1 = $2;
CREATE FUNCTION test_schema.pg19_sqlbody_eq_or_null(integer, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN $1 = $2 OR $1 IS NULL;
CREATE OPERATOR test_schema.=== (
    LEFTARG = integer, RIGHTARG = integer, FUNCTION = test_schema.pg19_sqlbody_eq
);
CREATE FUNCTION test_schema.pg19_sqlbody_uses_op(p integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN p OPERATOR(test_schema.===) 1;
CREATE TABLE test_schema.pg19_sqlbody_op_default (
    id integer,
    ok boolean DEFAULT test_schema.pg19_sqlbody_uses_op(1)
);


-- ---- reg-cascade: what a routine's DROP ... CASCADE takes along beyond the
-- table-level objects pgc always put back. rv_cascade_is_valid, _norm and
-- _first_code read a column this migration retypes (text -> varchar(10)), so
-- they are dropped with CASCADE ahead of the retype and re-created after it
-- (v1.0.27 stopped on "cannot alter type of a column used by a function");
-- rv_cascade_score's return type changes, so the routine phase drops it with
-- CASCADE and re-creates it. Without restoring them the migration silently
-- lost the domain CHECKs, the WHEN-clause triggers and the expression
-- statistics calling them and the functions whose argument defaults call
-- them — and, through a domain DEFAULT calling one, the domain itself and
-- every column of that type, with its data.
CREATE TABLE test_schema.rv_cascade_codes (code text PRIMARY KEY);
INSERT INTO test_schema.rv_cascade_codes VALUES ('ok');
CREATE FUNCTION test_schema.rv_cascade_is_valid(x text) RETURNS boolean
LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT EXISTS (SELECT 1 FROM test_schema.rv_cascade_codes WHERE code = x);
END;
CREATE FUNCTION test_schema.rv_cascade_norm(x text) RETURNS text
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT lower(x) || coalesce((SELECT min(code) FROM test_schema.rv_cascade_codes WHERE false), '');
END;
CREATE FUNCTION test_schema.rv_cascade_first_code() RETURNS text
LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT min(code) FROM test_schema.rv_cascade_codes;
END;
CREATE DOMAIN test_schema.rv_cascade_code_d AS text
    CONSTRAINT rv_cascade_code_d_check CHECK (test_schema.rv_cascade_is_valid(VALUE));
-- The domain depends on its default: DROP ... CASCADE of rv_cascade_first_code
-- would drop it and the column "fallback" below with its data.
CREATE DOMAIN test_schema.rv_cascade_code_def AS text
    DEFAULT test_schema.rv_cascade_first_code();
CREATE TABLE test_schema.rv_cascade_orders (
    id       integer PRIMARY KEY,
    code     text,
    checked  test_schema.rv_cascade_code_d,
    fallback test_schema.rv_cascade_code_def
);
INSERT INTO test_schema.rv_cascade_orders VALUES (1, 'ok', 'ok', 'kept');
CREATE FUNCTION test_schema.rv_cascade_reject() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rv_cascade: rejected'; END $$;
CREATE TRIGGER rv_cascade_orders_chk BEFORE INSERT ON test_schema.rv_cascade_orders
    FOR EACH ROW WHEN (NOT test_schema.rv_cascade_is_valid(NEW.code))
    EXECUTE FUNCTION test_schema.rv_cascade_reject();
CREATE STATISTICS test_schema.rv_cascade_orders_st
    ON (test_schema.rv_cascade_norm(code)), id FROM test_schema.rv_cascade_orders;
CREATE FUNCTION test_schema.rv_cascade_flag(ok boolean DEFAULT test_schema.rv_cascade_is_valid('ok'))
RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN RETURN ok; END $$;
REVOKE EXECUTE ON FUNCTION test_schema.rv_cascade_flag(boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION test_schema.rv_cascade_flag(boolean) TO pgc_pg19_sqlbody_reader;

-- plpgsql variant: the return type of rv_cascade_score changes (integer -> bigint).
CREATE FUNCTION test_schema.rv_cascade_score(x integer) RETURNS integer
LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN x; END $$;
CREATE DOMAIN test_schema.rv_cascade_score_d AS integer
    CONSTRAINT rv_cascade_score_d_check CHECK (test_schema.rv_cascade_score(VALUE) > 0);
CREATE DOMAIN test_schema.rv_cascade_score_def AS bigint
    DEFAULT test_schema.rv_cascade_score(7);
CREATE TABLE test_schema.rv_cascade_scores (
    id       integer PRIMARY KEY,
    qty      integer,
    positive test_schema.rv_cascade_score_d,
    preset   test_schema.rv_cascade_score_def
);
INSERT INTO test_schema.rv_cascade_scores VALUES (1, 5, 3, 42);
CREATE TRIGGER rv_cascade_scores_guard BEFORE INSERT ON test_schema.rv_cascade_scores
    FOR EACH ROW WHEN (test_schema.rv_cascade_score(NEW.qty) < 0)
    EXECUTE FUNCTION test_schema.rv_cascade_reject();
CREATE STATISTICS test_schema.rv_cascade_scores_st
    ON (test_schema.rv_cascade_score(qty)), id FROM test_schema.rv_cascade_scores;
CREATE FUNCTION test_schema.rv_cascade_score_or(x bigint DEFAULT test_schema.rv_cascade_score(1))
RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN x; END $$;

-- ---- reg-cascade review fixes (same rv_cascade_ prefix, so the reg-cascade
-- assertion covers them).
-- A materialized view that calls nothing, with indexes calling a routine
-- dropped with CASCADE: the CASCADE takes the index alone.
CREATE MATERIALIZED VIEW test_schema.rv_cascade_orders_mv AS
    SELECT id, code FROM test_schema.rv_cascade_orders;
CREATE INDEX rv_cascade_orders_mv_norm_idx
    ON test_schema.rv_cascade_orders_mv (test_schema.rv_cascade_norm(code));
CREATE MATERIALIZED VIEW test_schema.rv_cascade_scores_mv AS
    SELECT id, qty FROM test_schema.rv_cascade_scores;
CREATE INDEX rv_cascade_scores_mv_idx
    ON test_schema.rv_cascade_scores_mv (test_schema.rv_cascade_score(qty));
-- Foreign-table column defaults calling them.
CREATE FOREIGN DATA WRAPPER rv_cascade_fdw;
CREATE SERVER rv_cascade_srv FOREIGN DATA WRAPPER rv_cascade_fdw;
CREATE FOREIGN TABLE test_schema.rv_cascade_ft (
    id     integer,
    code   text DEFAULT test_schema.rv_cascade_first_code(),
    preset bigint DEFAULT test_schema.rv_cascade_score(9)
) SERVER rv_cascade_srv;
-- Taken through rv_cascade_score_or's argument default: a SQL-standard body
-- calling it, and a view calling that.
CREATE FUNCTION test_schema.rv_cascade_score_reader() RETURNS bigint
LANGUAGE sql STABLE
RETURN test_schema.rv_cascade_score_or();
CREATE VIEW test_schema.rv_cascade_score_view AS
    SELECT test_schema.rv_cascade_score_reader() AS r;
-- A CHECK calling rv_cascade_is_valid that TO rewrites without it: the
-- predrop's CASCADE takes it, and the table phase adds TO's.
ALTER TABLE test_schema.rv_cascade_orders
    ADD CONSTRAINT rv_cascade_orders_id_chk CHECK (test_schema.rv_cascade_is_valid(code) OR id > 0);
-- Partition clones of a WHEN trigger calling rv_cascade_is_valid, one
-- disabled on its own, one with its own comment (listed from PostgreSQL 15).
CREATE TABLE test_schema.rv_cascade_parted (id integer, code text) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_cascade_parted_1 PARTITION OF test_schema.rv_cascade_parted
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_cascade_parted_2 PARTITION OF test_schema.rv_cascade_parted
    FOR VALUES FROM (100) TO (200);
CREATE TRIGGER rv_cascade_parted_chk AFTER INSERT ON test_schema.rv_cascade_parted
    FOR EACH ROW WHEN (NOT test_schema.rv_cascade_is_valid(NEW.code))
    EXECUTE FUNCTION test_schema.rv_cascade_reject();
ALTER TABLE test_schema.rv_cascade_parted_1 DISABLE TRIGGER rv_cascade_parted_chk;
COMMENT ON TRIGGER rv_cascade_parted_chk ON test_schema.rv_cascade_parted_2 IS 'rv_cascade: clone comment';

-- ---- pre-cascade: what a routine's DROP ... CASCADE takes through a view,
-- and the privileges of what the migration re-creates (rv_pcascade_ prefix,
-- asserted by .github/scripts/assert-pre-cascade.sh).
-- The helper data.rv_pcascade_norm changes its return type (text -> varchar),
-- so the routine phase drops it with CASCADE, which takes every view calling
-- it and everything reading those: a view over it, a materialized view with
-- an index and statistics, a SQL-standard routine and a rule. The routines
-- live in the data schema, whose default privileges for functions (below)
-- no other fixture's routines see: a routine the migration creates starts
-- with them, and both sides revoke what they grant.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_pcascade_reader') THEN
        CREATE ROLE pgc_pg19_pcascade_reader;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_pcascade_exec') THEN
        CREATE ROLE pgc_pg19_pcascade_exec;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_pcascade_owner') THEN
        CREATE ROLE pgc_pg19_pcascade_owner;
    END IF;
END;
$$;
-- Created before the default privileges below, so their ACL is NULL: the
-- return type of rv_pcascade_plain_ret changes (integer -> bigint), and
-- rv_pcascade_plain_new is TO only. The migration creates both under those
-- defaults and revokes what they grant; that leaves the default spelled out,
-- {=X/postgres,postgres=X/postgres}, which the next compare has to take for
-- the NULL ACL it stands for rather than revoke PUBLIC's EXECUTE.
CREATE FUNCTION data.rv_pcascade_plain_ret(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT x $$;
ALTER DEFAULT PRIVILEGES IN SCHEMA data GRANT EXECUTE ON FUNCTIONS TO pgc_pg19_pcascade_exec;
-- Another role's default privileges in the schema: the migration does not
-- know which role creates a routine, so it plans for this one too.
ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_pcascade_owner IN SCHEMA data
    GRANT EXECUTE ON FUNCTIONS TO pgc_pg19_pcascade_reader;
CREATE FUNCTION data.rv_pcascade_norm(t text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT lower(t)::text $$;
REVOKE EXECUTE ON FUNCTION data.rv_pcascade_norm(text) FROM pgc_pg19_pcascade_exec;
GRANT EXECUTE ON FUNCTION data.rv_pcascade_norm(text) TO pgc_pg19_pcascade_reader;
CREATE TABLE test_schema.rv_pcascade_items (id integer PRIMARY KEY, code text);
INSERT INTO test_schema.rv_pcascade_items VALUES (1, 'A'), (2, 'B');
-- Calls the helper and is read by what follows: dropped ahead of the table
-- changes together with its readers, all re-created after the helper. The
-- test_schema default privileges give pgc_grant_reader SELECT on every new
-- view; this one revokes it.
CREATE VIEW test_schema.rv_pcascade_items_v AS
    SELECT id, data.rv_pcascade_norm(code) AS n FROM test_schema.rv_pcascade_items;
GRANT SELECT ON test_schema.rv_pcascade_items_v TO pgc_pg19_pcascade_reader;
REVOKE SELECT ON test_schema.rv_pcascade_items_v FROM pgc_grant_reader;
-- The owner of the view over it reads it.
GRANT SELECT ON test_schema.rv_pcascade_items_v TO pgc_pg19_pcascade_owner;
COMMENT ON VIEW test_schema.rv_pcascade_items_v IS 'rv_pcascade: calls the helper';
CREATE VIEW test_schema.rv_pcascade_items_v2 AS
    SELECT id, n FROM test_schema.rv_pcascade_items_v;
ALTER VIEW test_schema.rv_pcascade_items_v2 OWNER TO pgc_pg19_pcascade_owner;
GRANT SELECT, UPDATE ON test_schema.rv_pcascade_items_v2 TO pgc_pg19_pcascade_reader;
COMMENT ON COLUMN test_schema.rv_pcascade_items_v2.n IS 'rv_pcascade: normalised code';
CREATE MATERIALIZED VIEW test_schema.rv_pcascade_items_mv AS
    SELECT id, n FROM test_schema.rv_pcascade_items_v;
CREATE UNIQUE INDEX rv_pcascade_items_mv_id ON test_schema.rv_pcascade_items_mv (id);
COMMENT ON INDEX test_schema.rv_pcascade_items_mv_id IS 'rv_pcascade: matview index';
CREATE STATISTICS test_schema.rv_pcascade_items_mv_st (ndistinct)
    ON id, n FROM test_schema.rv_pcascade_items_mv;
GRANT SELECT ON test_schema.rv_pcascade_items_mv TO pgc_pg19_pcascade_reader;
COMMENT ON MATERIALIZED VIEW test_schema.rv_pcascade_items_mv IS 'rv_pcascade: over the view';
-- A SQL-standard body reading the second view: PUBLIC and the function
-- default's grantee are revoked, the reader granted.
CREATE FUNCTION data.rv_pcascade_count() RETURNS bigint
LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT count(*) FROM test_schema.rv_pcascade_items_v2;
END;
REVOKE EXECUTE ON FUNCTION data.rv_pcascade_count() FROM PUBLIC, pgc_pg19_pcascade_exec;
GRANT EXECUTE ON FUNCTION data.rv_pcascade_count() TO pgc_pg19_pcascade_reader;
-- A rule on the second view reading the first; its actions run as the
-- view's owner.
CREATE TABLE test_schema.rv_pcascade_log (id integer);
GRANT INSERT ON test_schema.rv_pcascade_log TO pgc_pg19_pcascade_owner;
CREATE RULE rv_pcascade_items_v2_del AS ON DELETE TO test_schema.rv_pcascade_items_v2
    DO INSTEAD INSERT INTO test_schema.rv_pcascade_log
        SELECT id FROM test_schema.rv_pcascade_items_v WHERE id = OLD.id;
-- A view calling the helper that nothing reads: re-created right after it
-- (the Phase 7 path), with its privileges and comment.
CREATE VIEW test_schema.rv_pcascade_solo_v AS
    SELECT id, data.rv_pcascade_norm(code) AS n FROM test_schema.rv_pcascade_items;
GRANT SELECT ON test_schema.rv_pcascade_solo_v TO pgc_pg19_pcascade_reader;
REVOKE SELECT ON test_schema.rv_pcascade_solo_v FROM pgc_grant_reader;
COMMENT ON VIEW test_schema.rv_pcascade_solo_v IS 'rv_pcascade: nothing reads it';
-- An unchanged overload of the helper, called by a view in public that a
-- materialized view reads and that has an INSTEAD OF trigger (which pgc does
-- not model). The routine phase's CASCADE of the helper does not take it,
-- although its name matches: it is neither dropped nor rebuilt, and keeps its
-- trigger. A delegate holding the grant option granted SELECT to the reader
-- (FROM only): the view keeps its privileges, so that entry is revoked in
-- the delegate's name.
CREATE FUNCTION data.rv_pcascade_norm(t integer) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT t::text $$;
CREATE VIEW public.rv_pcascade_int_v AS
    SELECT id, data.rv_pcascade_norm(id) AS n FROM test_schema.rv_pcascade_items;
CREATE MATERIALIZED VIEW public.rv_pcascade_int_mv AS
    SELECT id, n FROM public.rv_pcascade_int_v;
CREATE UNIQUE INDEX rv_pcascade_int_mv_id ON public.rv_pcascade_int_mv (id);
CREATE FUNCTION data.rv_pcascade_int_ins() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN INSERT INTO test_schema.rv_pcascade_log VALUES (NEW.id); RETURN NULL; END $$;
CREATE TRIGGER rv_pcascade_int_ins INSTEAD OF INSERT ON public.rv_pcascade_int_v
    FOR EACH ROW EXECUTE FUNCTION data.rv_pcascade_int_ins();
GRANT SELECT ON public.rv_pcascade_int_v TO pgc_pg19_pcascade_owner WITH GRANT OPTION;
SET ROLE pgc_pg19_pcascade_owner;
GRANT SELECT ON public.rv_pcascade_int_v TO pgc_pg19_pcascade_reader;
RESET ROLE;

-- pre-clear: dependencies the clear command has to order (or break) from
-- pg_depend. Identical on both sides, so the compare leaves them alone;
-- `pgc --command clear` on a database loaded from either file drops them
-- without an error and without CASCADE (see data/test/README.md):
--   * rv_preclear_cycle's CHECK calls rv_preclear_cycle_max(), whose body
--     reads rv_preclear_cycle, a cycle: the constraint is dropped first
--   * rv_preclear_rule_src's rule reads rv_preclear_rule_count, a view over
--     rv_preclear_rule_src, a cycle: the rule is dropped first
--   * rv_preclear_snap's default calls rv_preclear_schema_count(), whose body
--     reads only information_schema: the table goes first
--   * rv_preclear_agg over the plpgsql rv_preclear_agg_sfunc, created in the
--     usual order: the aggregate goes first
--   * rv_preclear_gen's stored generated column calls rv_preclear_gen_bump(),
--     whose body reads rv_preclear_gen, under a view reading the column: on
--     PG15+ a cycle broken by DROP EXPRESSION (dropping the column is blocked
--     by the view); on PG14 dropping the function drops the column, so the
--     view goes first
--   * rv_preclear_ddl_audit logs every DROP TABLE into rv_preclear_ddl_log:
--     the event trigger is dropped before any table (it would fail the drop
--     of its own log table)
CREATE TABLE test_schema.rv_preclear_cycle (x integer);
CREATE FUNCTION test_schema.rv_preclear_cycle_max() RETURNS integer
LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT max(x) FROM test_schema.rv_preclear_cycle;
END;
ALTER TABLE test_schema.rv_preclear_cycle
    ADD CONSTRAINT rv_preclear_cycle_chk
    CHECK (x IS NULL OR x > -1000000 OR test_schema.rv_preclear_cycle_max() IS NULL);
CREATE TABLE test_schema.rv_preclear_rule_src (id integer);
CREATE TABLE test_schema.rv_preclear_rule_log (n bigint);
CREATE VIEW test_schema.rv_preclear_rule_count AS
    SELECT count(*) AS n FROM test_schema.rv_preclear_rule_src;
CREATE RULE rv_preclear_rule_log_count AS ON INSERT TO test_schema.rv_preclear_rule_src
    DO ALSO INSERT INTO test_schema.rv_preclear_rule_log
        SELECT n FROM test_schema.rv_preclear_rule_count;
CREATE FUNCTION test_schema.rv_preclear_schema_count() RETURNS bigint
LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT count(*) FROM information_schema.schemata;
END;
CREATE TABLE test_schema.rv_preclear_snap (
    n bigint DEFAULT test_schema.rv_preclear_schema_count()
);
CREATE FUNCTION test_schema.rv_preclear_agg_sfunc(state bigint, v integer) RETURNS bigint
LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN state + v; END $$;
CREATE AGGREGATE test_schema.rv_preclear_agg(integer) (
    SFUNC = test_schema.rv_preclear_agg_sfunc,
    STYPE = bigint,
    INITCOND = '0'
);
CREATE TABLE test_schema.rv_preclear_gen (a integer);
CREATE FUNCTION test_schema.rv_preclear_gen_bump(x integer) RETURNS integer
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT x + (SELECT count(*) FROM test_schema.rv_preclear_gen)::integer;
END;
ALTER TABLE test_schema.rv_preclear_gen
    ADD COLUMN g integer GENERATED ALWAYS AS (test_schema.rv_preclear_gen_bump(a)) STORED;
CREATE VIEW test_schema.rv_preclear_gen_view AS
    SELECT a, g FROM test_schema.rv_preclear_gen;
CREATE FUNCTION test_schema.rv_preclear_gen_view_count() RETURNS bigint
LANGUAGE sql STABLE
BEGIN ATOMIC
    SELECT count(*) FROM test_schema.rv_preclear_gen_view;
END;
CREATE TABLE test_schema.rv_preclear_ddl_log (id bigserial PRIMARY KEY, tag text);
CREATE FUNCTION test_schema.rv_preclear_log_ddl() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_schema.rv_preclear_ddl_log (tag) VALUES (tg_tag);
END $$;
CREATE EVENT TRIGGER rv_preclear_ddl_audit ON ddl_command_end
    WHEN TAG IN ('DROP TABLE')
    EXECUTE FUNCTION test_schema.rv_preclear_log_ddl();

-- ==== END fix:sqlbody ====

-- ==== BEGIN fix:domains — domain CHECK constraint change detection and hash ordering ====
-- FROM side. Every domain and the enum below differs between FROM and TO in
-- its CHECK constraints (or labels) ONLY — no owner, comment or default
-- change — so the type hash alone has to notice it. pgc <= 1.0.27 hashed a
-- type before its domain constraints and enum labels were loaded, and
-- migrated none of these. Plain SQL on PG14+, except the IS JSON pair
-- (PG19+; see below).
CREATE DOMAIN test_schema.pg19_domains_body AS integer
    CONSTRAINT pg19_domains_body_range CHECK (VALUE > 0);          -- body changes
CREATE DOMAIN test_schema.pg19_domains_gain AS text;              -- gains a constraint
CREATE DOMAIN test_schema.pg19_domains_lose AS text
    CONSTRAINT pg19_domains_lose_len CHECK (char_length(VALUE) <= 8);  -- loses it
CREATE DOMAIN test_schema.pg19_domains_validate AS integer;       -- NOT VALID -> validated
                                                                  -- (constraint added below)
CREATE DOMAIN test_schema.pg19_domains_invalidate AS integer      -- validated -> NOT VALID
    CONSTRAINT pg19_domains_invalidate_nonneg CHECK (VALUE >= 0);
-- Unchanged, two constraints declared in the opposite order to TO: the
-- stored order is by name either way, so this must stay out of the diff.
CREATE DOMAIN test_schema.pg19_domains_same AS integer
    CONSTRAINT "Pg19_domains_same_min" CHECK (VALUE > 1)
    CONSTRAINT pg19_domains_same_max CHECK (VALUE < 100);
-- Enum whose only change is an inserted label (label ORDER is significant).
CREATE TYPE test_schema.pg19_domains_level AS ENUM ('low', 'high');
-- The CHECK calls a routine TO drops; TO's CHECK calls a routine only TO
-- has. The new CHECK can only be added once its routine exists, and the old
-- routine only dropped once the old CHECK is gone.
CREATE FUNCTION test_schema.pg19_domains_old_sku(v text) RETURNS boolean
    LANGUAGE sql IMMUTABLE AS $$ SELECT char_length(v) > 0 $$;
CREATE DOMAIN test_schema.pg19_domains_sku AS text
    CONSTRAINT pg19_domains_sku_fmt CHECK (test_schema.pg19_domains_old_sku(VALUE));
-- Live data in the changed domains, so the re-added CHECKs are validated
-- against rows on apply. old_qty (dropped in TO) holds 2, which the
-- tightened pg19_domains_body CHECK (> 5) rejects: the CHECK has to be added
-- after the column is dropped.
CREATE TABLE test_schema.pg19_domains_usage (
    id      integer PRIMARY KEY,
    qty     test_schema.pg19_domains_body,
    code    test_schema.pg19_domains_gain,
    tag     test_schema.pg19_domains_lose,
    level   test_schema.pg19_domains_level,
    sku     test_schema.pg19_domains_sku,
    old_qty test_schema.pg19_domains_body
);
INSERT INTO test_schema.pg19_domains_usage VALUES (1, 10, 'ok', 'short', 'high', 'ABC', 2);
-- Dropped in TO, holding values the TO-side CHECKs reject: 3 (body > 5) and
-- -3 (the NOT VALID nonneg CHECK, validated in TO). Adding / validating those
-- CHECKs has to wait until this table is gone.
CREATE TABLE test_schema.pg19_domains_legacy (
    qty    test_schema.pg19_domains_body,
    nonneg test_schema.pg19_domains_validate
);
INSERT INTO test_schema.pg19_domains_legacy VALUES (3, -3);
ALTER DOMAIN test_schema.pg19_domains_validate
    ADD CONSTRAINT pg19_domains_validate_nonneg CHECK (VALUE >= 0) NOT VALID;
-- PG19: IS JSON on a domain over a jsonb domain. PG16-18 reject the CHECK
-- ("cannot use type ... in IS JSON predicate"), PG14-15 have no IS JSON; it
-- fails identically on both sides there and the round trip still closes.
CREATE DOMAIN test_schema.pg19_domains_jsonb AS jsonb;
CREATE DOMAIN test_schema.pg19_domains_json_doc AS test_schema.pg19_domains_jsonb;
ALTER DOMAIN test_schema.pg19_domains_json_doc
    ADD CONSTRAINT pg19_domains_json_doc_shape CHECK (VALUE IS JSON ARRAY);

-- ==== END fix:domains ====

-- ==== BEGIN fix:grants — REVOKE of privileges granted by a non-owner (GRANTED BY) ====
-- =============================================================================
-- Grantor chains in full grants mode (FROM side)
-- =============================================================================
-- An entry granted by a role other than the object's owner (a delegate holding
-- WITH GRANT OPTION) survives a plain REVOKE run by the owner or a superuser,
-- without an error, and revoking a grant option other entries were granted
-- under fails with "dependent privileges exist". Full grants mode has to name
-- the grantor: REVOKE ... GRANTED BY on PG19+, SET ROLE <grantor> before.
-- The objects live in public, which has no default privileges, and belong to
-- pgc_pg19_grants_owner; a superuser's GRANT records that owner as grantor.
--   pg19_grants_chain      : delegate SELECT, UPDATE and SELECT (note) WITH
--                            GRANT OPTION (kept in TO); reader SELECT, UPDATE,
--                            PUBLIC SELECT and sub SELECT (note), all granted
--                            by the delegate (dropped in TO)
--   pg19_grants_opt        : owner -> delegate -> sub -> reader, each WITH
--                            GRANT OPTION but the last; TO keeps delegate
--                            SELECT without the option and reader SELECT from
--                            the owner, so the chain is revoked leaf first and
--                            reader's SELECT granted back by the owner
--   pg19_grants_seq        : USAGE granted by a mixed-case delegate (dropped)
--   pg19_grants_fn         : EXECUTE granted by the delegate (dropped)
--   schema public          : USAGE granted by the delegate (dropped)
--   pg19_grants_granted_by : GRANTED BY (PG19+ only; PG14-18 reject it with
--                            "grantor must be current user" on both sides):
--                            reader SELECT and sub SELECT (note), dropped
--   pg19_grants_same       : reader SELECT differs only by grantor (PG19+):
--                            expected no diff
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_grants_owner') THEN
          CREATE ROLE pgc_pg19_grants_owner;
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_grants_delegate') THEN
          CREATE ROLE pgc_pg19_grants_delegate;
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_grants_sub') THEN
          CREATE ROLE pgc_pg19_grants_sub;
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_grants_reader') THEN
          CREATE ROLE pgc_pg19_grants_reader;
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_Grants_Mixed') THEN
          CREATE ROLE "pgc_pg19_Grants_Mixed";
     END IF;
END;
$$;
CREATE TABLE public.pg19_grants_chain (id integer PRIMARY KEY, note text);
ALTER TABLE public.pg19_grants_chain OWNER TO pgc_pg19_grants_owner;
CREATE TABLE public.pg19_grants_opt (id integer PRIMARY KEY);
ALTER TABLE public.pg19_grants_opt OWNER TO pgc_pg19_grants_owner;
CREATE SEQUENCE public.pg19_grants_seq;
ALTER SEQUENCE public.pg19_grants_seq OWNER TO pgc_pg19_grants_owner;
CREATE FUNCTION public.pg19_grants_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1';
ALTER FUNCTION public.pg19_grants_fn() OWNER TO pgc_pg19_grants_owner;
CREATE TABLE public.pg19_grants_granted_by (id integer PRIMARY KEY, note text);
ALTER TABLE public.pg19_grants_granted_by OWNER TO pgc_pg19_grants_owner;
CREATE TABLE public.pg19_grants_same (id integer PRIMARY KEY);
ALTER TABLE public.pg19_grants_same OWNER TO pgc_pg19_grants_owner;
-- Grant options, recorded with the owner as grantor
GRANT SELECT, UPDATE ON public.pg19_grants_chain TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.pg19_grants_chain TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.pg19_grants_opt TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT USAGE ON SEQUENCE public.pg19_grants_seq TO "pgc_pg19_Grants_Mixed" WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION public.pg19_grants_fn() TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT USAGE ON SCHEMA public TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.pg19_grants_granted_by, public.pg19_grants_same
    TO pgc_pg19_grants_delegate WITH GRANT OPTION;
-- Delegated grants, recorded with the delegate as grantor (every major)
SET ROLE pgc_pg19_grants_delegate;
GRANT SELECT, UPDATE ON public.pg19_grants_chain TO pgc_pg19_grants_reader;
GRANT SELECT ON public.pg19_grants_chain TO PUBLIC;
GRANT SELECT (note) ON public.pg19_grants_chain TO pgc_pg19_grants_sub;
GRANT SELECT ON public.pg19_grants_opt TO pgc_pg19_grants_sub WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION public.pg19_grants_fn() TO pgc_pg19_grants_reader;
GRANT USAGE ON SCHEMA public TO pgc_pg19_grants_reader;
RESET ROLE;
SET ROLE pgc_pg19_grants_sub;
GRANT SELECT ON public.pg19_grants_opt TO pgc_pg19_grants_reader;
RESET ROLE;
SET ROLE "pgc_pg19_Grants_Mixed";
GRANT USAGE ON SEQUENCE public.pg19_grants_seq TO pgc_pg19_grants_reader;
RESET ROLE;
-- GRANTED BY (PG19+)
GRANT SELECT ON public.pg19_grants_granted_by TO pgc_pg19_grants_reader
    GRANTED BY pgc_pg19_grants_delegate;
GRANT SELECT (note) ON public.pg19_grants_granted_by TO pgc_pg19_grants_sub
    GRANTED BY pgc_pg19_grants_delegate;
GRANT SELECT ON public.pg19_grants_same TO pgc_pg19_grants_reader
    GRANTED BY pgc_pg19_grants_delegate;
-- Table-level REVOKEs reach the columns: PostgreSQL applies a table REVOKE to
-- every column's ACL too, removing the column entries the same grantor gave
-- the same grantee (every major).
--   pg19_grants_colkeep : reader SELECT on the table and on note, both from
--                         the owner; sub SELECT on the table and on note, both
--                         from the delegate. TO keeps only the column grants,
--                         so the table REVOKEs must be followed by column
--                         GRANTs in the same run
--   pg19_grants_coldep  : delegate SELECT WITH GRANT OPTION on the table and
--                         on note (from the owner), reader SELECT (note) from
--                         the delegate. TO takes only the table-level option
--                         away: reader's column entry is revoked first (the
--                         table statement would otherwise fail with
--                         "dependent privileges exist"), then both column
--                         grants come back from the owner
CREATE TABLE public.pg19_grants_colkeep (id integer PRIMARY KEY, note text);
ALTER TABLE public.pg19_grants_colkeep OWNER TO pgc_pg19_grants_owner;
CREATE TABLE public.pg19_grants_coldep (id integer PRIMARY KEY, note text);
ALTER TABLE public.pg19_grants_coldep OWNER TO pgc_pg19_grants_owner;
GRANT SELECT ON public.pg19_grants_colkeep TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.pg19_grants_colkeep TO pgc_pg19_grants_reader;
GRANT SELECT (note) ON public.pg19_grants_colkeep TO pgc_pg19_grants_reader;
GRANT SELECT ON public.pg19_grants_coldep TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.pg19_grants_coldep TO pgc_pg19_grants_delegate WITH GRANT OPTION;
SET ROLE pgc_pg19_grants_delegate;
GRANT SELECT ON public.pg19_grants_colkeep TO pgc_pg19_grants_sub;
GRANT SELECT (note) ON public.pg19_grants_colkeep TO pgc_pg19_grants_sub;
GRANT SELECT (note) ON public.pg19_grants_coldep TO pgc_pg19_grants_reader;
RESET ROLE;
-- Privileges the table DDL itself takes away (reg-misc). A column that the
-- migration drops and re-adds, or that belongs to a table it drops and
-- recreates, starts with no privileges: what b has is granted again, and
-- no FROM entry is revoked (in its grantor's name that fails with
-- "permission denied for column"). A partition whose column type changes
-- through its parent is not dropped and keeps its privileges.
--   rv_regmisc_grants_readd   : g becomes generated (a drop + add on every
--                               version); reader and delegate keep SELECT
--                               (g), sub's SELECT (g) from the delegate goes
--   rv_regmisc_grants_rebuilt : partition key changes (drop + create);
--                               reader and delegate keep SELECT (code),
--                               sub's SELECT (code) from the delegate goes
--   rv_regmisc_grants_pt1     : partition of rv_regmisc_grants_pt, whose v
--                               becomes bigint; reader's SELECT (from the
--                               owner) and sub's (from the delegate) go
CREATE TABLE public.rv_regmisc_grants_readd (id integer PRIMARY KEY, qty integer, g integer, note text);
ALTER TABLE public.rv_regmisc_grants_readd OWNER TO pgc_pg19_grants_owner;
GRANT SELECT (g) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_reader;
GRANT SELECT (g) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_reader;
CREATE TABLE public.rv_regmisc_grants_rebuilt (id integer NOT NULL, k integer NOT NULL, code text) PARTITION BY RANGE (id);
ALTER TABLE public.rv_regmisc_grants_rebuilt OWNER TO pgc_pg19_grants_owner;
GRANT SELECT ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (code) ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_reader;
GRANT SELECT (code) ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_delegate WITH GRANT OPTION;
CREATE TABLE public.rv_regmisc_grants_pt (id integer NOT NULL, v integer) PARTITION BY RANGE (id);
CREATE TABLE public.rv_regmisc_grants_pt1 PARTITION OF public.rv_regmisc_grants_pt FOR VALUES FROM (0) TO (100);
ALTER TABLE public.rv_regmisc_grants_pt OWNER TO pgc_pg19_grants_owner;
ALTER TABLE public.rv_regmisc_grants_pt1 OWNER TO pgc_pg19_grants_owner;
GRANT SELECT ON public.rv_regmisc_grants_pt1 TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.rv_regmisc_grants_pt1 TO pgc_pg19_grants_reader;
SET ROLE pgc_pg19_grants_delegate;
GRANT SELECT (g) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_sub;
GRANT SELECT (code) ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_sub;
GRANT SELECT ON public.rv_regmisc_grants_pt1 TO pgc_pg19_grants_sub;
RESET ROLE;

-- pre-grants (rv_pgrants_*, every version). Grantees are spelled as SQL
-- needs them, and a NULL ACL is compared as the default privileges it stands
-- for. aclitem text leaves "pgc_pg19_Pgrants_Reader" unquoted, which as SQL
-- names pgc_pg19_pgrants_reader; the spaced role did not parse at all.
--   rv_pgrants_tbl        : no grants -> SELECT and UPDATE (note) to the
--                           mixed-case role, INSERT to the spaced one
--   rv_pgrants_tbl_revoke : SELECT to the mixed-case role -> none
--   rv_pgrants_chain      : the mixed-case delegate holds SELECT WITH GRANT
--                           OPTION and granted SELECT to the mixed-case
--                           reader -> none: the grantor-named revoke and the
--                           owner-level ones spell the delegate alike
--   rv_pgrants_secret()   : SECURITY DEFINER, NULL ACL -> EXECUTE revoked
--                           from PUBLIC (was never migrated)
--   rv_pgrants_dom        : domain, NULL ACL -> USAGE revoked from PUBLIC
--   rv_pgrants_enum       : enum, NULL ACL -> USAGE revoked from PUBLIC
--   rv_pgrants_granted()  : NULL ACL -> EXECUTE to the mixed-case role and
--                           revoked from PUBLIC, both in the first run
--   rv_pgrants_reopen()   : PUBLIC revoked -> NULL ACL: GRANT ... TO PUBLIC
--   rv_pgrants_kept()     : EXECUTE to the spaced role -> NULL ACL: only that
--                           role's EXECUTE goes (PUBLIC's was revoked too)
--   rv_pgrants_emptied()  : every privilege revoked, the owner's too
--                           (proacl '{}', not NULL) -> NULL ACL: GRANT ... TO
--                           PUBLIC
--   TO only: rv_pgrants_new_fn() and rv_pgrants_new_dom, created with PUBLIC
--   revoked, and a default privilege granted to the mixed-case role
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_Pgrants_Reader') THEN
          CREATE ROLE "pgc_pg19_Pgrants_Reader";
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_pgrants reader') THEN
          CREATE ROLE "pgc_pg19_pgrants reader";
     END IF;
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_Pgrants_Deleg') THEN
          CREATE ROLE "pgc_pg19_Pgrants_Deleg";
     END IF;
END;
$$;
CREATE TABLE public.rv_pgrants_tbl (id integer PRIMARY KEY, note text);
CREATE TABLE public.rv_pgrants_tbl_revoke (id integer PRIMARY KEY);
GRANT SELECT ON public.rv_pgrants_tbl_revoke TO "pgc_pg19_Pgrants_Reader";
CREATE TABLE public.rv_pgrants_chain (id integer PRIMARY KEY);
GRANT SELECT ON public.rv_pgrants_chain TO "pgc_pg19_Pgrants_Deleg" WITH GRANT OPTION;
SET ROLE "pgc_pg19_Pgrants_Deleg";
GRANT SELECT ON public.rv_pgrants_chain TO "pgc_pg19_Pgrants_Reader";
RESET ROLE;
CREATE FUNCTION public.rv_pgrants_secret() RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'SELECT 42';
CREATE DOMAIN public.rv_pgrants_dom AS integer;
CREATE TYPE public.rv_pgrants_enum AS ENUM ('a', 'b');
CREATE FUNCTION public.rv_pgrants_granted() RETURNS integer LANGUAGE sql AS 'SELECT 1';
CREATE FUNCTION public.rv_pgrants_reopen() RETURNS integer LANGUAGE sql AS 'SELECT 1';
REVOKE EXECUTE ON FUNCTION public.rv_pgrants_reopen() FROM PUBLIC;
CREATE FUNCTION public.rv_pgrants_kept() RETURNS integer LANGUAGE sql AS 'SELECT 1';
GRANT EXECUTE ON FUNCTION public.rv_pgrants_kept() TO "pgc_pg19_pgrants reader";
CREATE FUNCTION public.rv_pgrants_emptied() RETURNS integer LANGUAGE sql AS 'SELECT 1';
REVOKE ALL ON FUNCTION public.rv_pgrants_emptied() FROM PUBLIC, CURRENT_USER;

-- pre-grants review (every version): objects the migration creates. The FROM
-- default privileges belong to pgc_pg19_pgrants_app (PUBLIC's USAGE on types
-- revoked database-wide, EXECUTE on functions granted to the mixed-case role
-- in test_order), while CI runs the migration as a superuser, whose objects
-- start from acldefault(). Only the creating role's rows apply, so they must
-- not be taken for granted. Whoever runs it, what the target lacks is revoked
-- and what it holds is granted, so every new type with a NULL ACL gets a
-- no-op GRANT USAGE ... TO PUBLIC (and an equivalent explicit ACL); test_order
-- has no other new routine for the schema row to reach.
--   test_order.rv_pgrants_app_fr()
--                        : SECURITY DEFINER owned by the app role, PUBLIC
--                          revoked and the mixed-case role granted, as its
--                          defaults would; integer -> bigint (drop + create):
--                          both statements again (the GRANT was taken for
--                          granted)
--   TO only: test_order.rv_pgrants_app_new(), likewise, and rv_pgrants_app_dom, with
--   PUBLIC revoked (the REVOKE was taken for granted)
--   rv_pgrants_range     : range type, USAGE revoked from PUBLIC and granted
--                          to the mixed-case role on both sides; subtype_diff
--                          removed (drop + create): both statements again
-- A database-wide REVOKE ... ON FUNCTIONS FROM PUBLIC is left out: every new
-- routine would get a GRANT EXECUTE ... TO PUBLIC, and the one on the
-- ordered-set aggregate pg19_routines_agg_kind fails (its GRANT signature
-- keeps ORDER BY, a separate bug). The unit tests cover that case.
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_pgrants_app') THEN
          CREATE ROLE pgc_pg19_pgrants_app;
     END IF;
END;
$$;
ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_pgrants_app REVOKE USAGE ON TYPES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_pgrants_app IN SCHEMA test_order
    GRANT EXECUTE ON FUNCTIONS TO "pgc_pg19_Pgrants_Reader";
CREATE FUNCTION test_order.rv_pgrants_app_fr() RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'SELECT 1';
REVOKE EXECUTE ON FUNCTION test_order.rv_pgrants_app_fr() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION test_order.rv_pgrants_app_fr() TO "pgc_pg19_Pgrants_Reader";
ALTER FUNCTION test_order.rv_pgrants_app_fr() OWNER TO pgc_pg19_pgrants_app;
CREATE TYPE public.rv_pgrants_range AS RANGE (subtype = float8, subtype_diff = float8mi);
REVOKE USAGE ON TYPE public.rv_pgrants_range FROM PUBLIC;
GRANT USAGE ON TYPE public.rv_pgrants_range TO "pgc_pg19_Pgrants_Reader";

-- ==== END fix:grants ====

-- ==== BEGIN fix:replident — REPLICA IDENTITY USING INDEX ====
-- pgc used to keep only relreplident, so it could not create, switch to, keep
-- or leave REPLICA IDENTITY USING INDEX. One table per transition; the TO side
-- is in schema_b.sql. Everything here runs on PG14+, except the NOT VALID
-- not-null of pg19_replident_notvalid (PG18+ syntax).
-- pg19_replident_enter: DEFAULT -> USING INDEX on a plain unique index.
CREATE TABLE test_schema.pg19_replident_enter (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_enter_code_uk ON test_schema.pg19_replident_enter (code);
-- pg19_replident_switch: USING INDEX a -> USING INDEX b (both indexes kept).
CREATE TABLE test_schema.pg19_replident_switch (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_switch_a_uk ON test_schema.pg19_replident_switch (a);
CREATE UNIQUE INDEX pg19_replident_switch_b_uk ON test_schema.pg19_replident_switch (b);
ALTER TABLE test_schema.pg19_replident_switch REPLICA IDENTITY USING INDEX pg19_replident_switch_a_uk;
-- pg19_replident_leave: USING INDEX -> DEFAULT while the key column loses NOT
-- NULL. DROP NOT NULL on a replica-identity key column fails, so the table has
-- to leave the index first.
CREATE TABLE test_schema.pg19_replident_leave (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_leave_code_uk ON test_schema.pg19_replident_leave (code);
ALTER TABLE test_schema.pg19_replident_leave REPLICA IDENTITY USING INDEX pg19_replident_leave_code_uk;
-- pg19_replident_full: USING INDEX -> FULL, and the index is dropped.
CREATE TABLE test_schema.pg19_replident_full (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_full_code_uk ON test_schema.pg19_replident_full (code);
ALTER TABLE test_schema.pg19_replident_full REPLICA IDENTITY USING INDEX pg19_replident_full_code_uk;
-- pg19_replident_rebuild: same index name, definition (a) -> (a, b). The
-- rebuilt index does not inherit indisreplident; USING INDEX is re-applied.
CREATE TABLE test_schema.pg19_replident_rebuild (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_rebuild_uk ON test_schema.pg19_replident_rebuild (a);
ALTER TABLE test_schema.pg19_replident_rebuild REPLICA IDENTITY USING INDEX pg19_replident_rebuild_uk;
-- pg19_replident_ukey: DEFAULT -> USING INDEX of a UNIQUE constraint.
CREATE TABLE test_schema.pg19_replident_ukey (id INT PRIMARY KEY, code INT NOT NULL CONSTRAINT pg19_replident_ukey_code_key UNIQUE);
-- pg19_replident_ukey_rebuild: USING INDEX of a UNIQUE constraint that is
-- dropped and re-added as UNIQUE (a, b).
CREATE TABLE test_schema.pg19_replident_ukey_rebuild (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, CONSTRAINT pg19_replident_ukey_rebuild_key UNIQUE (a));
ALTER TABLE test_schema.pg19_replident_ukey_rebuild REPLICA IDENTITY USING INDEX pg19_replident_ukey_rebuild_key;
-- pg19_replident_pkey: DEFAULT -> USING INDEX of the primary key.
CREATE TABLE test_schema.pg19_replident_pkey (id INT CONSTRAINT pg19_replident_pkey_pk PRIMARY KEY, code INT);
-- pg19_replident_orphan: the replica-identity index was dropped and created
-- again. relreplident stays 'i' with no indisreplident index, which the server
-- treats as NOTHING (pgc dumps it as NOTHING) -> USING INDEX in TO.
CREATE TABLE test_schema.pg19_replident_orphan (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_orphan_code_uk ON test_schema.pg19_replident_orphan (code);
ALTER TABLE test_schema.pg19_replident_orphan REPLICA IDENTITY USING INDEX pg19_replident_orphan_code_uk;
DROP INDEX test_schema.pg19_replident_orphan_code_uk;
CREATE UNIQUE INDEX pg19_replident_orphan_code_uk ON test_schema.pg19_replident_orphan (code);
-- pg19_replident_notvalid: NOT VALID not-null in FROM -> validated not-null +
-- USING INDEX in TO. PG19 (and 18.7+) reject USING INDEX while the key
-- column's not-null is NOT VALID, so it has to follow the constraint change.
-- The ALTER below fails harmlessly on PG14-17 (code stays nullable there, and
-- the migration sets it NOT NULL instead).
CREATE TABLE test_schema.pg19_replident_notvalid (id INT PRIMARY KEY, code INT);
ALTER TABLE test_schema.pg19_replident_notvalid ADD CONSTRAINT pg19_replident_notvalid_code_nn NOT NULL code NOT VALID;
CREATE UNIQUE INDEX pg19_replident_notvalid_code_uk ON test_schema.pg19_replident_notvalid (code);
-- pg19_replident_part: partitioned parent, DEFAULT -> USING INDEX of a new
-- unique index (built ON ONLY + per partition under --output-for-production).
CREATE TABLE test_schema.pg19_replident_part (id INT NOT NULL, k INT NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.pg19_replident_part_1 PARTITION OF test_schema.pg19_replident_part FOR VALUES FROM (0) TO (100);
-- pg19_replident_genrewrite: the key column becomes GENERATED ... STORED, which
-- pgc does as DROP COLUMN + ADD COLUMN. The drop takes the index with it, so
-- USING INDEX has to wait for the index to be re-created after the column.
CREATE TABLE test_schema.pg19_replident_genrewrite (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_genrewrite_code_uk ON test_schema.pg19_replident_genrewrite (code);
ALTER TABLE test_schema.pg19_replident_genrewrite REPLICA IDENTITY USING INDEX pg19_replident_genrewrite_code_uk;
-- pg19_replident_unrel: stays on USING INDEX while another column (g) becomes
-- GENERATED ... STORED, which pgc does as DROP COLUMN + ADD COLUMN. The index is
-- on code only, so nothing may take the table off it: under
-- --output-for-production the return would only come after COMMIT.
CREATE TABLE test_schema.pg19_replident_unrel (id INT PRIMARY KEY, code INT NOT NULL, g INT);
CREATE UNIQUE INDEX pg19_replident_unrel_code_uk ON test_schema.pg19_replident_unrel (code);
ALTER TABLE test_schema.pg19_replident_unrel REPLICA IDENTITY USING INDEX pg19_replident_unrel_code_uk;
-- pg19_replident_pp / pg19_replident_pkpart: partitions on USING INDEX of the
-- index PostgreSQL creates for them from a parent index (pp) or PRIMARY KEY
-- (pkpart) the migration adds, for an existing partition (_p1, _k1) and a new
-- one (_p2, _k2, in schema_b.sql). A new partition is created before its parent
-- is altered, so USING INDEX has to wait until the parent's statements have run.
CREATE TABLE test_schema.pg19_replident_pp (pg19_replident_ppk INT NOT NULL, v INT) PARTITION BY RANGE (pg19_replident_ppk);
CREATE TABLE test_schema.pg19_replident_pp_p1 PARTITION OF test_schema.pg19_replident_pp FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.pg19_replident_pkpart (id INT NOT NULL, v INT) PARTITION BY RANGE (id);
CREATE TABLE test_schema.pg19_replident_pkpart_k1 PARTITION OF test_schema.pg19_replident_pkpart FOR VALUES FROM (0) TO (100);
-- ==== END fix:replident ====

-- ==== BEGIN fix:rules — rule changes via CREATE OR REPLACE RULE ====

-- FROM side. Every rule here is on a relation named pg19_rules_* (or the
-- quoted "Pg19 Rules Mixed") in test_schema; see data/test/README.md,
-- "Rule Changes, Ordering and Dependencies".
CREATE TABLE test_schema.pg19_rules_src (
    id  integer PRIMARY KEY,
    val text,
    qty integer
);

CREATE TABLE test_schema.pg19_rules_log (
    id integer,
    op text
);

-- Changed: DO ALSO with one action -> DO INSTEAD with two. Applied with
-- CREATE OR REPLACE RULE; a plain CREATE RULE fails with "already exists".
CREATE RULE pg19_rules_chg AS ON INSERT TO test_schema.pg19_rules_src
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op) VALUES (NEW.id, 'v1');

-- Changed event (UPDATE -> DELETE), qualification and comment, on a quoted
-- rule and table name.
CREATE TABLE test_schema."Pg19 Rules Mixed" (
    id   integer,
    flag boolean
);
CREATE RULE "Pg19 Rules Where" AS ON UPDATE TO test_schema."Pg19 Rules Mixed"
    WHERE OLD.flag DO INSTEAD NOTHING;
COMMENT ON RULE "Pg19 Rules Where" ON test_schema."Pg19 Rules Mixed" IS 'rule comment (from)';

-- Removed: a quoted rule name, which the drop must not quote a second time.
CREATE RULE "Pg19 Rules Gone" AS ON DELETE TO test_schema."Pg19 Rules Mixed"
    DO INSTEAD NOTHING;

-- Removed together with the table its action writes to: DROP TABLE runs
-- without CASCADE, so the rule has to be gone before the table is dropped.
CREATE TABLE test_schema.pg19_rules_old_audit (
    id integer
);
CREATE RULE pg19_rules_to_old_audit AS ON DELETE TO test_schema.pg19_rules_src
    DO ALSO INSERT INTO test_schema.pg19_rules_old_audit (id) VALUES (OLD.id);

-- Unchanged rule reading a column whose type changes (qty integer ->
-- bigint in TO): ALTER COLUMN TYPE is refused while a rule depends on the
-- column, so the rule is dropped first and recreated afterwards.
CREATE RULE pg19_rules_qty AS ON UPDATE TO test_schema.pg19_rules_src
    WHERE NEW.qty < 0 DO INSTEAD NOTHING;

-- Unchanged rule on a view that is dropped and recreated (TO drops a
-- column, which CREATE OR REPLACE VIEW cannot do): the rule goes with the
-- view and has to be recreated after it.
CREATE TABLE test_schema.pg19_rules_vbase (
    id    integer PRIMARY KEY,
    name  text,
    extra text
);
CREATE VIEW test_schema.pg19_rules_view AS
SELECT id, name, extra FROM test_schema.pg19_rules_vbase;
CREATE RULE pg19_rules_view_ins AS ON INSERT TO test_schema.pg19_rules_view
    DO INSTEAD INSERT INTO test_schema.pg19_rules_vbase (id, name) VALUES (NEW.id, NEW.name);

-- Unchanged rule on a table recreated by a partition-key change (RANGE in
-- FROM, LIST in TO).
CREATE TABLE test_schema.pg19_rules_part (
    id integer,
    k  integer
) PARTITION BY RANGE (k);
CREATE RULE pg19_rules_part_del AS ON DELETE TO test_schema.pg19_rules_part
    DO INSTEAD NOTHING;

-- Unchanged rule calling a function whose return type changes (integer ->
-- bigint): DROP FUNCTION ... CASCADE takes the rule with it.
CREATE FUNCTION test_schema.pg19_rules_tag(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT p * 10 $$;
CREATE RULE pg19_rules_tagged AS ON UPDATE TO test_schema.pg19_rules_log
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    VALUES (test_schema.pg19_rules_tag(NEW.id), 'tagged');

-- Unchanged view that only filters on that function, carrying an unchanged
-- rule: the CASCADE drops the view and its rule, the routine-dependents pass
-- recreates the view, and the rule has to follow it.
CREATE VIEW test_schema.pg19_rules_tag_view AS
SELECT id, op FROM test_schema.pg19_rules_log
WHERE test_schema.pg19_rules_tag(id) > 0;
CREATE RULE pg19_rules_tag_view_del AS ON DELETE TO test_schema.pg19_rules_tag_view
    DO INSTEAD DELETE FROM test_schema.pg19_rules_log WHERE pg19_rules_log.id = OLD.id;

-- A text overload of pg19_rules_tag, unchanged in TO, and a view calling it
-- with a rule whose comment TO removes. The routine-dependents pass flags
-- the view by name, so the rule is recreated with CREATE OR REPLACE RULE
-- although the integer overload's CASCADE never touched it: the FROM comment
-- is still there and has to be cleared.
CREATE FUNCTION test_schema.pg19_rules_tag(p text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT p || '!' $$;
CREATE VIEW test_schema.pg19_rules_tag_txt_view AS
SELECT id, test_schema.pg19_rules_tag(op) AS tagged FROM test_schema.pg19_rules_log;
CREATE RULE pg19_rules_tag_txt_del AS ON DELETE TO test_schema.pg19_rules_tag_txt_view
    DO INSTEAD DELETE FROM test_schema.pg19_rules_log WHERE pg19_rules_log.id = OLD.id;
COMMENT ON RULE pg19_rules_tag_txt_del ON test_schema.pg19_rules_tag_txt_view IS 'removed in TO';

-- Unchanged rule on another table that reads pg19_rules_tag_view: the
-- function's CASCADE drops the view and, through it, this rule.
CREATE TABLE test_schema.pg19_rules_reader (
    id integer
);
CREATE RULE pg19_rules_reads_tag_view AS ON INSERT TO test_schema.pg19_rules_reader
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    SELECT id, 'via view' FROM test_schema.pg19_rules_tag_view;

-- A table that TO turns into a view of the same name, with the same
-- NOTIFY rule on both. DROP TABLE takes the rule, and a NOTIFY-only rule has
-- no pg_depend dependencies to go by.
CREATE TABLE test_schema.pg19_rules_tv (
    id integer
);
CREATE RULE pg19_rules_tv_notify AS ON INSERT TO test_schema.pg19_rules_tv
    DO ALSO NOTIFY pg19_rules_chan;

-- A FROM-only foreign table read by a rule TO keeps (in another form):
-- DROP FOREIGN TABLE runs without CASCADE. And an unchanged rule reading a
-- foreign-table column TO retypes (integer -> bigint).
CREATE FOREIGN TABLE test_schema.pg19_rules_ft (
    id integer
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'pg19_rules_ft');
CREATE RULE pg19_rules_ft_read AS ON UPDATE TO test_schema.pg19_rules_reader
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    SELECT id, 'ft' FROM test_schema.pg19_rules_ft;
CREATE FOREIGN TABLE test_schema.pg19_rules_ft2 (
    id integer,
    v  integer
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'pg19_rules_ft2');
CREATE RULE pg19_rules_ft2_read AS ON DELETE TO test_schema.pg19_rules_reader
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    SELECT v, 'ft2' FROM test_schema.pg19_rules_ft2;

-- PG19 (FROM side is valid everywhere): ON CONFLICT DO NOTHING here, DO
-- SELECT in TO. On PG14-18 the TO rule fails to create, so the diff drops it.
CREATE TABLE test_schema.pg19_rules_upsert_target (
    id  integer PRIMARY KEY,
    val text
);
CREATE TABLE test_schema.pg19_rules_upsert_front (
    id  integer PRIMARY KEY,
    val text
);
CREATE RULE pg19_rules_upsert AS ON INSERT TO test_schema.pg19_rules_upsert_front
    DO INSTEAD INSERT INTO test_schema.pg19_rules_upsert_target (id, val)
    VALUES (NEW.id, NEW.val) ON CONFLICT (id) DO NOTHING;

-- ==== END fix:rules ====

-- ==== BEGIN fix:serial — serial detection for non-integer nextval columns ====
-- FROM side: two tables that already exist and pick up an owned sequence in
-- TO (a new serial column, and an existing column that becomes serial), and
-- one whose extra owned sequence TO drops. The other pg19_serial_* objects
-- exist only in TO; see schema_b.sql.
CREATE TABLE test_schema.pg19_serial_add (
    label text
);
CREATE TABLE test_schema.pg19_serial_alter (
    id    integer NOT NULL,
    label text
);
-- identity column that also owns an ordinary sequence, which TO no longer
-- has: that sequence is dropped, the identity sequence stays
CREATE TABLE test_schema.pg19_serial_ident_drop (
    id    integer GENERATED ALWAYS AS IDENTITY,
    label text
);
CREATE SEQUENCE test_schema.pg19_serial_ident_drop_extra
    OWNED BY test_schema.pg19_serial_ident_drop.id;

-- ==== END fix:serial ====

-- ==== BEGIN fix:comments — names containing newlines or comment delimiters in generated comments ====
-- FROM side. An identifier may hold `*/` and `/*`, and pgc names every object
-- it touches in a block-comment header (`/* Table: s.t*/`, `/* View: ...*/`).
-- Written verbatim, the `*/` in the first table's name closes its header
-- early and the rest of the name runs as SQL, dropping pg19_comments_victim;
-- a `/*` opens a nested comment that swallows the rest of the migration. The
-- comment stripper behind USE_COMMENTS=false lexes the same way, so both
-- would reach the script CI applies. Names with a line break, which ends a
-- `--` comment, are covered by unit tests and a live check instead: with
-- USE_DROP=true every `--` note that names an object reports a change pgc
-- cannot make (an enum label removed, an extension owner changed), so such a
-- fixture could never give an empty second round.
CREATE TABLE public.pg19_comments_victim (id integer PRIMARY KEY);
INSERT INTO public.pg19_comments_victim VALUES (1);

-- altered in TO: one column dropped, one added
CREATE TABLE test_schema."pg19_comments_t*/drop table public.pg19_comments_victim;/*" (
    id                  integer PRIMARY KEY,
    "pg19_comments_c/*" integer,
    gone                integer
);
-- recreated in TO: its column list shrinks
CREATE VIEW test_schema."pg19_comments_v/*" AS SELECT 1 AS a, 2 AS b;
-- replaced in TO: its return type changes
CREATE FUNCTION test_schema."pg19_comments_f*/"(a integer) RETURNS integer
    LANGUAGE sql AS 'SELECT a';
-- dropped in TO
CREATE TABLE test_schema."pg19_comments_d*/" (id integer);
CREATE SEQUENCE test_schema."pg19_comments_s/*";

-- Names in statements, not only in comments (all versions).
-- Materialized views are named through quote_ident like every other object;
-- pgc read their schema, name and owner raw, so this one came out as
-- test_schema.pg19_comments_Mv*/, which does not even parse. Recreated in TO:
-- a column is added.
CREATE MATERIALIZED VIEW test_schema."pg19_comments_Mv*/" AS SELECT 1 AS a;
CREATE INDEX "pg19_comments_Mv_a" ON test_schema."pg19_comments_Mv*/" (a);
-- The guarded REPLICA IDENTITY of a partition whose index comes from its
-- parent is a DO block; its dollar tag must not occur in the names inside.
-- TO adds the parent index and a partition named with $pgc$.
CREATE TABLE test_schema.pg19_comments_rp (pg19_comments_rpk integer NOT NULL, v integer)
    PARTITION BY RANGE (pg19_comments_rpk);
-- A foreign table whose table_name changes and which gains an option: a bare
-- OPTIONS item means ADD, which fails for a key the table already has.
CREATE FOREIGN TABLE test_schema.pg19_comments_ft (id integer)
    SERVER test_foreign_server OPTIONS (schema_name 'public', table_name 'a');

-- ==== END fix:comments ====
