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

