-- Schema B: Modified PostgreSQL schema for testing database comparison
-- This schema represents the "TO" database in comparisons
-- Contains differences to test all comparison scenarios

-- Create schemas (one removed, one added)
CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE SCHEMA IF NOT EXISTS shared_schema;
CREATE SCHEMA IF NOT EXISTS new_reporting_schema;  -- NEW SCHEMA

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

-- Extensions (modified list)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
-- pgcrypto removed
CREATE EXTENSION IF NOT EXISTS "pg_trgm" WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS "hstore" WITH SCHEMA public;  -- NEW EXTENSION
-- Issue #241: same extension as FROM, different schema (shared_schema ->
-- public). Expect one `alter extension fuzzystrmatch set schema public;`.
CREATE EXTENSION IF NOT EXISTS "fuzzystrmatch" WITH SCHEMA public;

-- Custom types (some modified, some removed, some added)
CREATE TYPE test_schema.status_type AS ENUM ('active', 'inactive', 'pending', 'suspended');  -- MODIFIED: added 'suspended'
-- priority_type removed (FROM has routines depending on it; TO intentionally omits both type and those routines)
CREATE TYPE shared_schema.address_type AS (
    street VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    region VARCHAR(100)  -- MODIFIED: added region field
);

-- Modified composite type
CREATE TYPE test_schema.user_profile AS (
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    birth_date DATE,
    phone VARCHAR(20)  -- MODIFIED: added phone field
);

-- Composite type migration case (TO only): should be created when FROM lacks it
CREATE TYPE test_schema.test_type_B AS (
    street VARCHAR(255),
    city VARCHAR(100)
);

-- New type
CREATE TYPE test_schema.contact_type AS ENUM ('email', 'phone', 'sms', 'mail');

-- Domain type (modified)
CREATE DOMAIN test_schema.positive_integer AS INTEGER CHECK (VALUE > 0 AND VALUE <= 1000000);  -- MODIFIED: added upper limit

-- Sequences (some modified, some removed, some added)
CREATE SEQUENCE test_schema.user_id_seq
    START WITH 2000  -- MODIFIED: different start value
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 10;

-- order_id_seq removed

CREATE SEQUENCE shared_schema.global_counter_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 999999999
    CACHE 5;  -- MODIFIED: different cache size

-- MODIFIED: start_value and MINVALUE raised from 1 to 10000000.
-- The comparer must emit RESTART WITH 10000000 so PostgreSQL does not fall back to
-- the old recorded start value (1) and raise:
--   ERROR: RESTART value (1) cannot be less than MINVALUE (10000000)
CREATE SEQUENCE test_schema.minvalue_raise_seq
    START WITH 10000000  -- MODIFIED: raised from 1
    INCREMENT BY 1
    MINVALUE 10000000    -- MODIFIED: raised from 1
    MAXVALUE 999999999
    CACHE 1
    CYCLE;               -- MODIFIED: NO CYCLE → CYCLE

-- New sequence
CREATE SEQUENCE new_reporting_schema.report_id_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

-- Tables (some modified, some removed, some added)
CREATE TABLE test_schema.users (
    id INTEGER PRIMARY KEY DEFAULT nextval('test_schema.user_id_seq'),
    username VARCHAR(60) UNIQUE NOT NULL,  -- MODIFIED: increased length
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
    last_login TIMESTAMP WITH TIME ZONE,
    -- NEW COLUMNS
    preferred_contact test_schema.contact_type DEFAULT 'email',
    timezone VARCHAR(50) DEFAULT 'UTC',
    two_factor_enabled BOOLEAN DEFAULT FALSE
    -- last_login removed in modified version
);

CREATE TABLE test_schema.categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    parent_id INTEGER REFERENCES test_schema.categories(id),
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- NEW COLUMNS
    category_code VARCHAR(20) UNIQUE,
    icon_url TEXT
);

-- products table modified significantly
CREATE TABLE test_schema.products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(300) NOT NULL,  -- MODIFIED: increased length
    description TEXT,
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),  -- MODIFIED: increased precision
    category_id INTEGER NOT NULL REFERENCES test_schema.categories(id) ON DELETE CASCADE,
    sku VARCHAR(100) UNIQUE,
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    -- weight removed
    dimensions JSONB,
    tags TEXT[],
    status test_schema.status_type DEFAULT 'active',
    -- priority removed (type doesn't exist anymore)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER REFERENCES test_schema.users(id),
    -- NEW COLUMNS
    barcode VARCHAR(50),
    manufacturer VARCHAR(100),
    warranty_months INTEGER CHECK (warranty_months >= 0),
    is_featured BOOLEAN DEFAULT FALSE
);

-- orders table removed completely

-- order_items table removed completely

-- New table
CREATE TABLE test_schema.reviews (
    id SERIAL PRIMARY KEY,
    product_id UUID NOT NULL REFERENCES test_schema.products(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES test_schema.users(id),
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    title VARCHAR(255),
    content TEXT,
    is_verified BOOLEAN DEFAULT FALSE,
    helpful_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Generated column change test (TO: generated stored)
CREATE TABLE test_schema.generated_pricing (
    id SERIAL PRIMARY KEY,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
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
    user_agent TEXT,
    -- NEW COLUMNS
    session_id VARCHAR(255),
    request_id UUID
);

-- New table in new schema
CREATE TABLE new_reporting_schema.daily_stats (
    id INTEGER PRIMARY KEY DEFAULT nextval('new_reporting_schema.report_id_seq'),
    report_date DATE NOT NULL UNIQUE,
    total_users INTEGER DEFAULT 0,
    total_products INTEGER DEFAULT 0,
    total_reviews INTEGER DEFAULT 0,
    avg_rating DECIMAL(3,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes (some modified, some removed, some added)
CREATE INDEX idx_users_username ON test_schema.users(username);
CREATE INDEX idx_users_email ON test_schema.users(email);
CREATE INDEX idx_users_status ON test_schema.users(status);
CREATE INDEX idx_users_created_at ON test_schema.users(created_at);
CREATE INDEX idx_users_metadata_gin ON test_schema.users USING GIN(metadata);
-- NEW INDEX
CREATE INDEX idx_users_preferred_contact ON test_schema.users(preferred_contact);
CREATE INDEX idx_users_timezone ON test_schema.users(timezone);

CREATE INDEX idx_products_name ON test_schema.products(name);
CREATE INDEX idx_products_category_id ON test_schema.products(category_id);
CREATE INDEX idx_products_sku ON test_schema.products(sku);
CREATE INDEX idx_products_status ON test_schema.products(status);
CREATE INDEX idx_products_price ON test_schema.products(price);
CREATE INDEX idx_products_tags_gin ON test_schema.products USING GIN(tags);
CREATE INDEX idx_products_dimensions_gin ON test_schema.products USING GIN(dimensions);
-- NEW INDEXES
CREATE INDEX idx_products_manufacturer ON test_schema.products(manufacturer);
CREATE INDEX idx_products_is_featured ON test_schema.products(is_featured);
CREATE INDEX idx_products_barcode ON test_schema.products(barcode);

-- NEW INDEXES for reviews table
CREATE INDEX idx_reviews_product_id ON test_schema.reviews(product_id);
CREATE INDEX idx_reviews_user_id ON test_schema.reviews(user_id);
CREATE INDEX idx_reviews_rating ON test_schema.reviews(rating);
CREATE INDEX idx_reviews_created_at ON test_schema.reviews(created_at);
CREATE INDEX idx_reviews_is_verified ON test_schema.reviews(is_verified);

CREATE INDEX idx_audit_logs_table_name ON shared_schema.audit_logs(table_name);
CREATE INDEX idx_audit_logs_operation ON shared_schema.audit_logs(operation);
CREATE INDEX idx_audit_logs_changed_at ON shared_schema.audit_logs(changed_at);
CREATE INDEX idx_audit_logs_changed_by ON shared_schema.audit_logs(changed_by);
-- NEW INDEXES
CREATE INDEX idx_audit_logs_session_id ON shared_schema.audit_logs(session_id);
CREATE INDEX idx_audit_logs_request_id ON shared_schema.audit_logs(request_id);
-- Standalone unique index (TO) with different definition to ensure diff detection
CREATE UNIQUE INDEX idx_audit_logs_table_op_changed_at
    ON shared_schema.audit_logs(table_name, operation, changed_at, record_id);

-- Functions and procedures (some modified, some removed, some added)
CREATE OR REPLACE FUNCTION test_schema.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- calculate_order_total function removed (table doesn't exist)

-- get_user_order_count function removed (table doesn't exist)

-- NEW FUNCTION
CREATE OR REPLACE FUNCTION test_schema.calculate_average_rating(product_id_param UUID)
RETURNS DECIMAL(3,2) AS $$
DECLARE
    avg_rating DECIMAL(3,2);
BEGIN
    SELECT ROUND(AVG(rating)::NUMERIC, 2)
    INTO avg_rating
    FROM test_schema.reviews
    WHERE product_id = product_id_param;
    
    RETURN COALESCE(avg_rating, 0);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION shared_schema.generate_order_number()
RETURNS VARCHAR(50) AS $$
DECLARE
    seq_val BIGINT;
    order_num VARCHAR(50);
BEGIN
    seq_val := nextval('shared_schema.global_counter_seq');
    order_num := 'REF-' || LPAD(seq_val::TEXT, 8, '0');  -- MODIFIED: different prefix
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

-- Trigger ordering scenario (TO): modified function and trigger signature
CREATE OR REPLACE FUNCTION test_schema.fn_order_from()
RETURNS TRIGGER AS $$
BEGIN
    NEW.description := COALESCE(NEW.description, 'to');
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE test_schema.trigger_order_test (
    id SERIAL PRIMARY KEY,
    description TEXT,
    updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TRIGGER trg_order_from
BEFORE INSERT ON test_schema.trigger_order_test
FOR EACH ROW EXECUTE FUNCTION test_schema.fn_order_from();

-- Drop-order dependency scenario intentionally removed in TO to test drop ordering
-- (drop_status type, drop_parent partitioned table, drop_child partition, drop_orders FK, trg_drop_orders trigger)

-- Partition type-change scenario (TO): partition key switches to UUID to force recreate
CREATE TABLE test_schema.part_type_change_parent (
    tenant UUID NOT NULL,
    id INTEGER NOT NULL,
    note TEXT,
    PRIMARY KEY (tenant, id)
) PARTITION BY LIST (tenant);

CREATE TABLE test_schema.part_type_change_child PARTITION OF test_schema.part_type_change_parent
FOR VALUES IN ('00000000-0000-0000-0000-000000000000');

-- NEW FUNCTION
CREATE OR REPLACE FUNCTION test_schema.get_user_review_count(user_id_param INTEGER)
RETURNS INTEGER AS $$
DECLARE
    review_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO review_count
    FROM test_schema.reviews
    WHERE user_id = user_id_param;
    
    RETURN review_count;
END;
$$ LANGUAGE plpgsql;

-- NEW FUNCTION
CREATE OR REPLACE FUNCTION new_reporting_schema.update_daily_stats(report_date_param DATE DEFAULT CURRENT_DATE)
RETURNS VOID AS $$
DECLARE
    user_count INTEGER;
    product_count INTEGER;
    review_count INTEGER;
    avg_rating DECIMAL(3,2);
BEGIN
    SELECT COUNT(*) INTO user_count FROM test_schema.users;
    SELECT COUNT(*) INTO product_count FROM test_schema.products;
    SELECT COUNT(*) INTO review_count FROM test_schema.reviews;
    SELECT ROUND(AVG(rating)::NUMERIC, 2) INTO avg_rating FROM test_schema.reviews;
    
    INSERT INTO new_reporting_schema.daily_stats 
        (report_date, total_users, total_products, total_reviews, avg_rating)
    VALUES 
        (report_date_param, user_count, product_count, review_count, avg_rating)
    ON CONFLICT (report_date) 
    DO UPDATE SET
        total_users = EXCLUDED.total_users,
        total_products = EXCLUDED.total_products,
        total_reviews = EXCLUDED.total_reviews,
        avg_rating = EXCLUDED.avg_rating,
        created_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_schema.get_users_by_status(p_status test_schema.status_type)
RETURNS TABLE(user_id integer, username varchar, email varchar, created_at timestamp with time zone)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY SELECT id, username, email, created_at FROM test_schema.users WHERE status = p_status;
END;
$$;

-- get_products_by_priority(test_schema.priority_type) intentionally removed with priority_type

-- Function containing nested $$ to exercise custom dollar quoting (TO database variant of fn_dollar_from)
CREATE OR REPLACE FUNCTION test_schema.fn_dollar_from()
RETURNS text
LANGUAGE plpgsql
AS $c$
DECLARE
    inner_text text := $$inner-to$$;
BEGIN
    RETURN inner_text || '-v2';
END;
$c$;

-- cleanup_old_orders procedure removed (table doesn't exist)

-- NEW PROCEDURE
CREATE OR REPLACE PROCEDURE test_schema.cleanup_old_reviews(days_old INTEGER DEFAULT 730)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM test_schema.reviews
    WHERE created_at < CURRENT_DATE - INTERVAL '1 day' * days_old
    AND helpful_count = 0;

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

-- Triggers (some modified, some removed, some added)
CREATE TRIGGER trigger_users_update_timestamp
    BEFORE UPDATE ON test_schema.users
    FOR EACH ROW
    EXECUTE FUNCTION test_schema.update_timestamp();

CREATE TRIGGER trigger_products_update_timestamp
    BEFORE UPDATE ON test_schema.products
    FOR EACH ROW
    EXECUTE FUNCTION test_schema.update_timestamp();

-- NEW TRIGGER
CREATE TRIGGER trigger_reviews_update_timestamp
    BEFORE UPDATE ON test_schema.reviews
    FOR EACH ROW
    EXECUTE FUNCTION test_schema.update_timestamp();

CREATE TRIGGER trigger_users_audit
    AFTER INSERT OR UPDATE OR DELETE ON test_schema.users
    FOR EACH ROW
    EXECUTE FUNCTION shared_schema.audit_trigger();

-- trigger_orders_audit removed (table doesn't exist)

-- NEW TRIGGER
CREATE TRIGGER trigger_reviews_audit
    AFTER INSERT OR UPDATE OR DELETE ON test_schema.reviews
    FOR EACH ROW
    EXECUTE FUNCTION shared_schema.audit_trigger();

-- Views (some modified, some removed, some added)
-- user_order_summary removed (orders table doesn't exist)

-- MATERIALIZED VIEWS
-- Case 1 from_only_mat: removed (exists only in FROM → should be dropped)

-- Case 2: exists in both, definition changed → should be dropped and recreated
CREATE MATERIALIZED VIEW test_schema.active_users_mat AS
SELECT
    u.id,
    u.username,
    u.email,
    u.status  -- MODIFIED: added status column
FROM test_schema.users u
WHERE u.status = 'active';

-- Case 3: unchanged → no change expected
CREATE MATERIALIZED VIEW test_schema.user_count_mat AS
SELECT
    status,
    COUNT(*) AS cnt
FROM test_schema.users
GROUP BY status;

-- Case 4: exists only in TO → should be created
CREATE MATERIALIZED VIEW test_schema.product_stock_mat AS
SELECT
    p.id,
    p.name,
    p.sku,
    p.stock_quantity,
    p.status
FROM test_schema.products p
WHERE p.stock_quantity > 0;

-- MODIFIED VIEW
CREATE VIEW test_schema.product_inventory AS
SELECT 
    p.id,
    p.name,
    p.sku,
    p.price,
    p.stock_quantity,
    c.name as category_name,
    p.status,
    p.manufacturer,  -- NEW COLUMN
    p.is_featured,   -- NEW COLUMN
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity <= 5 THEN 'Low Stock'  -- MODIFIED: changed threshold
        ELSE 'In Stock'
    END as inventory_status
FROM test_schema.products p
JOIN test_schema.categories c ON p.category_id = c.id;

-- NEW VIEW
CREATE VIEW test_schema.user_review_summary AS
SELECT 
    u.id,
    u.username,
    u.email,
    u.status as user_status,
    COUNT(r.id) as total_reviews,
    ROUND(AVG(r.rating)::NUMERIC, 2) as avg_rating_given,
    MAX(r.created_at) as last_review_date
FROM test_schema.users u
LEFT JOIN test_schema.reviews r ON u.id = r.user_id
GROUP BY u.id, u.username, u.email, u.status;

-- NEW VIEW
CREATE VIEW test_schema.product_review_stats AS
SELECT 
    p.id,
    p.name,
    p.sku,
    COUNT(r.id) as review_count,
    ROUND(AVG(r.rating)::NUMERIC, 2) as avg_rating,
    COUNT(CASE WHEN r.is_verified THEN 1 END) as verified_reviews
FROM test_schema.products p
LEFT JOIN test_schema.reviews r ON p.id = r.product_id
GROUP BY p.id, p.name, p.sku;

-- Constraints (some modified, some removed, some added)
ALTER TABLE test_schema.users ADD CONSTRAINT chk_users_email_format 
    CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

-- chk_products_weight_positive removed (weight column doesn't exist)

-- NEW CONSTRAINTS
ALTER TABLE test_schema.products ADD CONSTRAINT chk_products_warranty_reasonable 
    CHECK (warranty_months IS NULL OR warranty_months <= 120);

ALTER TABLE test_schema.reviews ADD CONSTRAINT chk_reviews_content_not_empty 
    CHECK (content IS NULL OR LENGTH(TRIM(content)) > 0);

ALTER TABLE test_schema.reviews ADD CONSTRAINT chk_reviews_helpful_count_positive 
    CHECK (helpful_count >= 0);

-- Create some sample data
INSERT INTO test_schema.categories (name, description, category_code) VALUES 
('Electronics', 'Electronic devices and gadgets', 'ELEC'),
('Books', 'Physical and digital books', 'BOOK'),
('Clothing', 'Apparel and accessories', 'CLTH'),
('Home & Garden', 'Home improvement and garden items', 'HOME'),
('Sports', 'Sports equipment and accessories', 'SPRT');  -- NEW CATEGORY

-- Comments (some modified, some added)
COMMENT ON SCHEMA test_schema IS 'Main application schema for testing - updated version';
COMMENT ON SCHEMA new_reporting_schema IS 'Schema for reporting and analytics';
COMMENT ON TABLE test_schema.users IS 'User accounts and profiles with enhanced features';
COMMENT ON TABLE test_schema.products IS 'Product catalog with manufacturer details';
COMMENT ON TABLE test_schema.reviews IS 'Product reviews and ratings from users';
COMMENT ON COLUMN test_schema.users.metadata IS 'Additional user data in JSON format';
COMMENT ON COLUMN test_schema.products.dimensions IS 'Product dimensions (length, width, height) in JSON';
COMMENT ON COLUMN test_schema.users.preferred_contact IS 'Preferred method of contact for notifications';
COMMENT ON COLUMN test_schema.products.barcode IS 'Product barcode for inventory tracking';
COMMENT ON TYPE test_schema.status_type IS 'User status values including suspended (TO)';
COMMENT ON DOMAIN test_schema.positive_integer IS 'Positive integer capped at 1,000,000 (TO)';
COMMENT ON SEQUENCE test_schema.user_id_seq IS 'User id sequence starting at 2000 (TO)';
COMMENT ON MATERIALIZED VIEW test_schema.active_users_mat IS 'Active users snapshot (TO version)';
COMMENT ON VIEW test_schema.product_inventory IS 'Inventory overview with featured flag (TO)';
COMMENT ON FUNCTION test_schema.get_users_by_status(test_schema.status_type) IS 'Returns users with created_at for status filter (TO)';

-- Special characters test (Modified: added column)
CREATE TABLE test_schema."special$table" (
    "id" SERIAL PRIMARY KEY,
    "user@name" VARCHAR(50),
    "e-mail" VARCHAR(100),
    "new#col" INT
);

-- table with spaces (Modified: changed type)
CREATE TABLE test_schema."table with spaces" (
    "id" SERIAL PRIMARY KEY,
    "column with spaces" VARCHAR(255)
);

-- Ensure roles exist for row-level security policies in TO schema
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tenant_reader') THEN
        CREATE ROLE tenant_reader;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tenant_editor') THEN
        CREATE ROLE tenant_editor;
    END IF;
END$$;

-- Row-level security (TO: restrictive policy with different predicate and an extra update policy)
ALTER TABLE test_schema.users ENABLE ROW LEVEL SECURITY;
CREATE POLICY users_rls_select ON test_schema.users
    AS RESTRICTIVE
    FOR SELECT
    TO "tenant_reader"
    USING ((metadata ->> 'tenant_id') = current_setting('app.current_tenant') AND two_factor_enabled = TRUE);

CREATE POLICY users_rls_update ON test_schema.users
    FOR UPDATE
    TO "tenant_editor"
    USING ((metadata ->> 'tenant_id') = current_setting('app.current_tenant'))
    WITH CHECK ((metadata ->> 'tenant_id') = current_setting('app.current_tenant') AND preferred_contact <> 'sms');

-- Complex Foreign Keys test (Modified: ON DELETE CASCADE)
CREATE TABLE test_schema.composite_pk (
    part_one INT,
    part_two INT,
    data TEXT,
    PRIMARY KEY (part_one, part_two)
);

-- Function argument change test (Modified: added currency argument)
CREATE OR REPLACE FUNCTION test_schema.calculate_tax(price numeric, tax_rate numeric, currency varchar)
RETURNS numeric
LANGUAGE plpgsql
AS $$
BEGIN
    -- Just a dummy implementation using the new argument
    IF currency = 'USD' THEN
        RETURN price * tax_rate;
    ELSE
        RETURN price * tax_rate * 1.1;
    END IF;
END;
$$;

-- =============================================================================
-- Routine flags & special routine kinds test
-- =============================================================================

-- VOLATILE PARALLEL UNSAFE function (TO)
-- FROM had STABLE STRICT PARALLEL SAFE → flag-only diff
CREATE OR REPLACE FUNCTION test_schema.lookup_username(p_id integer)
RETURNS varchar
LANGUAGE sql
VOLATILE PARALLEL UNSAFE
AS $$
    SELECT username FROM test_schema.users WHERE id = p_id;
$$;

-- normalize_email_from removed (FROM-only → should be dropped)

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

-- SECURITY INVOKER procedure (TO — default, no SECURITY DEFINER)
-- FROM had SECURITY DEFINER → flag-only diff
CREATE OR REPLACE PROCEDURE test_schema.admin_reset_counters()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE test_schema.users SET login_count = 1;
END;
$$;

-- STABLE STRICT PARALLEL RESTRICTED function (TO)
-- FROM had IMMUTABLE STRICT PARALLEL SAFE → volatility & parallel change
CREATE OR REPLACE FUNCTION test_schema.tax_label(rate numeric)
RETURNS text
LANGUAGE sql
STABLE STRICT PARALLEL RESTRICTED
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

-- Function with SET params (TO)
-- FROM had only SET search_path = 'public' → config modified + added
CREATE OR REPLACE FUNCTION test_schema.secure_lookup(key text)
RETURNS text
LANGUAGE plpgsql
STABLE SECURITY DEFINER
SET search_path = 'public, pg_temp'
SET lock_timeout = '5s'
AS $$
BEGIN
    RETURN current_setting(key, true);
END;
$$;

-- Procedure with SET params (TO only → should be created with SET clauses)
CREATE OR REPLACE PROCEDURE test_schema.apply_secure_settings(IN pvalue text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = 'public, pg_temp'
SET lock_timeout = '5s'
AS $$
BEGIN
    RAISE NOTICE 'value: %', pvalue;
END;
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

-- concat_agg removed (FROM-only → should be dropped)
-- concat_agg_sfunc also removed (FROM-only)

-- AGGREGATE function (TO only → should be created)
CREATE OR REPLACE FUNCTION test_schema.product_agg_sfunc(state numeric, val numeric, weight numeric)
RETURNS numeric
LANGUAGE sql
IMMUTABLE STRICT
AS $$
    SELECT state + val * weight;
$$;

CREATE AGGREGATE test_schema.weighted_sum(numeric, numeric) (
    SFUNC = test_schema.product_agg_sfunc,
    STYPE = numeric,
    INITCOND = '0'
);
COMMENT ON AGGREGATE test_schema.weighted_sum(numeric, numeric) IS 'Weighted sum aggregate (TO only)';

-- IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE function (TO only → should be created with all flags)
CREATE OR REPLACE FUNCTION test_schema.normalize_email_to(raw text)
RETURNS text
LANGUAGE sql
IMMUTABLE STRICT LEAKPROOF PARALLEL SAFE
AS $$
    SELECT lower(trim(raw));
$$;

CREATE TABLE test_schema.composite_fk (
    id SERIAL PRIMARY KEY,
    ref_part_one INT,
    ref_part_two INT,
    CONSTRAINT fk_composite FOREIGN KEY (ref_part_one, ref_part_two) 
        REFERENCES test_schema.composite_pk (part_one, part_two) ON DELETE CASCADE
);

-- Named primary key fixture (TO side): table exists only in TO.
CREATE TABLE test_schema.table1 (
    id int4 NOT NULL,
    CONSTRAINT pk_table1_id PRIMARY KEY (id)
);

-- Identity column test (New table in TO schema)
CREATE SCHEMA IF NOT EXISTS data;

CREATE TABLE data.test (
    id int8 GENERATED ALWAYS AS IDENTITY (
        INCREMENT BY 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        START 1
        CACHE 1
        NO CYCLE
    ) NOT NULL,
    CONSTRAINT test_pkey PRIMARY KEY (id)
);

-- Identity column update test (TO)
CREATE TABLE test_schema.identity_update_test (
    id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 5),
    payload TEXT
);

-- Issue reproduction: Sequences created automatically for serial/identity columns
CREATE TABLE test_schema.issue_serial_test (
    id BIGSERIAL PRIMARY KEY,
    data TEXT
);

CREATE TABLE test_schema.issue_identity_always_test (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data TEXT
);

CREATE TABLE test_schema.issue_identity_default_test (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    data TEXT
);

-- Multi-level partition test (TO: 3-level, events_2023 is now a sub-partition parent by LIST(region))
CREATE TABLE data.events (
    id        BIGINT NOT NULL,
    year      INT    NOT NULL,
    region    TEXT   NOT NULL,
    payload   TEXT,
    CONSTRAINT events_pkey PRIMARY KEY (id, year, region)
) PARTITION BY RANGE (year);

-- events_2023 changes from a leaf to a sub-partition parent (partition_key change → recreate)
CREATE TABLE data.events_2023
    PARTITION OF data.events
    FOR VALUES FROM (2023) TO (2024)
    PARTITION BY LIST (region);

CREATE TABLE data.events_2023_us
    PARTITION OF data.events_2023
    FOR VALUES IN ('us');

CREATE TABLE data.events_2023_eu
    PARTITION OF data.events_2023
    FOR VALUES IN ('eu');

-- sensor_data hierarchy removed in TO (tests deep drop ordering: leaf → mid → parent)

-- New 3-level hierarchy only in TO (tests deep create ordering: parent → mid → leaf)
CREATE TABLE data.metrics (
    id     BIGINT NOT NULL,
    year   INT    NOT NULL,
    type   TEXT   NOT NULL,
    value  NUMERIC,
    CONSTRAINT metrics_pkey PRIMARY KEY (id, year, type)
) PARTITION BY RANGE (year);

CREATE TABLE data.metrics_2024
    PARTITION OF data.metrics
    FOR VALUES FROM (2024) TO (2025)
    PARTITION BY LIST (type);

CREATE TABLE data.metrics_2024_cpu
    PARTITION OF data.metrics_2024
    FOR VALUES IN ('cpu');

CREATE TABLE data.metrics_2024_mem
    PARTITION OF data.metrics_2024
    FOR VALUES IN ('mem');

-- Partitioned table test
CREATE TABLE data.partition_test (
    id bigserial NOT NULL,
    flow_id varchar(255) NOT NULL,
    CONSTRAINT partition_test_pkey PRIMARY KEY (id, flow_id)
)
PARTITION BY LIST (flow_id);

CREATE TABLE data.partition_test_default
    PARTITION OF data.partition_test
    DEFAULT;

-- Partition bound change test (TO: value 'inactive')
CREATE TABLE data.partition_bound_test (
    id int,
    status text,
    CONSTRAINT partition_bound_test_pkey PRIMARY KEY (id, status)
) PARTITION BY LIST (status);

CREATE TABLE data.partition_bound_test_active PARTITION OF data.partition_bound_test FOR VALUES IN ('inactive');

-- =============================================================================
-- Partition DDL inheritance test: structural changes on a partitioned table
-- must only be applied to the parent table. Partitions inherit column add/drop,
-- alter (NOT NULL, DEFAULT), and non-FK constraint changes automatically.
-- The comparer must NOT emit these DDL statements for partition children.
--
-- TO state (changes from FROM):
--   parent: data.customers  (PARTITION BY RANGE (created_at))
--     columns: id (PK, NOT NULL), name (NOT NULL, DEFAULT added), email (SET NOT NULL),
--             phone (NEW column), legacy (DROPPED)
--     constraints: customers_pkey, chk_customers_name MODIFIED,
--                  chk_customers_email (NEW)
--   partition: data.customers_2024  (inherits structure)
--   partition: data.customers_2025  (inherits structure)
--
-- Expected: DDL for column add/drop/alter and constraint add/drop/alter
-- must appear ONLY for the parent table data.customers, NOT for
-- data.customers_2024 or data.customers_2025.
-- =============================================================================
CREATE TABLE data.customers (
    id         BIGINT       NOT NULL,
    name       TEXT         NOT NULL DEFAULT 'unknown',
    email      TEXT         NOT NULL,
    phone      TEXT,
    created_at DATE         NOT NULL,
    CONSTRAINT customers_pkey PRIMARY KEY (id, created_at),
    CONSTRAINT chk_customers_name CHECK (char_length(name) > 0),
    CONSTRAINT chk_customers_email CHECK (email ~* '^.+@.+$')
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

-- NEW: second partition only in TO
CREATE TABLE data.logs_2025
    PARTITION OF data.logs
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX idx_logs_message ON data.logs (message);

-- =============================================================================
-- Issue #118: non-partition-key column type change on a partitioned table
-- Changed: amount numeric(10,2) → numeric(15,4).  Partition key is expense_date.
-- Expected: only ALTER COLUMN on the parent, NO DROP/CREATE for the partition.
-- =============================================================================
CREATE TABLE data.expenses (
    id           BIGINT       NOT NULL,
    expense_date DATE         NOT NULL,
    amount       NUMERIC(15,4),
    CONSTRAINT expenses_pkey PRIMARY KEY (id, expense_date)
) PARTITION BY RANGE (expense_date);

CREATE TABLE data.expenses_2024_01
    PARTITION OF data.expenses
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- =============================================================================
-- Partition index test: brand-new partitioned table with index (TO only)
-- The comparer creates parent + partitions from scratch. Indexes on the parent
-- must be emitted once; the partitions must NOT get explicit CREATE INDEX
-- because PostgreSQL propagates parent indexes to partitions automatically.
-- =============================================================================
CREATE TABLE data.tagged_items (
    id     BIGINT  NOT NULL,
    tag    TEXT    NOT NULL,
    detail TEXT,
    CONSTRAINT tagged_items_pkey PRIMARY KEY (id, tag)
) PARTITION BY LIST (tag);

CREATE TABLE data.tagged_items_alpha
    PARTITION OF data.tagged_items
    FOR VALUES IN ('alpha');

CREATE TABLE data.tagged_items_beta
    PARTITION OF data.tagged_items
    FOR VALUES IN ('beta');

CREATE INDEX idx_tagged_items_detail ON data.tagged_items (detail);

-- SQL routines to test dependency handling
CREATE OR REPLACE FUNCTION test_schema.get_active_usernames_sql()
RETURNS TABLE(username varchar, preferred_contact test_schema.contact_type)
LANGUAGE sql
AS $$
    SELECT username, preferred_contact FROM test_schema.users WHERE status = 'active';
$$;

CREATE OR REPLACE FUNCTION test_schema.product_price_with_tax_sql(p_product_id uuid, p_tax_rate numeric, p_currency varchar DEFAULT 'USD')
RETURNS numeric
LANGUAGE sql
AS $$
    SELECT p.price + test_schema.calculate_tax(p.price, p_tax_rate, p_currency)
    FROM test_schema.products p
    WHERE p.id = p_product_id;
$$;

-- View ↔ Routine cross-dependency test (TO side)
-- These objects are NEW (not present in schema_a / FROM).
-- The migration script must create them in dependency order:
--   1. get_user_count()       – function, depends only on test_schema.users
--   2. v_user_stats           – view that calls get_user_count() and reads users
--   3. report_user_stats()    – function that reads v_user_stats
--   4. print_user_stats()     – procedure that reads v_user_stats

CREATE FUNCTION test_schema.get_user_count() RETURNS int AS $$
  SELECT count(*) FROM test_schema.users;
$$ LANGUAGE sql;

CREATE VIEW test_schema.v_user_stats AS
SELECT test_schema.get_user_count() AS total_users,
       username
FROM test_schema.users;

CREATE FUNCTION test_schema.report_user_stats() RETURNS text AS $$
  SELECT 'Total users in view: ' || total_users
  FROM test_schema.v_user_stats
  LIMIT 1;
$$ LANGUAGE sql;

CREATE PROCEDURE test_schema.print_user_stats() LANGUAGE plpgsql AS $$
DECLARE
    cnt int;
BEGIN
    SELECT total_users INTO cnt
    FROM test_schema.v_user_stats
    LIMIT 1;
    RAISE NOTICE 'Total users in view: %', cnt;
END;
$$;

-- Routine dependency ordering test (TO side)
-- These routines are NEW (not present in schema_a / FROM).
-- The migration script must create them in dependency order:
--   1. r_base_value   – no dependencies (leaf)
--   2. x_step_one     – depends on r_base_value
--   3. a_middle_layer – depends on x_step_one and r_base_value
--   4. z_final_report – depends on a_middle_layer

CREATE FUNCTION test_schema.r_base_value()
RETURNS integer
LANGUAGE sql
AS $$
    SELECT 10;
$$;

CREATE FUNCTION test_schema.x_step_one()
RETURNS integer
LANGUAGE sql
AS $$
    SELECT test_schema.r_base_value() + 5;
$$;

CREATE FUNCTION test_schema.a_middle_layer()
RETURNS integer
LANGUAGE sql
AS $$
    SELECT test_schema.x_step_one() * test_schema.r_base_value();
$$;

CREATE PROCEDURE test_schema.z_final_report()
LANGUAGE plpgsql
AS $$
DECLARE
    result integer;
BEGIN
    SELECT test_schema.a_middle_layer() INTO result;
    RAISE NOTICE 'Final result: %', result;
END;
$$;

-- =============================================================================
-- Extension object exclusion test
-- =============================================================================
-- Extensions create their own objects (functions, types, operators, casts, etc.)
-- in the database. These extension-owned objects must NOT be included in the dump
-- or comparison output. Only the extensions themselves should be compared.
--
-- In TO (schema_b):
--   - uuid-ossp  → creates uuid_generate_v4(), uuid_generate_v1(), etc.
--   - pg_trgm    → creates similarity(), show_trgm(), gin_trgm_ops, etc.
--   - hstore     → creates hstore type, hstore(), akeys(), avals(), each(), etc.
--   (pgcrypto removed compared to FROM)
--
-- Expected comparison behavior:
--   - Extension diff: DROP pgcrypto, CREATE hstore.
--   - hstore's own type, functions, operators, casts must NOT appear as
--     individual object creates in the comparison script.
--   - pgcrypto's own functions must NOT appear as individual object drops.
--   - User-defined objects that reference extension types/functions should
--     still be compared normally.
-- =============================================================================

-- User-defined function wrapping extension function (different from FROM).
-- FROM used pgcrypto's gen_random_bytes; TO uses a different implementation
-- since pgcrypto is removed. This tests that the user function is compared
-- (hash changed) while extension functions themselves are excluded.
CREATE OR REPLACE FUNCTION test_schema.generate_token(length INTEGER DEFAULT 32)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    -- pgcrypto removed in TO, use md5-based approach instead
    RETURN substr(md5(random()::text || clock_timestamp()::text), 1, length);
END;
$$;

-- User-defined function that uses uuid-ossp extension function (same as FROM → no diff).
CREATE OR REPLACE FUNCTION test_schema.new_entity_id()
RETURNS UUID
LANGUAGE sql
AS $$
    SELECT uuid_generate_v4();
$$;

-- Table using hstore extension type.
-- This is a user-defined table and SHOULD appear in the dump/comparison,
-- even though the hstore type itself is extension-owned.
-- This table only exists in TO (new table).
CREATE TABLE test_schema.user_preferences (
    user_id INTEGER NOT NULL REFERENCES test_schema.users(id),
    preferences hstore DEFAULT ''::hstore,
    settings hstore DEFAULT ''::hstore,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);

CREATE INDEX idx_user_preferences_prefs ON test_schema.user_preferences USING GIN(preferences);

-- =============================================================================
-- CHECK constraint string literal case preservation test
-- =============================================================================
-- chk_category_values is UNCHANGED → must NOT produce a false diff
-- chk_priority_label is MODIFIED → added 'P5-Informational' value
CREATE TABLE test_schema.check_literal_case_test (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    priority VARCHAR(20) NOT NULL,
    CONSTRAINT chk_category_values CHECK (category IN ('Electronics', 'Home & Garden', 'Books')),
    CONSTRAINT chk_priority_label CHECK (priority IN ('P1-Critical', 'P2-High', 'P3-Medium', 'P4-Low', 'P5-Informational'))  -- MODIFIED: added P5-Informational
);

-- Owner change coverage (TO side)
ALTER SCHEMA test_schema OWNER TO pgc_owner_to;
ALTER TYPE test_schema.status_type OWNER TO pgc_owner_to;
ALTER DOMAIN test_schema.positive_integer OWNER TO pgc_owner_to;
ALTER SEQUENCE test_schema.user_id_seq OWNER TO pgc_owner_to;
ALTER TABLE test_schema.users OWNER TO pgc_owner_to;
ALTER FUNCTION test_schema.update_timestamp() OWNER TO pgc_owner_to;
ALTER VIEW test_schema.product_inventory OWNER TO pgc_owner_to;
ALTER MATERIALIZED VIEW test_schema.active_users_mat OWNER TO pgc_owner_to;

-- v1.0.18 regression: former-owner explicit grant after owner change.
-- After the owner of `test_schema.users` changes from `pgc_owner_from` to
-- `pgc_owner_to`, the TO side retains an explicit grant to the previous
-- owner. Pre-v1.0.18 the comparer filtered both old and new owners as
-- implicit privilege holders, silently dropping this grant from the
-- diff. Post-fix only the TO-side owner is filtered, so the explicit
-- grant to the former owner survives and appears in the migration.
GRANT SELECT ON test_schema.users TO pgc_owner_from;

-- Serial / bigserial / identity column test (TO side)
-- These tables are NEW (not present in schema_a / FROM).
-- The migration script must use serial/bigserial types instead of
-- creating separate sequences + integer/bigint with nextval defaults.

CREATE TABLE test_schema.test_serial (
    id serial
);

CREATE TABLE test_schema.test_bigserial (
    id bigserial
);

CREATE TABLE test_schema.test_identity (
    id integer GENERATED ALWAYS AS IDENTITY
);

-- =============================================================================
-- PG14–18 feature test: Exclusion constraints (btree_gist)
-- =============================================================================
-- TO: table with exclusion constraint (unchanged) plus new column.
-- shift_schedule removed (FROM-only → should be dropped).
-- New: booking_slots table with exclusion constraint (TO-only → should be created).
CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;

CREATE TABLE test_schema.reservations (
    id SERIAL PRIMARY KEY,
    room_id INTEGER NOT NULL,
    during TSTZRANGE NOT NULL,
    guest_name VARCHAR(255),  -- NEW column in TO
    EXCLUDE USING gist (room_id WITH =, during WITH &&)
);

-- shift_schedule removed (FROM-only)

-- TO-only exclusion constraint table (should be created)
CREATE TABLE test_schema.booking_slots (
    id SERIAL PRIMARY KEY,
    resource_id INTEGER NOT NULL,
    slot TSTZRANGE NOT NULL,
    EXCLUDE USING gist (resource_id WITH =, slot WITH &&)
);

-- =============================================================================
-- PG14–18 feature test: NULLS NOT DISTINCT (PG15+)
-- =============================================================================
-- TO: UNIQUE NULLS NOT DISTINCT (only one NULL allowed).
-- FROM had standard UNIQUE.
CREATE TABLE test_schema.unique_nulls_test (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50),
    CONSTRAINT uq_unique_nulls_code UNIQUE NULLS NOT DISTINCT (code)
);

-- =============================================================================
-- PG14–18 feature test: NO INHERIT constraint flag
-- =============================================================================
-- TO: same CHECK constraint with NO INHERIT flag added.
ALTER TABLE test_schema.categories ADD CONSTRAINT chk_categories_sort_order
    CHECK (sort_order >= 0) NO INHERIT;

-- =============================================================================
-- PG14–18 feature test: Column STORAGE and COMPRESSION (PG14+)
-- =============================================================================
-- TO: STORAGE changed to EXTERNAL on payload, compression set to lz4 on blob.
CREATE TABLE test_schema.storage_test (
    id SERIAL PRIMARY KEY,
    payload TEXT,
    blob BYTEA
);
ALTER TABLE test_schema.storage_test ALTER COLUMN payload SET STORAGE EXTERNAL;
ALTER TABLE test_schema.storage_test ALTER COLUMN blob SET STORAGE EXTERNAL;
-- Note: COMPRESSION lz4 requires PG14+ compiled --with-lz4.
-- Uncomment below if available:
-- ALTER TABLE test_schema.storage_test ALTER COLUMN blob SET COMPRESSION lz4;

-- =============================================================================
-- PG14–18 feature test: SECURITY INVOKER views (PG15+)
-- =============================================================================
-- TO: same view definition but WITH (security_invoker = true).
CREATE VIEW test_schema.security_invoker_view
WITH (security_invoker = true)
AS
SELECT id, username, email
FROM test_schema.users
WHERE status = 'active';

-- =============================================================================
-- PG14–18 feature test: Range types
-- =============================================================================
-- TO: float_range without subtype_diff (modified → drop+recreate).
-- old_range removed (FROM-only → should be dropped).
-- int_range added (TO-only → should be created).
CREATE TYPE test_schema.float_range AS RANGE (
    SUBTYPE = float8
);

-- old_range removed (FROM-only)

CREATE TYPE test_schema.int_range AS RANGE (
    SUBTYPE = int4
);

-- =============================================================================
-- PG14–18 feature test: Foreign tables (postgres_fdw)
-- =============================================================================
-- TO: foreign table modified (column type widened, new column added).
-- foreign_logs removed (FROM-only → should be dropped).
-- foreign_products added (TO-only → should be created).
CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;

CREATE SERVER test_foreign_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'localhost', dbname 'postgres');

-- TO: modified foreign table (username widened, status column added)
CREATE FOREIGN TABLE test_schema.foreign_users (
    id INTEGER NOT NULL,
    username VARCHAR(100),
    email VARCHAR(255),
    status VARCHAR(20)
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'users');

-- foreign_logs removed (FROM-only)

-- TO-only foreign table (should be created)
CREATE FOREIGN TABLE test_schema.foreign_products (
    product_id UUID,
    name VARCHAR(255),
    price NUMERIC(10,2)
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'products');

-- =============================================================================
-- PG14–18 feature test: Extended statistics (PG10+)
-- =============================================================================
-- TO: modified statistics (added mcv kind).
-- stat_products_old removed (FROM-only → should be dropped).
-- stat_products_new added (TO-only → should be created).
CREATE STATISTICS test_schema.stat_users_email_status (dependencies, ndistinct, mcv)
    ON email, status FROM test_schema.users;

-- stat_products_old removed (FROM-only)

-- TO-only statistics (should be created)
CREATE STATISTICS test_schema.stat_products_new (ndistinct)
    ON name, category_id FROM test_schema.products;

-- =============================================================================
-- PG18 feature test: NOT ENFORCED constraints (PG18+)
-- =============================================================================
-- TO: same constraint with NOT ENFORCED flag.
-- Note: Requires PostgreSQL 18+. Comment out if testing on earlier versions.
ALTER TABLE test_schema.products ADD CONSTRAINT chk_products_sku_format
    CHECK (sku ~ '^[A-Z]{2,4}-[0-9]+$') NOT ENFORCED;

-- =============================================================================
-- PG18 feature test: Virtual generated columns (PG18+)
-- =============================================================================
-- TO: full_name is now GENERATED ALWAYS AS ... STORED.
-- Note: On PostgreSQL 18+ this could also be VIRTUAL.
CREATE TABLE test_schema.virtual_gen_test (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    full_name VARCHAR(101) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
);

-- =============================================================================
-- UNLOGGED tables test (TO side)
-- =============================================================================
-- TO: UNLOGGED table (was regular/logged)
CREATE UNLOGGED TABLE test_schema.unlogged_test (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- =============================================================================
-- Storage parameters (reloptions) test (TO side)
-- =============================================================================
-- TO: table with fillfactor=90 and autovacuum_enabled=false (was fillfactor=70)
CREATE TABLE test_schema.storage_params_test (
    id SERIAL PRIMARY KEY,
    value TEXT
) WITH (fillfactor = 90, autovacuum_enabled = false);

-- =============================================================================
-- REPLICA IDENTITY test (TO side)
-- =============================================================================
-- TO: table with REPLICA IDENTITY FULL (was DEFAULT)
CREATE TABLE test_schema.replica_identity_test (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
ALTER TABLE test_schema.replica_identity_test REPLICA IDENTITY FULL;

-- =============================================================================
-- FORCE ROW LEVEL SECURITY test (TO side)
-- =============================================================================
-- TO: table with RLS enabled AND forced (was enabled but not forced)
CREATE TABLE test_schema.force_rls_test (
    id SERIAL PRIMARY KEY,
    tenant_id INT NOT NULL,
    data TEXT
);
ALTER TABLE test_schema.force_rls_test ENABLE ROW LEVEL SECURITY;
ALTER TABLE test_schema.force_rls_test FORCE ROW LEVEL SECURITY;
CREATE POLICY force_rls_policy ON test_schema.force_rls_test
    FOR ALL
    USING (tenant_id = current_setting('app.tenant_id')::int);

-- =============================================================================
-- Classical inheritance test (TO side)
-- =============================================================================
-- TO: parent table with child (child_data type changed)
CREATE TABLE test_schema.inheritance_parent (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);
CREATE TABLE test_schema.inheritance_child (
    child_data VARCHAR(255)
) INHERITS (test_schema.inheritance_parent);

-- =============================================================================
-- Typed table (OF type) test (TO side)
-- =============================================================================
-- TO: same typed table
CREATE TYPE test_schema.address_type AS (
    street VARCHAR(200),
    city VARCHAR(100),
    zip VARCHAR(20)
);
CREATE TABLE test_schema.typed_table_test OF test_schema.address_type;

-- =============================================================================
-- Per-column statistics target test (TO side)
-- =============================================================================
-- TO: column with statistics target 500 (was 100)
CREATE TABLE test_schema.col_stats_test (
    id SERIAL PRIMARY KEY,
    searchable_data TEXT
);
ALTER TABLE test_schema.col_stats_test ALTER COLUMN searchable_data SET STATISTICS 500;

-- =============================================================================
-- Function COST/ROWS test (TO side)
-- =============================================================================
-- TO: function with COST 200 and ROWS 500 (was COST 100, ROWS 1000)
CREATE OR REPLACE FUNCTION test_schema.cost_rows_test(n INT)
RETURNS SETOF INT
LANGUAGE sql
COST 200
ROWS 500
AS $$
    SELECT generate_series(1, n);
$$;

-- =============================================================================
-- Grants comparison test (TO side)
-- =============================================================================
-- These GRANT statements establish the TO target for grant comparison.
-- Differences from Schema A (FROM):
--   Unchanged: users SELECT→reader, view SELECT→reader, schema USAGE→reader
--   Modified:  users writer loses UPDATE (was SELECT,INSERT,UPDATE → SELECT,INSERT)
--              products reader gains INSERT (was SELECT → SELECT,INSERT)
--   Added:     products writer (SELECT,UPDATE) — new grantee
--              view writer (SELECT) — new grantee
--              schema writer (USAGE,CREATE) — new grantee
--              new function calculate_average_rating EXECUTE→reader
--   Removed:   sequence user_id_seq USAGE→reader (no grant in TO)
--              function update_timestamp EXECUTE→reader (no grant in TO)

-- Schema grants
GRANT USAGE ON SCHEMA test_schema TO pgc_grant_reader;
GRANT USAGE, CREATE ON SCHEMA test_schema TO pgc_grant_writer;

-- Table grants
GRANT SELECT ON test_schema.users TO pgc_grant_reader;
GRANT SELECT, INSERT ON test_schema.users TO pgc_grant_writer;
GRANT SELECT, INSERT ON test_schema.products TO pgc_grant_reader;
GRANT SELECT, UPDATE ON test_schema.products TO pgc_grant_writer;

-- Sequence grants (user_id_seq grant removed compared to FROM)

-- View grants
GRANT SELECT ON test_schema.product_inventory TO pgc_grant_reader;
GRANT SELECT ON test_schema.product_inventory TO pgc_grant_writer;

-- Function grants (update_timestamp grant removed; new function grant added)
GRANT EXECUTE ON FUNCTION test_schema.calculate_average_rating(UUID) TO pgc_grant_reader;

-- Foreign table grants
-- v1.0.18 regression: pre-fix, FOREIGN TABLE ACL parsing fell through to
-- the empty default privilege set, so any explicit grant on a foreign
-- table was silently filtered out of the diff. Post-fix, FOREIGN TABLE
-- shares the TABLE privilege set (SELECT, INSERT, UPDATE, DELETE,
-- TRUNCATE, REFERENCES, TRIGGER, MAINTAIN).
GRANT SELECT ON test_schema.foreign_users TO pgc_grant_reader;
-- MAINTAIN is PG17+; uncomment on PG17+ to additionally exercise the
-- new privilege bit on a foreign table:
-- GRANT MAINTAIN ON test_schema.foreign_users TO pgc_grant_writer;

-- =============================================================================
-- Rules comparison test (TO side)
-- =============================================================================
-- test_products_no_delete: same as FROM (no diff)
-- test_users_soft_delete:  removed (FROM-only)
-- test_reviews_audit:      added (TO-only)
CREATE RULE rule_products_no_delete AS ON DELETE TO test_schema.products DO INSTEAD NOTHING;

CREATE RULE rule_reviews_audit AS ON INSERT TO test_schema.reviews
    DO ALSO
        INSERT INTO shared_schema.audit_logs (table_name, operation, new_values)
        VALUES ('reviews', 'INSERT', row_to_json(NEW)::jsonb);

-- =============================================================================
-- Event triggers comparison test (TO side)
-- =============================================================================
-- test_etrig_unchanged: same as FROM (no diff)
-- test_etrig_from_only: removed (FROM-only)
-- test_etrig_to_only:   added (TO-only)
CREATE OR REPLACE FUNCTION test_schema.etrig_log_ddl() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'DDL event: % on %', TG_EVENT, TG_TAG;
END;
$$;

CREATE OR REPLACE FUNCTION test_schema.etrig_to_only_fn() RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
    NULL;
END;
$$;

CREATE EVENT TRIGGER test_etrig_unchanged
    ON ddl_command_start
    EXECUTE FUNCTION test_schema.etrig_log_ddl();

CREATE EVENT TRIGGER test_etrig_to_only
    ON ddl_command_end
    EXECUTE FUNCTION test_schema.etrig_to_only_fn();

-- =============================================================================
-- Collations comparison test (TO side)
-- =============================================================================
-- test_coll_unchanged: same as FROM (no diff); test_coll_from_only: removed; test_coll_to_only: added
CREATE COLLATION test_schema.test_coll_unchanged (provider = libc, lc_collate = 'C', lc_ctype = 'C');
CREATE COLLATION test_schema.test_coll_to_only (provider = libc, lc_collate = 'C', lc_ctype = 'C');

-- =============================================================================
-- Text search dictionary comparison test (TO side)
-- =============================================================================
-- test_dict_unchanged: same; test_dict_from_only: removed; test_dict_modified: stopwords removed; test_dict_to_only: added
CREATE TEXT SEARCH DICTIONARY test_schema.test_dict_unchanged (
    TEMPLATE = pg_catalog.simple,
    STOPWORDS = english
);
CREATE TEXT SEARCH DICTIONARY test_schema.test_dict_modified (  -- MODIFIED: STOPWORDS removed
    TEMPLATE = pg_catalog.simple
);
CREATE TEXT SEARCH DICTIONARY test_schema.test_dict_to_only (
    TEMPLATE = pg_catalog.simple
);

-- =============================================================================
-- Text search configuration comparison test (TO side)
-- =============================================================================
-- test_tsconfig_unchanged: same; test_tsconfig_from_only: removed; test_tsconfig_to_only: added
CREATE TEXT SEARCH CONFIGURATION test_schema.test_tsconfig_unchanged (
    PARSER = pg_catalog.default
);
CREATE TEXT SEARCH CONFIGURATION test_schema.test_tsconfig_to_only (
    PARSER = pg_catalog.default
);

-- =============================================================================
-- Cast comparison test (TO side)
-- =============================================================================
-- FROM-only cast (test_type_A → text) removed (test_type_A doesn't exist in TO)
-- Shared cast unchanged: user_profile → text
-- TO-only cast added: test_type_B → text
CREATE OR REPLACE FUNCTION test_schema.user_profile_to_text(test_schema.user_profile) RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT ($1).first_name || ' ' || ($1).last_name; $$;

CREATE CAST (test_schema.user_profile AS text)
    WITH FUNCTION test_schema.user_profile_to_text(test_schema.user_profile);

CREATE OR REPLACE FUNCTION test_schema.test_type_b_to_text(test_schema.test_type_B) RETURNS text
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT ($1).street || ', ' || ($1).city; $$;

CREATE CAST (test_schema.test_type_B AS text)
    WITH FUNCTION test_schema.test_type_b_to_text(test_schema.test_type_B);

-- =============================================================================
-- Operator comparison test (TO side)
-- =============================================================================
-- Unchanged: ~< (text, int) → bool (same as FROM)
-- FROM-only ~> dropped (text_longer_than and ~> not present in TO)
-- TO-only added: ~= (text, int) → bool — length($1) = $2
CREATE OR REPLACE FUNCTION test_schema.text_shorter_than(text, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT length($1) < $2; $$;

CREATE OPERATOR test_schema.~< (
    LEFTARG = text,
    RIGHTARG = integer,
    FUNCTION = test_schema.text_shorter_than
);

CREATE OR REPLACE FUNCTION test_schema.text_equals_length(text, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT length($1) = $2; $$;

CREATE OPERATOR test_schema.~= (
    LEFTARG = text,
    RIGHTARG = integer,
    FUNCTION = test_schema.text_equals_length
);

-- =============================================================================
-- Default privileges comparison test (TO side)
-- =============================================================================
-- reader SELECT on new tables in test_schema: unchanged
-- writer INSERT only: MODIFIED to SELECT, INSERT, UPDATE
ALTER DEFAULT PRIVILEGES IN SCHEMA test_schema
    GRANT SELECT ON TABLES TO pgc_grant_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA test_schema
    GRANT SELECT, INSERT, UPDATE ON TABLES TO pgc_grant_writer;  -- MODIFIED: was INSERT only

-- =============================================================================
-- Foreign server and user mapping comparison test (TO side)
-- =============================================================================
-- test_server_modified: host and dbname changed; test_server_from_only: removed; test_server_to_only: added
CREATE SERVER test_server_modified
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'server-host-b', dbname 'db_b', port '5432');  -- MODIFIED: host and dbname changed

CREATE SERVER test_server_to_only
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'new-host', dbname 'new_db');

-- User mapping unchanged: PUBLIC on test_foreign_server
-- User mapping dropped:   PUBLIC on test_server_from_only (server removed)
-- User mapping added:     PUBLIC on test_server_to_only (server added)
CREATE USER MAPPING FOR PUBLIC
    SERVER test_foreign_server
    OPTIONS (user 'readonly_user');

CREATE USER MAPPING FOR PUBLIC
    SERVER test_server_to_only
    OPTIONS (user 'new_readonly_user');

-- =============================================================================
-- Publications comparison test (TO side)
-- =============================================================================
-- Requires wal_level = logical.
-- test_pub_from_only: removed; test_pub_unchanged: same as FROM; test_pub_to_only: added
CREATE PUBLICATION test_pub_unchanged FOR TABLE test_schema.users, test_schema.products;
CREATE PUBLICATION test_pub_to_only FOR ALL TABLES;

-- =============================================================================
-- Issue #179 — DROP FUNCTION ... CASCADE recreate test (TO side)
-- =============================================================================
-- Function `cascade_compute(integer)` returns BIGINT here (was INTEGER in
-- Schema A). The argument list (`integer`) is unchanged, so the
-- routine's signature in PostgreSQL terms is identical to FROM; only
-- the return type differs. PostgreSQL has no `ALTER FUNCTION` for
-- return types, so the comparer still emits `DROP FUNCTION ... CASCADE`,
-- which silently removes the CHECK / functional index / generated column
-- / column DEFAULT / RLS policy below — every dependent that
-- `pg_depend` connects to the function. Phase 7 of
-- `compare_routines_and_views` must re-emit each one. The dependent
-- definitions below are intentionally identical (modulo the integer→
-- bigint type bumps that follow from the new return type) to FROM, so
-- the regular table/index/policy diffs would emit nothing for them —
-- without Phase 7 the migration would leave them missing.
CREATE FUNCTION test_schema.cascade_compute(x integer)
RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
    SELECT (x * 2)::bigint;
$$;

CREATE TABLE test_schema.cascade_items (
    id      serial  PRIMARY KEY,
    value   integer NOT NULL,
    -- def_col type bumped integer → bigint to match the new return type;
    -- the DEFAULT itself is still cascaded by DROP FUNCTION ... CASCADE.
    def_col bigint  DEFAULT test_schema.cascade_compute(0),
    -- gen_col type bumped integer → bigint; the column itself is
    -- cascaded by DROP FUNCTION ... CASCADE (Postgres drops generated
    -- columns wholesale, not just their expression) and must be re-added.
    gen_col bigint  GENERATED ALWAYS AS (test_schema.cascade_compute(value)) STORED
);

ALTER TABLE test_schema.cascade_items
    ADD CONSTRAINT chk_cascade_compute CHECK (test_schema.cascade_compute(value) > 0);

CREATE INDEX idx_cascade_compute
    ON test_schema.cascade_items (test_schema.cascade_compute(value));

ALTER TABLE test_schema.cascade_items ENABLE ROW LEVEL SECURITY;
CREATE POLICY pol_cascade_items ON test_schema.cascade_items
    USING (test_schema.cascade_compute(value) > 0);

-- =============================================================================
-- Issue #188 — Secondary dependents of CASCADE-dropped generated column
-- =============================================================================
-- Function `cascade_compute_v2(integer)` returns BIGINT here (was INTEGER
-- in Schema A). The DROP FUNCTION ... CASCADE removes the generated
-- column `gen_total` from FROM, which in turn CASCADE-drops the plain
-- index and CHECK constraint attached to it. Both objects' definitions
-- here are byte-identical to Schema A (modulo the integer→bigint type
-- bump on the column) — without the pg_depend graph, the comparer
-- would emit nothing for them and the migrated database would be
-- missing them.
CREATE FUNCTION test_schema.cascade_compute_v2(x integer)
RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
    SELECT (x + 1)::bigint;
$$;

CREATE TABLE test_schema.cascade_col_dep_items (
    id        serial  PRIMARY KEY,
    value     integer NOT NULL,
    -- gen_total type bumped integer → bigint to match the new return type;
    -- the column itself is CASCADE-dropped by the function drop.
    gen_total bigint  GENERATED ALWAYS AS (test_schema.cascade_compute_v2(value)) STORED
);

CREATE INDEX idx_cascade_col_dep_gen_total
    ON test_schema.cascade_col_dep_items (gen_total);

ALTER TABLE test_schema.cascade_col_dep_items
    ADD CONSTRAINT chk_cascade_col_dep_gen_total CHECK (gen_total >= 0);

ALTER TABLE test_schema.cascade_col_dep_items
    ADD CONSTRAINT uq_cascade_col_dep_gen_total UNIQUE (gen_total);

CREATE TABLE test_schema.cascade_col_dep_children (
    id        serial  PRIMARY KEY,
    -- ref_total type bumped integer → bigint to match the new
    -- gen_total type on the parent. FK target type must match.
    ref_total bigint
);

ALTER TABLE test_schema.cascade_col_dep_children
    ADD CONSTRAINT fk_cascade_col_dep_children_ref_total
        FOREIGN KEY (ref_total)
        REFERENCES test_schema.cascade_col_dep_items (gen_total);

-- =============================================================================
-- Issue #180 — FK-ordered SET LOGGED/UNLOGGED + redundant owned-sequence ALTER
-- =============================================================================
-- TO mirrors Schema A's FK chain (child -> parent -> grandparent) but
-- every table is now UNLOGGED. The comparer must emit
-- `ALTER TABLE ... SET UNLOGGED` in FK-leaf-first order
-- (child, then parent, then grandparent) — alphabetical order rejects
-- with:
--   ERROR: could not change table "grandparent" to unlogged because
--   it references logged table "parent"
--
-- The serial PKs auto-create owned sequences (`*_id_seq`). PostgreSQL
-- propagates the table's `ALTER TABLE SET UNLOGGED` to every owned
-- sequence on its own, so no explicit `ALTER SEQUENCE ... SET
-- UNLOGGED` should appear for those sequences in the migration.
CREATE SCHEMA IF NOT EXISTS test_order;

CREATE UNLOGGED TABLE test_order.grandparent (
    id serial PRIMARY KEY
);

CREATE UNLOGGED TABLE test_order.parent (
    id serial PRIMARY KEY,
    grandparent_id integer REFERENCES test_order.grandparent(id)
);

CREATE UNLOGGED TABLE test_order.child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES test_order.parent(id)
);

-- =============================================================================
-- Issue #191 — Persistence-ordering with mutual FK cycle
-- =============================================================================
-- Mirrors the cycle pair in Schema A but flipped to UNLOGGED. The
-- mutual deferrable FKs `cycle_a ↔ cycle_b` are unchanged between
-- FROM and TO (live edges), so they remain on the FK adjacency at
-- the SET phase. With both endpoints in the cyclic set, the comparer
-- must DROP both FKs before either SET UNLOGGED, run the SETs in any
-- order, and re-ADD the FKs from the TO definitions.
CREATE UNLOGGED TABLE test_order.cycle_a (
    id serial PRIMARY KEY,
    b_id integer
);

CREATE UNLOGGED TABLE test_order.cycle_b (
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
-- Mirrors the LOGGED pair in Schema A but flipped to UNLOGGED. The FK
-- references `public.persistence_chain_parent(id)` via the unqualified
-- form `pg_get_constraintdef` produces when `public` is on the
-- search_path; the comparer must restore the FK edge against the FK
-- owner's schema (`public`) so the SET UNLOGGED order is leaves-first.
CREATE UNLOGGED TABLE public.persistence_chain_parent (
    id serial PRIMARY KEY
);

CREATE UNLOGGED TABLE public.persistence_chain_child (
    id serial PRIMARY KEY,
    parent_id integer REFERENCES public.persistence_chain_parent(id)
);

-- =============================================================================
-- Auto-named NOT NULL collision fixture (intentionally REVERSED order)
-- =============================================================================
-- See schema_a.sql for full context.
--
-- IMPORTANT — load-bearing asymmetry: the CREATE TABLE statements
-- below appear in the REVERSE order from schema_a.sql. PG's
-- `ChooseConstraintName` is deterministic for a given creation order,
-- so reversing the order forces PG to assign the numeric collision
-- suffix (digits appended directly to `not_null`, no underscore) to
-- the OPPOSITE table relative to Schema A:
--
--   Schema A: nn_coll_a   .b_c → ..._not_null  (no suffix)
--             nn_coll_a_b .c   → ..._not_null1 (suffix)
--   Schema B: nn_coll_a_b .c   → ..._not_null  (no suffix)
--             nn_coll_a   .b_c → ..._not_null1 (suffix)
--
-- This asymmetry is the WHOLE POINT of the regression test: with
-- identical CREATE order PG would assign identical names in both DBs
-- and a naïve comparer would also produce zero diffs (false pass).
-- Do not align the order with schema_a.sql.
--
-- The comparer (post-fix) must still produce ZERO diffs for these
-- tables despite the suffixed constraint living on a different table
-- on each side.
CREATE TABLE test_schema.nn_coll_a_b (
    id serial PRIMARY KEY,
    c integer NOT NULL
);

CREATE TABLE test_schema.nn_coll_a (
    id serial PRIMARY KEY,
    b_c integer NOT NULL
);

-- =============================================================================
-- Regression: views that reference only functions (issue #219)
-- =============================================================================
-- See schema_a.sql for the full description. Here the view body carries the
-- extra WHERE clause, which is the change `compare` must detect and emit.
CREATE FUNCTION test_schema.fn_only_view_source()
RETURNS TABLE(id integer, val text)
LANGUAGE sql AS $$ SELECT 1, 'hello' $$;

CREATE VIEW test_schema.vw_from_function AS
SELECT id, val FROM test_schema.fn_only_view_source()
WHERE id > 0;

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
-- See schema_a.sql for the full description. Here mv_no_data exists and is
-- unpopulated; the generated diff must create it WITH NO DATA rather than run the
-- query and fill it.
CREATE TABLE test_schema.mv_nodata_base (
    id integer PRIMARY KEY,
    val text,
    amount numeric
);

CREATE MATERIALIZED VIEW test_schema.mv_with_data AS
SELECT id, val FROM test_schema.mv_nodata_base;

-- TO-only, and deliberately never refreshed.
CREATE MATERIALIZED VIEW test_schema.mv_no_data AS
SELECT id, val, amount FROM test_schema.mv_nodata_base WHERE amount > 0
WITH NO DATA;

-- check option changed from LOCAL (FROM) to CASCADED (TO).
CREATE VIEW test_schema.vw_check_opt AS
SELECT id, val FROM test_schema.mv_nodata_base WHERE amount > 0
WITH CASCADED CHECK OPTION;

-- =============================================================================
-- Regression: reloptions on partitions of a table recreated as partitioned (#216)
-- =============================================================================
-- See schema_a.sql for the full description. Here the table is partitioned and every
-- partition carries reloptions that the generated CREATE TABLE ... PARTITION OF must
-- keep.
CREATE TABLE test_schema.reloptions_to_partitioned (
    id INTEGER,
    created_at TIMESTAMPTZ NOT NULL
) PARTITION BY RANGE (created_at);

CREATE TABLE test_schema.reloptions_to_partitioned_2025
    PARTITION OF test_schema.reloptions_to_partitioned
    FOR VALUES FROM ('2024-12-31 23:00:00+00') TO ('2025-12-31 23:00:00+00');
ALTER TABLE test_schema.reloptions_to_partitioned_2025 SET (
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_scale_factor = 0.05
);

-- A DEFAULT partition with a different reloption set, so the fixture also covers a
-- bound that is not FOR VALUES.
CREATE TABLE test_schema.reloptions_to_partitioned_default
    PARTITION OF test_schema.reloptions_to_partitioned DEFAULT;
ALTER TABLE test_schema.reloptions_to_partitioned_default SET (
    fillfactor = 70
);

-- Inverse direction: partitioned -> regular, reloptions via the CREATE TABLE WITH.
CREATE TABLE test_schema.reloptions_to_regular (
    id INTEGER,
    created_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE test_schema.reloptions_to_regular SET (
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_scale_factor = 0.05
);

-- =============================================================================
-- Regression: SET config values must round-trip (issue #217)
-- =============================================================================
-- See schema_a.sql. Here the procedure carries an unquoted list-valued search_path
-- alongside a scalar and a numeric SET; the generated CREATE OR REPLACE must emit the
-- list verbatim so the applied routine's proconfig matches this source exactly.
CREATE OR REPLACE PROCEDURE test_schema.set_config_roundtrip(p_id integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = test_schema, pg_temp
SET lock_timeout = '5s'
SET statement_timeout = 30000
AS $$
BEGIN
    RAISE NOTICE 'id=%', p_id;
END;
$$;

-- =============================================================================
-- Regression: inline NOT NULL must not become a named ADD/DROP CONSTRAINT (#218)
-- =============================================================================
-- See schema_a.sql. Here nn_col is gone and null_col is NOT NULL; the diff must be
-- just `drop column nn_col` and `alter column null_col set not null`.
CREATE TABLE test_schema.notnull_recreate (
    id       integer,
    null_col varchar(10) NOT NULL
);

-- =============================================================================
-- Regression: index on a partitioned parent must be created valid (#223)
-- =============================================================================
-- See schema_a.sql. Here the partitioned parent gains an index; the generated
-- CREATE INDEX must be a plain `ON` (not `ON ONLY`) so the applied index is valid.
CREATE TABLE test_schema.partidx (
    id      integer,
    value   text,
    bucket  date
) PARTITION BY RANGE (bucket);

CREATE TABLE test_schema.partidx_2024 PARTITION OF test_schema.partidx
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE test_schema.partidx_2025 PARTITION OF test_schema.partidx
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX idx_partidx_value ON test_schema.partidx (value);

-- =============================================================================
-- Regression: non-idempotent IN-list deparsing must not loop (issue #226)
-- =============================================================================
-- See schema_a.sql. The matview and partial index below carry an IN-list whose
-- deparsed form is not idempotent; the second diff must be empty despite the base
-- table being identical in both schemas.
CREATE TABLE test_schema.innorm (
    id          integer PRIMARY KEY,
    label       character varying(50),
    action_type character varying(50),
    device      jsonb
);

CREATE MATERIALIZED VIEW test_schema.innorm_mv AS
SELECT id, label
FROM test_schema.innorm
WHERE label IN ('FOO', 'BAR')
WITH NO DATA;

CREATE INDEX ix_innorm_trust
    ON test_schema.innorm USING btree (id, ((device ->> 'id'::text)))
    WHERE action_type IN ('FOO', 'BAR');

-- Companion to the innorm case (#226): explicit varchar-typmod IN-list. See
-- schema_a.sql for the full description.
CREATE TABLE test_schema.innorm_typmod (
    id   integer PRIMARY KEY,
    code character varying(10)
);

ALTER TABLE test_schema.innorm_typmod
    ADD CONSTRAINT chk_innorm_typmod
    CHECK (code IN ('A'::varchar(10), 'B'::varchar(10)));

-- =============================================================================
-- Regression: incompatible view column change needs DROP+CREATE (issue #227)
-- =============================================================================
-- See schema_a.sql. `kind` is inserted before profile_id, which CREATE OR
-- REPLACE VIEW cannot express; v227_dep is textually unchanged but must ride
-- along through drop+recreate.
CREATE TABLE test_schema.v227_item (
    id         integer PRIMARY KEY,
    status     text,
    kind       text,
    profile_id integer
);

CREATE VIEW test_schema.v227_base AS
SELECT id, status, kind, profile_id FROM test_schema.v227_item;

CREATE VIEW test_schema.v227_dep AS
SELECT id, status FROM test_schema.v227_base;

-- =============================================================================
-- CASCADE recreation of an index on a partitioned parent (PR #234 review)
-- =============================================================================
-- See schema_a.sql: the return type changed integer -> bigint, forcing
-- DROP FUNCTION ... CASCADE which takes ix_cascade_part_fn with it.
CREATE FUNCTION test_schema.cascade_part_fn(text)
RETURNS bigint IMMUTABLE LANGUAGE sql AS $$ SELECT length($1)::bigint $$;

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
-- See schema_a.sql for the full description. Here mv235_recreated gains a column
-- (forcing the drop+recreate that used to lose every index), mv235_stable keeps
-- its definition while its index set changes underneath it, and mv235_new is a
-- TO-only view whose index has to be created with it.
CREATE TABLE test_schema.mv235_base (
    id     integer PRIMARY KEY,
    label  text,
    amount numeric,
    active boolean
);

CREATE MATERIALIZED VIEW test_schema.mv235_recreated AS
SELECT id, label, amount FROM test_schema.mv235_base;

CREATE UNIQUE INDEX ix_mv235_recreated_id ON test_schema.mv235_recreated (id);
CREATE INDEX ix_mv235_recreated_partial ON test_schema.mv235_recreated (label)
    WHERE label IS NOT NULL;
COMMENT ON INDEX test_schema.ix_mv235_recreated_partial IS 'partial index on a matview';

CREATE MATERIALIZED VIEW test_schema.mv235_stable AS
SELECT id, label, amount FROM test_schema.mv235_base;

-- ix_mv235_stable_drop is gone.
CREATE INDEX ix_mv235_stable_redef ON test_schema.mv235_stable (label DESC);
CREATE INDEX ix_mv235_stable_cmt ON test_schema.mv235_stable (id, amount);
COMMENT ON INDEX test_schema.ix_mv235_stable_cmt IS 'comment after';
-- TO-only index on a view that is not being recreated.
CREATE UNIQUE INDEX ix_mv235_stable_added ON test_schema.mv235_stable (id);

CREATE MATERIALIZED VIEW test_schema.mv235_new AS
SELECT id, label, active FROM test_schema.mv235_base WHERE active;

CREATE INDEX ix_mv235_new_label ON test_schema.mv235_new (label);

-- ─────────────────────────────────────────────────────────────────────
-- Issue #240: a routine named in prose or in a literal is not a call.
--
-- Three TO-only routines forming one real chain, util_a <- helper_d <-
-- z_caller, with a false back-edge planted at each link:
--   * i240_util_a's comment names i240_helper_d, which calls it;
--   * i240_helper_d's string literal names i240_z_caller, which calls it.
-- Each false edge closes a 2-cycle with the real one. Kahn's sort cannot
-- order a cycle, so it appends all three in name order — i240_helper_d
-- first, which fails because i240_util_a does not exist yet, and the
-- round-2 diff is then non-empty.
-- ─────────────────────────────────────────────────────────────────────
CREATE FUNCTION test_schema.i240_util_a(pvalue text) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    -- NB: mirrors test_schema.i240_helper_d's normalization rules.
    SELECT translate(coalesce(pvalue, ''), 'ao', 'AO');
$$;

CREATE FUNCTION test_schema.i240_helper_d(pvalue text) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT test_schema.i240_util_a(pvalue) || ''
        || coalesce(nullif('', 'test_schema.i240_z_caller'), '');
$$;

CREATE FUNCTION test_schema.i240_z_caller(pvalue text) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT test_schema.i240_helper_d(pvalue);
$$;

-- ─────────────────────────────────────────────────────────────────────
-- Issue #242: `unrelated_2` is added and `status` is retyped. The two
-- views reading only `id` must survive untouched; the one reading
-- `status` must still be dropped and recreated.
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE test_schema.i242_orders (
    id           integer PRIMARY KEY,
    status       varchar(50) NOT NULL,
    unrelated    integer,
    unrelated_2  timestamptz
);

CREATE MATERIALIZED VIEW test_schema.i242_mv_untouched AS
SELECT id, count(*) AS cnt FROM test_schema.i242_orders GROUP BY id;

CREATE INDEX ix_i242_mv_untouched_id ON test_schema.i242_mv_untouched (id);

CREATE VIEW test_schema.i242_v_untouched AS
SELECT id FROM test_schema.i242_orders;

CREATE MATERIALIZED VIEW test_schema.i242_mv_touched AS
SELECT status, count(*) AS cnt FROM test_schema.i242_orders GROUP BY status;

-- ─────────────────────────────────────────────────────────────────────
-- Issue #243: each column drops its identity and relaxes one of the three
-- things an identity column guarantees.
-- ─────────────────────────────────────────────────────────────────────
CREATE TABLE test_schema.i243_identity_drop (
    goes_nullable   bigint,
    gains_default   bigint NOT NULL DEFAULT 42,
    leaves_integer  text NOT NULL,
    label           text
);

-- ═════════════════════════════════════════════════════════════════════
-- PostgreSQL 19 feature tests (pgc v1.1.0), TO side.
-- PG19-only statements fail harmlessly on PG14-18: CI applies this file
-- with ON_ERROR_STOP=0 and the round trip must still close there.
-- Keep every statement parenthesis-balanced — psql does not end a
-- statement at ';' inside an open parenthesis, so one broken statement
-- would swallow everything after it.
-- ═════════════════════════════════════════════════════════════════════

-- ==== BEGIN pg19:publications — publications: FOR ALL SEQUENCES, FOR ALL TABLES EXCEPT, TABLES IN SCHEMA, row filters, column lists, options ====
-- =============================================================================
-- PG19 feature test: publication membership and options (TO side)
-- =============================================================================
-- Requires wal_level = logical. Statements a server cannot parse fail
-- harmlessly under ON_ERROR_STOP=0 (column lists, row filters and TABLES IN
-- SCHEMA need PG15+, publish_generated_columns PG18+, FOR ALL SEQUENCES and
-- EXCEPT PG19+). See the FROM side for what each case exercises.
CREATE TABLE test_schema.tbl_pg19_pub_items (id integer PRIMARY KEY, qty integer, note text);
CREATE TABLE test_schema.tbl_pg19_pub_orders (id integer PRIMARY KEY, amount numeric);
CREATE TABLE test_schema."tbl_pg19_pub_Mixed" (id integer PRIMARY KEY);
CREATE TABLE test_schema.tbl_pg19_pub_events (id integer, kind text NOT NULL) PARTITION BY LIST (kind);
CREATE TABLE test_schema.tbl_pg19_pub_events_a PARTITION OF test_schema.tbl_pg19_pub_events FOR VALUES IN ('a');
CREATE TABLE test_schema.tbl_pg19_pub_rebuilt (id integer NOT NULL) PARTITION BY LIST (id);
CREATE TABLE test_schema.tbl_pg19_pub_parent (id integer);
CREATE TABLE test_schema.tbl_pg19_pub_child () INHERITS (test_schema.tbl_pg19_pub_parent);

CREATE PUBLICATION pg19_pub_all_to_list FOR TABLE test_schema.tbl_pg19_pub_items;
CREATE PUBLICATION pg19_pub_all_to_empty;
CREATE PUBLICATION pg19_pub_list_to_empty;
CREATE PUBLICATION pg19_pub_empty_to_all FOR ALL TABLES;
CREATE PUBLICATION pg19_pub_list_to_all FOR ALL TABLES;
CREATE PUBLICATION pg19_pub_rebuilt_member FOR TABLE test_schema.tbl_pg19_pub_rebuilt, test_schema.tbl_pg19_pub_orders;
CREATE PUBLICATION pg19_pub_only_parent_list FOR TABLE ONLY test_schema.tbl_pg19_pub_parent, test_schema.tbl_pg19_pub_orders;

CREATE PUBLICATION pg19_pub_filter_change FOR TABLE test_schema.tbl_pg19_pub_items (id, note) WHERE (qty > 10);
CREATE PUBLICATION pg19_pub_filter_drop FOR TABLE test_schema.tbl_pg19_pub_items, test_schema."tbl_pg19_pub_Mixed";
CREATE PUBLICATION pg19_pub_list_to_schema FOR TABLES IN SCHEMA shared_schema;
CREATE PUBLICATION pg19_pub_schema_to_mixed FOR TABLE test_schema.tbl_pg19_pub_orders, TABLES IN SCHEMA shared_schema;
CREATE PUBLICATION pg19_pub_schema_to_empty;
CREATE PUBLICATION pg19_pub_viaroot_on FOR TABLE test_schema.tbl_pg19_pub_events WHERE (kind <> 'x') WITH (publish_via_partition_root = true);
CREATE PUBLICATION pg19_pub_viaroot_off FOR TABLE test_schema.tbl_pg19_pub_events;
CREATE PUBLICATION pg19_pub_only_parent_filter FOR TABLE ONLY test_schema.tbl_pg19_pub_parent WHERE (id > 0);
CREATE PUBLICATION pg19_pub_parent_filter_to_only FOR TABLE test_schema.tbl_pg19_pub_parent WHERE (id > 0);

CREATE PUBLICATION pg19_pub_gencols_on FOR TABLE test_schema.tbl_pg19_pub_items WITH (publish_generated_columns = stored);
CREATE PUBLICATION pg19_pub_gencols_off FOR TABLE test_schema.tbl_pg19_pub_items;

CREATE PUBLICATION pg19_pub_seq_unchanged FOR ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_seq_add FOR ALL TABLES, ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_seq_remove FOR ALL TABLES;
CREATE PUBLICATION pg19_pub_seq_to_tables FOR ALL TABLES WITH (publish = 'insert, update');
COMMENT ON PUBLICATION pg19_pub_seq_to_tables IS 'switched from FOR ALL SEQUENCES in place';
CREATE PUBLICATION pg19_pub_seq_to_list FOR TABLE test_schema.tbl_pg19_pub_items;
CREATE PUBLICATION pg19_pub_list_to_seq FOR ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_seq_to_only FOR ALL SEQUENCES WITH (publish = 'insert, update');
CREATE PUBLICATION pg19_pub_except_unchanged FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_items);
CREATE PUBLICATION pg19_pub_except_change FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_orders, TABLE test_schema."tbl_pg19_pub_Mixed");
CREATE PUBLICATION pg19_pub_except_clear FOR ALL TABLES, ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_except_add FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_items);
CREATE PUBLICATION pg19_pub_except_rebuilt FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_rebuilt);
CREATE PUBLICATION pg19_pub_except_to_only FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_events), ALL SEQUENCES;
CREATE PUBLICATION pg19_pub_mixed_to_except FOR ALL TABLES EXCEPT (TABLE test_schema.tbl_pg19_pub_orders), ALL SEQUENCES;

-- reg-misc: see schema_a.sql. The parent's v is bigint here.
CREATE TABLE test_schema.rv_regmisc_pubtbl_pt (id integer NOT NULL, v bigint) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_regmisc_pubtbl_pt1 PARTITION OF test_schema.rv_regmisc_pubtbl_pt FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_regmisc_pubtbl_other (id integer PRIMARY KEY);
CREATE PUBLICATION rv_regmisc_pub_list_to_empty;
CREATE PUBLICATION rv_regmisc_pub_list_to_all FOR ALL TABLES;
CREATE PUBLICATION rv_regmisc_pub_list_to_except FOR ALL TABLES EXCEPT (TABLE test_schema.rv_regmisc_pubtbl_other);
CREATE PUBLICATION rv_regmisc_pub_unchanged FOR TABLE test_schema.rv_regmisc_pubtbl_pt1, test_schema.rv_regmisc_pubtbl_other;

-- pre-grants (rv_pgrants_*): see schema_a.sql.
CREATE TABLE test_schema.rv_pgrants_pubtbl (id integer PRIMARY KEY, code varchar(20), note text); -- MODIFIED: qty dropped, code retyped
CREATE TABLE test_schema.rv_pgrants_pubfilter (id integer PRIMARY KEY, qty bigint);              -- MODIFIED: qty bigint
CREATE UNLOGGED TABLE test_schema.rv_pgrants_publog (id integer PRIMARY KEY);                    -- MODIFIED: unlogged
CREATE TABLE test_schema.rv_pgrants_pubgen (id integer PRIMARY KEY, qty integer, total bigint GENERATED ALWAYS AS (qty * 3) STORED); -- MODIFIED
CREATE UNLOGGED TABLE test_schema.rv_pgrants_pubexc_log (id integer PRIMARY KEY);                -- MODIFIED: unlogged
CREATE TABLE test_schema.rv_pgrants_pubexc_parent (id integer NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pgrants_pubexc_part PARTITION OF test_schema.rv_pgrants_pubexc_parent
    FOR VALUES FROM (0) TO (100);                                                                -- MODIFIED: a partition
CREATE PUBLICATION rv_pgrants_pub_collist FOR TABLE test_schema.rv_pgrants_pubtbl (id, code);    -- MODIFIED
CREATE PUBLICATION rv_pgrants_pub_collist_same FOR TABLE test_schema.rv_pgrants_pubtbl (id, code);
CREATE PUBLICATION rv_pgrants_pub_filter FOR TABLE test_schema.rv_pgrants_pubfilter WHERE (qty > 0);
CREATE PUBLICATION rv_pgrants_pub_unlogged FOR TABLE test_schema.rv_pgrants_pubfilter;           -- MODIFIED
CREATE PUBLICATION rv_pgrants_pub_gen FOR TABLE test_schema.rv_pgrants_pubgen (id, total);
CREATE PUBLICATION rv_pgrants_pub_except FOR ALL TABLES;                                          -- MODIFIED
CREATE SUBSCRIPTION rv_pgrants_sub_setpub
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_pgrants_remote_p1, rv_pgrants_remote_p2                                        -- MODIFIED
    WITH (connect = false, slot_name = NONE);
CREATE SUBSCRIPTION rv_pgrants_sub_quoted
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION "RvPgrants_Remote", "rv pgrants remote", "select"                                 -- MODIFIED
    WITH (connect = false, slot_name = NONE);

-- ==== END pg19:publications ====

-- ==== BEGIN pg19:subscriptions-fdw — subscriptions (SERVER, retain_dead_tuples, max_retention_duration, wal_receiver_timeout) and FDW CONNECTION ====

-- =============================================================================
-- PG19 feature test: subscriptions (SERVER source, retain_dead_tuples,
-- max_retention_duration, wal_receiver_timeout) and FDW CONNECTION functions
-- (TO side)
-- =============================================================================
-- Note: Requires PostgreSQL 19+. See schema_a.sql for the case list; the
-- PG19-only statements fail harmlessly on PG14-18 on both sides alike.
-- Expected diff on PG19 (in this order):
--   DROP SUBSCRIPTION pg19_subfdw_sub_from_only (before every other section)
--   ALTER FOREIGN DATA WRAPPER pg19_subfdw_fdw_added CONNECTION ...
--   ALTER FOREIGN DATA WRAPPER pg19_subfdw_fdw_removed NO CONNECTION
--   CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_to_only ... CONNECTION ...
--   CREATE SERVER pg19_subfdw_srv_b + its user mapping
--   ALTER SUBSCRIPTION ... SERVER / CONNECTION / SET (...), with OWNER TO
--     ahead of the SERVER switch of pg19_subfdw_sub_owner_switch
--   DROP SERVER pg19_subfdw_srv_from_only, then
--   DROP FOREIGN DATA WRAPPER pg19_subfdw_fdw_from_only
CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_same
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
ALTER FOREIGN DATA WRAPPER pg19_subfdw_fdw_same CONNECTION public.postgres_fdw_connection;

-- MODIFIED: gains a connection function
CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_added
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
ALTER FOREIGN DATA WRAPPER pg19_subfdw_fdw_added CONNECTION public.postgres_fdw_connection;

-- MODIFIED: loses its connection function
CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_removed
    HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;

-- ADDED: connection function only (NO HANDLER NO VALIDATOR CONNECTION ...)
CREATE FOREIGN DATA WRAPPER pg19_subfdw_fdw_to_only
    CONNECTION public.postgres_fdw_connection;

CREATE SERVER pg19_subfdw_srv_a
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_a');
CREATE USER MAPPING FOR PUBLIC SERVER pg19_subfdw_srv_a OPTIONS (user 'pgc_repl');

-- ADDED: on pg19_subfdw_fdw_added, whose CONNECTION the migration adds
CREATE SERVER pg19_subfdw_srv_b
    FOREIGN DATA WRAPPER pg19_subfdw_fdw_added
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_b');
CREATE USER MAPPING FOR PUBLIC SERVER pg19_subfdw_srv_b OPTIONS (user 'pgc_repl');

-- MODIFIED: CONNECTION -> SERVER pg19_subfdw_srv_a
CREATE SUBSCRIPTION pg19_subfdw_sub_to_server
    SERVER pg19_subfdw_srv_a
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);

-- MODIFIED: SERVER pg19_subfdw_srv_from_only -> SERVER pg19_subfdw_srv_b
CREATE SUBSCRIPTION pg19_subfdw_sub_switch
    SERVER pg19_subfdw_srv_b
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);

-- MODIFIED: SERVER pg19_subfdw_srv_a -> CONNECTION
CREATE SUBSCRIPTION pg19_subfdw_sub_to_conn
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE);

-- MODIFIED: retain_dead_tuples off, max_retention_duration 5000, unreachable host
--   (expect SET (retain_dead_tuples = false, ...) BEFORE the CONNECTION change)
CREATE SUBSCRIPTION pg19_subfdw_rdt_off
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, max_retention_duration = 5000);

-- MODIFIED: retain_dead_tuples on, conninfo pointing at this server
--   (expect the CONNECTION change BEFORE SET (retain_dead_tuples = true))
CREATE SUBSCRIPTION pg19_subfdw_rdt_on
    CONNECTION 'host=127.0.0.1 port=5432 dbname=postgres user=postgres'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, retain_dead_tuples = true);

-- MODIFIED: '30s' -> '10s'
CREATE SUBSCRIPTION pg19_subfdw_wrt_changed
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '10s');
-- MODIFIED: '45s' -> default. The explicit '-1' keeps this side PG19-only too.
CREATE SUBSCRIPTION pg19_subfdw_wrt_reset
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '-1');
-- MODIFIED: default -> '2min', max_retention_duration 0 -> 10000
CREATE SUBSCRIPTION pg19_subfdw_wrt_set
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '2min', max_retention_duration = 10000);
-- UNCHANGED
CREATE SUBSCRIPTION pg19_subfdw_wrt_same
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION pgc_pg19_remote_pub
    WITH (connect = false, slot_name = NONE, wal_receiver_timeout = '1min');

-- MODIFIED: owner postgres -> pgc_pg19_subfdw_owner and SERVER
--   pg19_subfdw_srv_a -> pg19_subfdw_srv_owner, which maps only the new owner
--   (expect OWNER TO BEFORE ALTER SUBSCRIPTION ... SERVER)
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
ALTER SUBSCRIPTION pg19_subfdw_sub_owner_switch OWNER TO pgc_pg19_subfdw_owner;
ALTER SUBSCRIPTION pg19_subfdw_sub_owner_switch SERVER pg19_subfdw_srv_owner;

-- r3-constraints (rv_r3c_*): a foreign server that changes its wrapper (every
-- version). ALTER SERVER cannot change it, so it is dropped and created again
-- — without CASCADE, which silently took its user mapping, the foreign table
-- on it (with its grant and comment) and the view over that. The migration
-- drops the view and the foreign table first, and creates the server, its
-- user mapping, the foreign table and the view again.
-- TO: rv_r3c_srv on rv_r3c_fdw_b (FROM: rv_r3c_fdw_a); everything else is the same.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_r3c_reader') THEN
        CREATE ROLE pgc_pg19_r3c_reader;
    END IF;
END
$$;
CREATE FOREIGN DATA WRAPPER rv_r3c_fdw_a HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
CREATE FOREIGN DATA WRAPPER rv_r3c_fdw_b HANDLER public.postgres_fdw_handler VALIDATOR public.postgres_fdw_validator;
CREATE SERVER rv_r3c_srv FOREIGN DATA WRAPPER rv_r3c_fdw_b
    OPTIONS (host '127.0.0.1', port '1', dbname 'pgc_nowhere_r3c');
COMMENT ON SERVER rv_r3c_srv IS 'rv_r3c server';
CREATE USER MAPPING FOR PUBLIC SERVER rv_r3c_srv OPTIONS (user 'pgc_repl');
CREATE FOREIGN TABLE test_schema.rv_r3c_ft (id integer, label text)
    SERVER rv_r3c_srv OPTIONS (table_name 'rv_r3c_remote');
COMMENT ON FOREIGN TABLE test_schema.rv_r3c_ft IS 'rv_r3c foreign table';
GRANT SELECT ON test_schema.rv_r3c_ft TO pgc_pg19_r3c_reader;
CREATE VIEW test_schema.rv_r3c_ft_view AS SELECT id, label FROM test_schema.rv_r3c_ft;
-- Review fixes: see schema_a.sql (the same grants and foreign partition).
GRANT USAGE ON FOREIGN SERVER rv_r3c_srv TO pgc_pg19_r3c_reader;
GRANT UPDATE (label) ON test_schema.rv_r3c_ft TO pgc_pg19_r3c_reader;
CREATE TABLE test_schema.rv_r3c_fp (id integer, label text) PARTITION BY RANGE (id);
CREATE FOREIGN TABLE test_schema.rv_r3c_fp_remote PARTITION OF test_schema.rv_r3c_fp
    FOR VALUES FROM (0) TO (100) SERVER rv_r3c_srv OPTIONS (table_name 'rv_r3c_fp_remote');

-- r4-sub (rv_r4s_*): replication slots, every version (see schema_a.sql).
-- Expected diff: ALTER SUBSCRIPTION ... SET (slot_name = NONE / '...') for
-- the three changed slots, and CREATE SUBSCRIPTION ... WITH (connect = false,
-- ...) for the TO-only ones, none of which contacts the (unreachable)
-- publisher.
-- MODIFIED: its own slot -> NONE
CREATE SUBSCRIPTION rv_r4s_sub_own_to_none
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = NONE);
-- MODIFIED: NONE -> rv_r4s_named_slot
CREATE SUBSCRIPTION rv_r4s_sub_none_to_named
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = 'rv_r4s_named_slot');
-- MODIFIED: rv_r4s_old_slot -> its own slot
CREATE SUBSCRIPTION rv_r4s_sub_named_to_own
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false);
-- UNCHANGED
CREATE SUBSCRIPTION rv_r4s_sub_same_named
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = 'rv_r4s_kept_slot');
-- ADDED: no slot -> WITH (connect = false, slot_name = NONE, enabled = false)
CREATE SUBSCRIPTION rv_r4s_sub_to_only_none
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = NONE);
-- ADDED: its own slot -> WITH (connect = false, enabled = false)
CREATE SUBSCRIPTION rv_r4s_sub_to_only_own
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false);
-- ADDED: another slot, with a comment -> WITH (connect = false,
--   slot_name = 'rv_r4s_other_slot', enabled = false)
CREATE SUBSCRIPTION rv_r4s_sub_to_only_named
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = 'rv_r4s_other_slot');
COMMENT ON SUBSCRIPTION rv_r4s_sub_to_only_named IS 'rv_r4s TO-only subscription';
-- ADDED (PG19+; a syntax error on PG14-18): a SERVER subscription with the
--   PG19 options. retain_dead_tuples = true does not contact the publisher
--   either under connect = false.
CREATE SUBSCRIPTION rv_r4s_sub_to_only_server
    SERVER pg19_subfdw_srv_a
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false, slot_name = NONE, retain_dead_tuples = true,
          max_retention_duration = 1000, wal_receiver_timeout = '30s');
-- r4-sub review: TO slots PostgreSQL refuses as a slot_name (see
-- schema_a.sql). Expected diff: nothing for the two MODIFIED ones, whose
-- slots the migration leaves out (a note under USE_COMMENTS=true, a warning
-- on stderr), and CREATE SUBSCRIPTION ... WITH (connect = false, enabled =
-- false) for the ADDED one, which records its own name unchecked.
-- MODIFIED, left out: rv_r4s_upper_slot -> its own slot rv_r4s_Sub_Upper
CREATE SUBSCRIPTION "rv_r4s_Sub_Upper"
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false);
-- MODIFIED, left out: NONE -> a slot named none (created under the name
--   "none", whose own slot it records, then renamed)
CREATE SUBSCRIPTION "none"
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false);
ALTER SUBSCRIPTION "none" RENAME TO rv_r4s_sub_none_alias;
-- ADDED: its own slot, which is no valid slot name -> WITH (connect = false,
--   enabled = false), never connected
CREATE SUBSCRIPTION "rv_r4s_Sub_To_Only_Upper"
    CONNECTION 'host=127.0.0.1 port=1 dbname=pgc_nowhere'
    PUBLICATION rv_r4s_remote_pub
    WITH (connect = false);

-- ==== END pg19:subscriptions-fdw ====

-- ==== BEGIN pg19:routines — routines: aggregate SUPPORT / PARALLEL / negative SSPACE, SET <list GUC> TO NULL ====

-- -----------------------------------------------------------------------------
-- Aggregate modify path (all versions). See schema_a.sql.
-- -----------------------------------------------------------------------------
CREATE AGGREGATE test_schema.pg19_routines_agg_parallel(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    COMBINEFUNC = int8pl,
    INITCOND = '0',
    PARALLEL = SAFE
);

CREATE AGGREGATE test_schema.pg19_routines_agg_initcond(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '1'
);
CREATE VIEW test_schema.pg19_routines_agg_initcond_v AS
SELECT test_schema.pg19_routines_agg_initcond(v.x) AS n
FROM (VALUES (1::bigint), (2::bigint)) AS v(x);

CREATE AGGREGATE test_schema.pg19_routines_agg_sspace(integer) (
    SFUNC = int4pl,
    STYPE = integer,
    SSPACE = -1
);

CREATE AGGREGATE test_schema.pg19_routines_agg_sspace_reset(integer) (
    SFUNC = int4pl,
    STYPE = integer
);

CREATE AGGREGATE test_schema.pg19_routines_agg_msspace(integer) (
    SFUNC = int4pl,
    STYPE = integer,
    MSFUNC = int4pl,
    MINVFUNC = int4mi,
    MSTYPE = integer,
    MSSPACE = -1
);

-- TO-only aggregate with a negative SSPACE -> created with it.
CREATE AGGREGATE test_schema.pg19_routines_agg_sspace_new(integer) (
    SFUNC = int4pl,
    STYPE = integer,
    SSPACE = -1
);

CREATE AGGREGATE test_schema.pg19_routines_agg_kind(VARIADIC "any" ORDER BY VARIADIC "any") (
    SFUNC = ordered_set_transition_multi,
    STYPE = internal,
    FINALFUNC = rank_final,
    FINALFUNC_EXTRA,
    HYPOTHETICAL
);

CREATE AGGREGATE test_schema.pg19_routines_fn_to_agg(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0'
);

-- -----------------------------------------------------------------------------
-- Aggregate SUPPORT function (PG19+). See schema_a.sql; ignored with a WARNING on
-- PG14-18.
-- -----------------------------------------------------------------------------
CREATE AGGREGATE test_schema.pg19_routines_support_add(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0',
    SUPPORT = int8inc_support
);
CREATE AGGREGATE test_schema.pg19_routines_support_drop(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0'
);
CREATE AGGREGATE test_schema.pg19_routines_support_same(*) (
    SFUNC = int8inc,
    STYPE = bigint,
    INITCOND = '0',
    SUPPORT = int8inc_support
);
-- TO only -> created with SUPPORT and PARALLEL.
CREATE AGGREGATE test_schema.pg19_routines_support_new(bigint) (
    SFUNC = int8inc_any,
    STYPE = bigint,
    INITCOND = '0',
    SUPPORT = int8inc_support,
    PARALLEL = SAFE
);

-- -----------------------------------------------------------------------------
-- Routine SET clauses for GUC_LIST_QUOTE parameters. See schema_a.sql.
-- -----------------------------------------------------------------------------
-- Must be emitted as SET search_path TO '' (not = "").
CREATE FUNCTION test_schema.pg19_routines_guc_empty_string()
RETURNS integer
LANGUAGE sql
SET search_path = ''
AS $$ SELECT 1 $$;

CREATE FUNCTION test_schema.pg19_routines_guc_empty_element()
RETURNS integer
LANGUAGE sql
SET search_path = '', public
AS $$ SELECT 1 $$;

CREATE FUNCTION test_schema.pg19_routines_guc_long_element()
RETURNS integer
LANGUAGE sql
SET search_path = 'pg19_routines_a_schema_name_that_is_longer_than_sixty_three_bytes_x', public
AS $$ SELECT 1 $$;

CREATE FUNCTION test_schema.pg19_routines_guc_output_plugins()
RETURNS integer
LANGUAGE sql
SET output_plugin_libraries = pgoutput, test_decoding
AS $$ SELECT 1 $$;

-- PG19: Requires PostgreSQL 19+ (TO NULL is a syntax error on PG14-18).
CREATE FUNCTION test_schema.pg19_routines_guc_to_null()
RETURNS integer
LANGUAGE sql
SET search_path TO NULL
AS $$ SELECT 1 $$;

CREATE FUNCTION test_schema.pg19_routines_guc_from_null()
RETURNS integer
LANGUAGE sql
SET search_path = public
AS $$ SELECT 1 $$;

-- PG19: TO-only procedure with an empty temp_tablespaces list (create path).
CREATE PROCEDURE test_schema.pg19_routines_guc_null_proc()
LANGUAGE sql
SET temp_tablespaces TO NULL
AS $$ SELECT 1 $$;

-- -----------------------------------------------------------------------------
-- Aggregate SORTOP (all versions). See schema_a.sql.
-- -----------------------------------------------------------------------------
-- Must be emitted as SORTOP = OPERATOR(pg_catalog.>).
CREATE AGGREGATE test_schema.pg19_routines_agg_sortop(integer) (
    SFUNC = int4larger,
    STYPE = integer,
    SORTOP = >
);

-- TO only -> created with a sort operator.
CREATE AGGREGATE test_schema.pg19_routines_agg_sortop_new(integer) (
    SFUNC = int4smaller,
    STYPE = integer,
    SORTOP = <
);

-- -----------------------------------------------------------------------------
-- Numeric elements of a list GUC (all versions). See schema_a.sql.
-- -----------------------------------------------------------------------------
-- Must be emitted verbatim: SET search_path = public, 42.
CREATE FUNCTION test_schema.pg19_routines_guc_numeric()
RETURNS integer
LANGUAGE sql
SET search_path = public, 42
AS $$ SELECT 1 $$;

-- Must be emitted as SET search_path TO '', 7.
CREATE FUNCTION test_schema.pg19_routines_guc_numeric_mixed()
RETURNS integer
LANGUAGE sql
SET search_path = '', 7
AS $$ SELECT 1 $$;

-- -----------------------------------------------------------------------------
-- Privileges of a routine the migration drops and recreates. See schema_a.sql.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgc_pg19_routines_role') THEN
          CREATE ROLE pgc_pg19_routines_role;
     END IF;
END;
$$;

CREATE AGGREGATE test_schema.pg19_routines_grant_kind(bigint) (
    SFUNC = int8pl,
    STYPE = bigint
);
GRANT EXECUTE ON FUNCTION test_schema.pg19_routines_grant_kind(bigint) TO pgc_pg19_routines_role;

CREATE FUNCTION test_schema.pg19_routines_grant_revoked(bigint)
RETURNS integer
LANGUAGE sql
AS $$ SELECT 1 $$;
REVOKE EXECUTE ON FUNCTION test_schema.pg19_routines_grant_revoked(bigint) FROM PUBLIC;

-- ==== END pg19:routines ====

-- ==== BEGIN pg19:tables — tables: CHECK ALTER CONSTRAINT [NOT] ENFORCED, generated column SET EXPRESSION ====
-- Object names use the wp19_tables_ prefix rather than pg19_: pgc's table
-- query filters `tablename NOT LIKE 'pg_%'` with an unescaped `_`, which
-- silently drops every table whose name starts with "pg" plus any character.
-- -----------------------------------------------------------------------------
-- CHECK constraints: ALTER CONSTRAINT ... [NOT] ENFORCED (PG19+)
-- -----------------------------------------------------------------------------
-- TO: every enforceability flipped. Expected pgc output on PG19:
--   alter table test_schema.wp19_tables_ck alter constraint wp19_tables_ck_notvalid not enforced;
--   alter table test_schema.wp19_tables_ck alter constraint wp19_tables_ck_relax not enforced;
--   alter table test_schema.wp19_tables_ck alter constraint wp19_tables_ck_tighten enforced;
--   alter table test_schema.wp19_tables_ck_parent alter constraint wp19_tables_ck_amount not enforced;
--   alter table test_schema.wp19_tables_ck_child alter constraint wp19_tables_ck_amount not enforced;
--   alter table test_schema.wp19_tables_ck_part alter constraint wp19_tables_ck_part_amount enforced;
-- On PG14-18 the ALTER CONSTRAINT statements below fail (ON_ERROR_STOP=0),
-- leaving TO identical to FROM for this part.
CREATE TABLE test_schema.wp19_tables_ck (
    id    integer PRIMARY KEY,
    qty   integer,
    price numeric
);
ALTER TABLE test_schema.wp19_tables_ck
    ADD CONSTRAINT wp19_tables_ck_relax CHECK (qty >= 0);
ALTER TABLE test_schema.wp19_tables_ck
    ALTER CONSTRAINT wp19_tables_ck_relax NOT ENFORCED;
ALTER TABLE test_schema.wp19_tables_ck
    ADD CONSTRAINT wp19_tables_ck_tighten CHECK (price > 0);
ALTER TABLE test_schema.wp19_tables_ck
    ADD CONSTRAINT wp19_tables_ck_notvalid CHECK (qty < 1000000) NOT VALID;
ALTER TABLE test_schema.wp19_tables_ck
    ALTER CONSTRAINT wp19_tables_ck_notvalid NOT ENFORCED;

CREATE TABLE test_schema.wp19_tables_ck_parent (
    id     integer,
    amount integer,
    CONSTRAINT wp19_tables_ck_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.wp19_tables_ck_child (
    CONSTRAINT wp19_tables_ck_amount CHECK (amount > 0)
) INHERITS (test_schema.wp19_tables_ck_parent);
ALTER TABLE test_schema.wp19_tables_ck_parent
    ALTER CONSTRAINT wp19_tables_ck_amount NOT ENFORCED;

CREATE TABLE test_schema.wp19_tables_ck_part (
    id     integer NOT NULL,
    amount integer,
    CONSTRAINT wp19_tables_ck_part_amount CHECK (amount >= 0)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.wp19_tables_ck_part_1
    PARTITION OF test_schema.wp19_tables_ck_part FOR VALUES FROM (0) TO (1000);

-- -----------------------------------------------------------------------------
-- Generated columns: ALTER COLUMN ... SET EXPRESSION (STORED PG17+, VIRTUAL PG19+)
-- -----------------------------------------------------------------------------
-- TO: expressions changed (qty * 2) -> (qty * 3); everything else identical.
-- Expected on PG19 (no view or column is dropped):
--   alter table only test_schema.wp19_tables_gen_stored alter column total set expression as (qty * 3);
--   alter table only test_schema.wp19_tables_gen_virtual alter column doubled set expression as (qty * 3);
--   alter table only test_schema.wp19_tables_gen_virtual_nochk alter column doubled set expression as (qty * 3);
-- and DROP COLUMN + ADD COLUMN for test_schema.wp19_tables_gen_published.
CREATE TABLE test_schema.wp19_tables_gen_stored (
    id    integer PRIMARY KEY,
    qty   integer NOT NULL,
    total integer GENERATED ALWAYS AS (qty * 3) STORED
);
CREATE INDEX wp19_tables_gen_stored_total_idx
    ON test_schema.wp19_tables_gen_stored (total);
CREATE VIEW test_schema.wp19_tables_gen_stored_v AS
    SELECT id, total FROM test_schema.wp19_tables_gen_stored;

-- Note: Requires PostgreSQL 18+ (fails harmlessly on PG14-17).
CREATE TABLE test_schema.wp19_tables_gen_virtual (
    id      integer PRIMARY KEY,
    qty     integer NOT NULL CONSTRAINT wp19_tables_gen_virtual_qty_check CHECK (qty >= 0),
    doubled integer GENERATED ALWAYS AS (qty * 3) VIRTUAL
        CONSTRAINT wp19_tables_gen_virtual_doubled_check CHECK (doubled < 1000000)
);
CREATE VIEW test_schema.wp19_tables_gen_virtual_v AS
    SELECT id, doubled FROM test_schema.wp19_tables_gen_virtual;

CREATE TABLE test_schema.wp19_tables_gen_virtual_nochk (
    id      integer PRIMARY KEY,
    qty     integer NOT NULL,
    doubled integer GENERATED ALWAYS AS (qty * 3) VIRTUAL
);

CREATE TABLE test_schema.wp19_tables_gen_published (
    id      integer PRIMARY KEY,
    qty     integer NOT NULL,
    doubled integer GENERATED ALWAYS AS (qty * 3) VIRTUAL
);
CREATE PUBLICATION wp19_tables_pub FOR TABLE test_schema.wp19_tables_gen_published;

-- -----------------------------------------------------------------------------
-- Classical inheritance ordering (every version)
-- -----------------------------------------------------------------------------
-- TO-only family whose child sorts first: CREATE TABLE ... INHERITS fails
-- before the parent exists, so pgc must create the parent first.
CREATE TABLE test_schema.wp19_tables_inh_z_parent (id integer);
CREATE TABLE test_schema.wp19_tables_inh_a_child (note text)
    INHERITS (test_schema.wp19_tables_inh_z_parent);

-- -----------------------------------------------------------------------------
-- Generated-column changes that interact with the rest of the table
-- -----------------------------------------------------------------------------
-- Every version: drop wp19_tables_rc_g_check, DROP/ADD COLUMN g, re-add the
-- CHECK (NOT ENFORCED on PG19, where the ALTER CONSTRAINT below applies).
CREATE TABLE test_schema.wp19_tables_rc (
    id integer PRIMARY KEY,
    g  integer GENERATED ALWAYS AS (id * 2) STORED,
    CONSTRAINT wp19_tables_rc_g_check CHECK (g > 0)
);
ALTER TABLE test_schema.wp19_tables_rc
    ALTER CONSTRAINT wp19_tables_rc_g_check NOT ENFORCED;

CREATE TABLE test_schema.wp19_tables_gen_retype (
    id    integer PRIMARY KEY,
    stamp date GENERATED ALWAYS AS ('2020-01-01'::date + id) STORED
);

-- Expected on PG17+, in this order:
--   alter table test_schema.wp19_tables_gen_newcol add column bonus integer;
--   alter table only test_schema.wp19_tables_gen_newcol alter column total set expression as ((qty * 2) + (bonus * 3));
--   alter table test_schema.wp19_tables_gen_newcol drop column legacy;
CREATE TABLE test_schema.wp19_tables_gen_newcol (
    id    integer PRIMARY KEY,
    qty   integer,
    total integer GENERATED ALWAYS AS ((qty * 2) + (bonus * 3)) STORED,
    bonus integer
);
CREATE TABLE test_schema.wp19_tables_gen_newcol_p (
    id    integer PRIMARY KEY,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 2 + bonus) STORED,
    bonus integer
);
-- TO-only, created with the same kind of expression.
CREATE TABLE test_schema.wp19_tables_gen_parens (
    a integer,
    b integer,
    g integer GENERATED ALWAYS AS ((a * 2) + (b * 3)) STORED
);

-- PG19 expected: drop + re-add of wp19_tables_keep_amount on the parent
-- only; the child's copy stays enforced.
CREATE TABLE test_schema.wp19_tables_keep_parent (
    id     integer,
    amount integer,
    CONSTRAINT wp19_tables_keep_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.wp19_tables_keep_child (
    CONSTRAINT wp19_tables_keep_amount CHECK (amount > 0)
) INHERITS (test_schema.wp19_tables_keep_parent);
ALTER TABLE test_schema.wp19_tables_keep_parent
    ALTER CONSTRAINT wp19_tables_keep_amount NOT ENFORCED;
ALTER TABLE test_schema.wp19_tables_keep_child
    ALTER CONSTRAINT wp19_tables_keep_amount ENFORCED;

CREATE TABLE test_schema.wp19_tables_gen_vpart (
    id integer NOT NULL,
    v  integer GENERATED ALWAYS AS (id * 3) VIRTUAL
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.wp19_tables_gen_vpart_1
    PARTITION OF test_schema.wp19_tables_gen_vpart FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.wp19_tables_gen_vpart_2
    PARTITION OF test_schema.wp19_tables_gen_vpart (
        CONSTRAINT wp19_tables_gen_vpart_2_check CHECK (id >= 100)
    ) FOR VALUES FROM (100) TO (200);

CREATE TABLE test_schema.wp19_tables_gen_part (
    id    integer NOT NULL,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 3) STORED
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.wp19_tables_gen_part_1
    PARTITION OF test_schema.wp19_tables_gen_part (
        CONSTRAINT wp19_tables_gen_part_1_total_check CHECK (total > -1)
    ) FOR VALUES FROM (0) TO (100);

-- Expected on PG17+ (nothing for wp19_tables_gen_inh_own):
--   alter table only test_schema.wp19_tables_gen_inh alter column total set expression as (qty * 3);
--   alter table only test_schema.wp19_tables_gen_inh_follow alter column total set expression as (qty * 3);
--   alter table only test_schema.wp19_tables_gen_inh_merged alter column total set expression as (qty * 3);
CREATE TABLE test_schema.wp19_tables_gen_inh (
    id    integer,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 3) STORED
);
CREATE TABLE test_schema.wp19_tables_gen_inh_follow (
    CONSTRAINT wp19_tables_gen_inh_follow_check CHECK (total > -1)
) INHERITS (test_schema.wp19_tables_gen_inh);
CREATE INDEX wp19_tables_gen_inh_follow_total_idx
    ON test_schema.wp19_tables_gen_inh_follow (total);
CREATE VIEW test_schema.wp19_tables_gen_inh_follow_v AS
    SELECT id, total FROM test_schema.wp19_tables_gen_inh_follow;
CREATE TABLE test_schema.wp19_tables_gen_inh_merged (
    id    integer,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 3) STORED
) INHERITS (test_schema.wp19_tables_gen_inh);
CREATE TABLE test_schema.wp19_tables_gen_inh_own (
    id    integer,
    qty   integer,
    total integer GENERATED ALWAYS AS (qty * 5) STORED
) INHERITS (test_schema.wp19_tables_gen_inh);

CREATE TABLE test_schema.wp19_tables_gen_vinh (
    id      integer,
    qty     integer,
    doubled integer GENERATED ALWAYS AS (qty * 3) VIRTUAL
);
CREATE TABLE test_schema.wp19_tables_gen_vinh_c ()
    INHERITS (test_schema.wp19_tables_gen_vinh);

-- reg-misc: CHECK enforceability flips next to a column rewrite (see
-- schema_a.sql). NOT ENFORCED is added separately, so PG14-17 keep the table.
CREATE TABLE test_schema.rv_regmisc_ck_expr (
    id integer PRIMARY KEY,
    c  integer,
    g  integer GENERATED ALWAYS AS (c * 3) STORED
);
ALTER TABLE test_schema.rv_regmisc_ck_expr
    ADD CONSTRAINT rv_regmisc_ck_expr_g CHECK (g > -5) NOT ENFORCED;
CREATE TABLE test_schema.rv_regmisc_ck_type (
    id integer PRIMARY KEY,
    c  numeric(6,0)
);
ALTER TABLE test_schema.rv_regmisc_ck_type
    ADD CONSTRAINT rv_regmisc_ck_type_c CHECK (c <> 1) NOT ENFORCED;
CREATE TABLE test_schema.rv_regmisc_ck_virt (
    id integer PRIMARY KEY,
    c  integer,
    g  integer GENERATED ALWAYS AS (c * 3) VIRTUAL
);
ALTER TABLE test_schema.rv_regmisc_ck_virt
    ADD CONSTRAINT rv_regmisc_ck_virt_g CHECK (g > -5) NOT ENFORCED;
CREATE TABLE test_schema.rv_regmisc_ck_tight_expr (
    id integer PRIMARY KEY,
    c  integer,
    g  integer GENERATED ALWAYS AS (c + 1) STORED,
    CONSTRAINT rv_regmisc_ck_tight_expr_g CHECK (g > -5)
);
CREATE TABLE test_schema.rv_regmisc_ck_tight_type (
    id integer PRIMARY KEY,
    c  numeric(6,0),
    CONSTRAINT rv_regmisc_ck_tight_type_c CHECK (c <> 0.6)
);
-- Partitions whose rows the parent's ALTER COLUMN ... TYPE rewrites.
CREATE TABLE test_schema.rv_regmisc_ckfam_p (
    id integer NOT NULL,
    c  numeric(6,0)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_regmisc_ckfam_p1 PARTITION OF test_schema.rv_regmisc_ckfam_p
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_regmisc_ckfam_p2 PARTITION OF test_schema.rv_regmisc_ckfam_p
    FOR VALUES FROM (100) TO (200);
ALTER TABLE test_schema.rv_regmisc_ckfam_p1
    ADD CONSTRAINT rv_regmisc_ckfam_p1_c CHECK (c <> 1) NOT ENFORCED;
ALTER TABLE test_schema.rv_regmisc_ckfam_p2
    ADD CONSTRAINT rv_regmisc_ckfam_p2_c CHECK (c <> 0.6);

-- pre-recreate: tables dropped and recreated with everything that went with
-- their DROP TABLE (see schema_a.sql).
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
CREATE TABLE test_schema.rv_recreate_orders (
    id        serial  NOT NULL,
    region_id integer NOT NULL
        CONSTRAINT rv_recreate_orders_region_fk REFERENCES test_schema.rv_recreate_regions (id),
    kind      integer NOT NULL,
    amount    numeric(10,2),
    CONSTRAINT rv_recreate_orders_pkey PRIMARY KEY (id, region_id, kind)
) PARTITION BY RANGE (region_id);
CREATE TABLE test_schema.rv_recreate_orders_p1 PARTITION OF test_schema.rv_recreate_orders
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_recreate_orders_p2 PARTITION OF test_schema.rv_recreate_orders
    FOR VALUES FROM (100) TO (300) PARTITION BY LIST (kind);
CREATE TABLE test_schema.rv_recreate_orders_p2a PARTITION OF test_schema.rv_recreate_orders_p2
    FOR VALUES IN (1);
CREATE TABLE test_schema.rv_recreate_orders_p2b PARTITION OF test_schema.rv_recreate_orders_p2
    FOR VALUES IN (2);
CREATE TABLE test_schema.rv_recreate_orders_def PARTITION OF test_schema.rv_recreate_orders DEFAULT;
CREATE INDEX rv_recreate_orders_p1_amount_idx ON test_schema.rv_recreate_orders_p1 (amount);
COMMENT ON TABLE test_schema.rv_recreate_orders_p1 IS 'first hundred orders';
GRANT SELECT ON test_schema.rv_recreate_orders_p1 TO pgc_pg19_rv_recreate_reader;
GRANT USAGE ON SEQUENCE test_schema.rv_recreate_orders_id_seq TO pgc_pg19_rv_recreate_reader;
CREATE STATISTICS test_schema.rv_recreate_orders_p1_st ON id, amount FROM test_schema.rv_recreate_orders_p1;
CREATE TRIGGER rv_recreate_orders_touch BEFORE INSERT ON test_schema.rv_recreate_orders
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE TRIGGER rv_recreate_orders_p1_touch BEFORE UPDATE ON test_schema.rv_recreate_orders_p1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE VIEW test_schema.rv_recreate_orders_p1_v AS
    SELECT id, amount FROM test_schema.rv_recreate_orders_p1;
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
        REFERENCES test_schema.rv_recreate_orders (id, region_id, kind) ON DELETE CASCADE
);
CREATE TABLE test_schema.rv_recreate_p1_notes (
    id           integer PRIMARY KEY,
    order_id     integer,
    order_region integer,
    order_kind   integer,
    CONSTRAINT rv_recreate_p1_notes_order_fk FOREIGN KEY (order_id, order_region, order_kind)
        REFERENCES test_schema.rv_recreate_orders_p1 (id, region_id, kind)
);
CREATE TABLE test_schema.rv_recreate_events (
    id         bigserial NOT NULL,
    created_on date      NOT NULL,
    payload    text,
    CONSTRAINT rv_recreate_events_pkey PRIMARY KEY (id, created_on)
) PARTITION BY RANGE (created_on);
CREATE TABLE test_schema.rv_recreate_events_2024 PARTITION OF test_schema.rv_recreate_events
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE test_schema.rv_recreate_event_tags (
    id         integer PRIMARY KEY,
    event_id   bigint,
    event_date date,
    CONSTRAINT rv_recreate_event_tags_event_fk FOREIGN KEY (event_id, event_date)
        REFERENCES test_schema.rv_recreate_events (id, created_on)
);
-- Review fixes (see schema_a.sql).
CREATE TABLE test_schema.rv_recreate_aa_event_archive (
    id         bigint NOT NULL,
    created_on date   NOT NULL,
    payload    text
);
ALTER TABLE test_schema.rv_recreate_events ATTACH PARTITION test_schema.rv_recreate_aa_event_archive
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TRIGGER rv_recreate_orders_audit AFTER INSERT ON test_schema.rv_recreate_orders
    FOR EACH STATEMENT EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE TRIGGER rv_recreate_orders_audit AFTER INSERT ON test_schema.rv_recreate_orders_p1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE TABLE public.rv_recreate_pub_orders (
    id integer NOT NULL,
    k  integer NOT NULL,
    CONSTRAINT rv_recreate_pub_orders_pkey PRIMARY KEY (id, k)
) PARTITION BY RANGE (k);
CREATE TABLE public.rv_recreate_pub_orders_p1 PARTITION OF public.rv_recreate_pub_orders
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_recreate_pub_lines (
    id       integer PRIMARY KEY,
    order_id integer,
    order_k  integer,
    CONSTRAINT rv_recreate_pub_lines_order_fk FOREIGN KEY (order_id, order_k)
        REFERENCES public.rv_recreate_pub_orders (id, k)
);
CREATE TABLE test_schema.rv_recreate_tree (
    id integer NOT NULL,
    k  integer NOT NULL,
    CONSTRAINT rv_recreate_tree_pkey PRIMARY KEY (id, k)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_recreate_tree_c PARTITION OF test_schema.rv_recreate_tree
    FOR VALUES FROM (0) TO (100) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_recreate_tree_c1 PARTITION OF test_schema.rv_recreate_tree_c
    FOR VALUES IN (1);
CREATE TABLE test_schema.rv_recreate_tree_refs (
    id  integer PRIMARY KEY,
    tid integer,
    tk  integer,
    CONSTRAINT rv_recreate_tree_refs_fk FOREIGN KEY (tid, tk)
        REFERENCES test_schema.rv_recreate_tree (id, k)
);
CREATE TABLE test_schema."rv_recreate_Cased" (
    "Id" integer NOT NULL,
    "K"  integer NOT NULL,
    CONSTRAINT "rv_recreate_Cased_pkey" PRIMARY KEY ("Id", "K")
) PARTITION BY RANGE ("K");
CREATE TABLE test_schema."rv_recreate_Cased_P1" PARTITION OF test_schema."rv_recreate_Cased"
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema."rv_recreate_Cased_P2" PARTITION OF test_schema."rv_recreate_Cased"
    FOR VALUES FROM (100) TO (200);
CREATE TRIGGER "rv_recreate_Cased_Trg" BEFORE INSERT ON test_schema."rv_recreate_Cased"
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_recreate_touch();
CREATE TABLE test_schema."rv_recreate_CasedRefs" (
    "Id"      integer PRIMARY KEY,
    "OrderId" integer,
    "K"       integer,
    CONSTRAINT "rv_recreate_Cased_FK" FOREIGN KEY ("OrderId", "K")
        REFERENCES test_schema."rv_recreate_Cased" ("Id", "K")
);

-- pre-columns (rv_pcols_*): see schema_a.sql and data/test/README.md.
-- (1) The retyped column under a generated column.
CREATE TABLE test_schema.rv_pcols_gen (
    id   integer PRIMARY KEY,
    a    bigint NOT NULL,
    g    integer GENERATED ALWAYS AS (a * 2) STORED,
    flag integer,
    CONSTRAINT rv_pcols_gen_g_check CHECK (g >= 0)
);
CREATE INDEX rv_pcols_gen_g_idx ON test_schema.rv_pcols_gen (g);
CREATE STATISTICS test_schema.rv_pcols_gen_st (ndistinct) ON g, flag FROM test_schema.rv_pcols_gen;
CREATE VIEW test_schema.rv_pcols_gen_v AS SELECT id, g FROM test_schema.rv_pcols_gen;
CREATE TABLE test_schema.rv_pcols_gen_expr (
    id integer PRIMARY KEY,
    a  bigint,
    g  bigint GENERATED ALWAYS AS (a * 3) STORED
);
CREATE TABLE test_schema.rv_pcols_gen_plain (
    id integer PRIMARY KEY,
    a  bigint,
    g  integer
);
CREATE TABLE test_schema.rv_pcols_gen_gone (
    id integer PRIMARY KEY,
    a  bigint
);
CREATE TABLE test_schema.rv_pcols_gen_both (
    id integer PRIMARY KEY
);
CREATE TABLE test_schema.rv_pcols_gen_part (
    id integer NOT NULL,
    a  bigint,
    g  integer GENERATED ALWAYS AS (a * 10) STORED
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pcols_gen_part_1 PARTITION OF test_schema.rv_pcols_gen_part
    FOR VALUES FROM (0) TO (100);
CREATE INDEX rv_pcols_gen_part_g_idx ON test_schema.rv_pcols_gen_part (g);
CREATE TABLE test_schema.rv_pcols_gen_virt (
    id integer PRIMARY KEY,
    a  bigint,
    g  integer GENERATED ALWAYS AS (a * 2) VIRTUAL
);

-- (2) The same statistics on the columns in their declared order.
CREATE TABLE test_schema.rv_pcols_st (a integer, b integer, c integer);
CREATE STATISTICS test_schema.rv_pcols_st_same (ndistinct) ON b, c FROM test_schema.rv_pcols_st;
CREATE STATISTICS test_schema.rv_pcols_st_expr (ndistinct, mcv)
    ON c, lower(c::text), b FROM test_schema.rv_pcols_st;
CREATE STATISTICS test_schema.rv_pcols_st_changed (ndistinct) ON a, b FROM test_schema.rv_pcols_st;

-- (3) CHECK families (see schema_a.sql).
CREATE TABLE test_schema.rv_pcols_ckx_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckx_amount CHECK (amount > 1)
);
CREATE TABLE test_schema.rv_pcols_ckx_ch (
    CONSTRAINT rv_pcols_ckx_amount CHECK (amount > 1)
) INHERITS (test_schema.rv_pcols_ckx_par);
CREATE TABLE test_schema.rv_pcols_ckx_ch2 () INHERITS (test_schema.rv_pcols_ckx_par);
CREATE TABLE test_schema.rv_pcols_ckx_gch (
    CONSTRAINT rv_pcols_ckx_amount CHECK (amount > 1)
) INHERITS (test_schema.rv_pcols_ckx_ch);
CREATE TABLE test_schema.rv_pcols_cku_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_cku_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.rv_pcols_cku_ch () INHERITS (test_schema.rv_pcols_cku_par);
-- NOT ENFORCED at CREATE needs PostgreSQL 18: before, these tables exist
-- only in FROM and are dropped.
CREATE TABLE test_schema.rv_pcols_ckr_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckr_amount CHECK (amount > 0) NOT ENFORCED
);
CREATE TABLE test_schema.rv_pcols_ckr_ch (
    CONSTRAINT rv_pcols_ckr_amount CHECK (amount > 0) NOT ENFORCED
) INHERITS (test_schema.rv_pcols_ckr_par);
CREATE TABLE test_schema.rv_pcols_ckt_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckt_amount CHECK (amount > 0)
);
CREATE TABLE test_schema.rv_pcols_ckt_ch (
    CONSTRAINT rv_pcols_ckt_amount CHECK (amount > 0)
) INHERITS (test_schema.rv_pcols_ckt_par);
CREATE TABLE test_schema.rv_pcols_ckc_par (
    id     integer,
    amount integer,
    CONSTRAINT rv_pcols_ckc_amount CHECK (amount > 0) NOT ENFORCED
);
CREATE TABLE test_schema.rv_pcols_ckc_ch (
    CONSTRAINT rv_pcols_ckc_amount CHECK (amount > 0) NOT ENFORCED
) INHERITS (test_schema.rv_pcols_ckc_par);

-- (4) Partition trigger clones (see schema_a.sql).
CREATE FUNCTION test_schema.rv_pcols_trg_fn() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
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
CREATE TRIGGER rv_pcols_trg_changed BEFORE INSERT OR UPDATE ON test_schema.rv_pcols_trg_ex
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TRIGGER rv_pcols_trg_kept BEFORE UPDATE ON test_schema.rv_pcols_trg_ex
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TRIGGER rv_pcols_trg_added AFTER INSERT ON test_schema.rv_pcols_trg_ex
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
ALTER TABLE test_schema.rv_pcols_trg_ex_2 DISABLE TRIGGER rv_pcols_trg_changed;
ALTER TABLE test_schema.rv_pcols_trg_ex_1 ENABLE REPLICA TRIGGER rv_pcols_trg_kept;
ALTER TABLE test_schema.rv_pcols_trg_ex_1a ENABLE TRIGGER rv_pcols_trg_kept;
COMMENT ON TRIGGER rv_pcols_trg_added ON test_schema.rv_pcols_trg_ex_1a IS 'rv_pcols: new clone comment';
-- New on this side: a partitioned table whose parent's row trigger clones
-- itself onto the partitions.
CREATE TABLE test_schema.rv_pcols_trg_new (
    id integer NOT NULL,
    k  integer NOT NULL,
    PRIMARY KEY (id, k)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_pcols_trg_new_1 PARTITION OF test_schema.rv_pcols_trg_new
    FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.rv_pcols_trg_new_2 PARTITION OF test_schema.rv_pcols_trg_new
    FOR VALUES FROM (100) TO (200);
CREATE TRIGGER rv_pcols_trg_new_row BEFORE INSERT ON test_schema.rv_pcols_trg_new
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TRIGGER rv_pcols_trg_new_stmt BEFORE INSERT ON test_schema.rv_pcols_trg_new
    FOR EACH STATEMENT EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
ALTER TABLE test_schema.rv_pcols_trg_new_2 DISABLE TRIGGER rv_pcols_trg_new_row;

-- pre-columns review fixes (rv_pcols_*): see data/test/README.md.
-- (5) INHERITS: the parent's `a` becomes bigint, and so do the generated
-- columns of the children that read it.
CREATE TABLE test_schema.rv_pcols_ihp (id integer, a bigint);
CREATE TABLE test_schema.rv_pcols_ihc (
    g bigint GENERATED ALWAYS AS (a * 2) STORED
) INHERITS (test_schema.rv_pcols_ihp);
CREATE TABLE test_schema.rv_pcols_ihgc (
    h bigint GENERATED ALWAYS AS (a + 1) STORED
) INHERITS (test_schema.rv_pcols_ihc);
CREATE TABLE test_schema.rv_pcols_ihc2 (x integer) INHERITS (test_schema.rv_pcols_ihp);
CREATE INDEX rv_pcols_ihc_g_idx ON test_schema.rv_pcols_ihc (g);
INSERT INTO test_schema.rv_pcols_ihc (id, a) VALUES (1, 4);
INSERT INTO test_schema.rv_pcols_ihgc (id, a) VALUES (2, 5);
-- (6) `a` and "G x" become bigint; the blockers are unchanged.
CREATE TABLE test_schema.rv_pcols_blk (
    id    integer PRIMARY KEY,
    a     bigint,
    "G x" bigint GENERATED ALWAYS AS (a * 2) STORED CONSTRAINT rv_pcols_blk_gx_nn NOT NULL,
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
    gx bigint REFERENCES test_schema.rv_pcols_blk ("G x")
);
INSERT INTO test_schema.rv_pcols_blk (id, a) VALUES (1, 3);
INSERT INTO test_schema.rv_pcols_blk_ref VALUES (1, 6);
CREATE TABLE test_schema.rv_pcols_blkp (
    id integer,
    k  integer,
    a  bigint,
    g  bigint GENERATED ALWAYS AS (a * 2) STORED
) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_pcols_blkp_1 PARTITION OF test_schema.rv_pcols_blkp FOR VALUES IN (1);
CREATE TABLE test_schema.rv_pcols_blkp_2 PARTITION OF test_schema.rv_pcols_blkp FOR VALUES IN (2);
CREATE TRIGGER rv_pcols_blkp_upd AFTER UPDATE OF g ON test_schema.rv_pcols_blkp
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
ALTER TABLE test_schema.rv_pcols_blkp_2 DISABLE TRIGGER rv_pcols_blkp_upd;
COMMENT ON TRIGGER rv_pcols_blkp_upd ON test_schema.rv_pcols_blkp_1 IS 'rv_pcols: blkp clone comment';
CREATE TRIGGER rv_pcols_blkp_own AFTER UPDATE OF g ON test_schema.rv_pcols_blkp_1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
-- (7) The same parent outside the dumped schemas; the clones' states differ,
-- a partition is new and one is gone.
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
CREATE TABLE test_schema.rv_pcols_out_3 PARTITION OF rv_pcols_outside.rv_pcols_out FOR VALUES IN (3);
ALTER TABLE test_schema.rv_pcols_out_1a ENABLE ALWAYS TRIGGER rv_pcols_out_ins;
ALTER TABLE test_schema.rv_pcols_out_2 DISABLE TRIGGER rv_pcols_out_upd;
ALTER TABLE test_schema.rv_pcols_out_3 DISABLE TRIGGER rv_pcols_out_ins;
COMMENT ON TRIGGER rv_pcols_out_upd ON test_schema.rv_pcols_out_3 IS 'rv_pcols: new orphan clone comment';
-- (8) rv_pcols_att_2 is a partition here; its clone of the parent's trigger
-- is disabled.
CREATE TABLE test_schema.rv_pcols_att (id integer NOT NULL, k integer NOT NULL) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_pcols_att_1 PARTITION OF test_schema.rv_pcols_att FOR VALUES IN (1);
CREATE TRIGGER rv_pcols_att_trg AFTER INSERT ON test_schema.rv_pcols_att
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_pcols_trg_fn();
CREATE TABLE test_schema.rv_pcols_att_2 PARTITION OF test_schema.rv_pcols_att FOR VALUES IN (2);
ALTER TABLE test_schema.rv_pcols_att_2 DISABLE TRIGGER rv_pcols_att_trg;

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
CREATE FUNCTION test_schema.rv_r3g_fn(x integer) RETURNS bigint
    LANGUAGE sql IMMUTABLE AS $$ SELECT ($1 * 2)::bigint $$;
CREATE TABLE test_schema.rv_r3g_items (
    id integer PRIMARY KEY,
    a  integer,
    g  bigint GENERATED ALWAYS AS (test_schema.rv_r3g_fn(a)) STORED
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
    g  bigint GENERATED ALWAYS AS (test_schema.rv_r3g_fn(a)) STORED
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
CREATE FUNCTION test_schema.rv_r3g_ren_fn(y integer) RETURNS integer
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
CREATE TABLE test_schema.rv_r3g_cfg (id integer PRIMARY KEY, n bigint);
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
CREATE FUNCTION test_schema.rv_r3g_f16(x bigint) RETURNS bigint
    LANGUAGE sql IMMUTABLE AS $$ SELECT x * 2 $$;
CREATE TABLE test_schema.rv_r3g_c16 (
    id integer PRIMARY KEY,
    a  bigint,
    g  bigint GENERATED ALWAYS AS (test_schema.rv_r3g_f16(a)) STORED
);
INSERT INTO test_schema.rv_r3g_c16 (id, a) VALUES (1, 5);
CREATE VIEW test_schema.rv_r3g_c16_v AS SELECT id, g FROM test_schema.rv_r3g_c16;
CREATE PUBLICATION rv_r3g_pub_c16 FOR TABLE test_schema.rv_r3g_c16 WHERE (g > 0);
-- (2) Publication rows using a generated column that is dropped and re-added
-- because TO retypes a column it reads, on a table and on a partition.
CREATE TABLE test_schema.rv_r3g_pubrt (
    id integer PRIMARY KEY,
    a  bigint,
    v  integer GENERATED ALWAYS AS (a * 2) STORED
);
CREATE PUBLICATION rv_r3g_pub_rt_filter FOR TABLE test_schema.rv_r3g_pubrt WHERE (v > 0);
CREATE PUBLICATION rv_r3g_pub_rt_cols FOR TABLE test_schema.rv_r3g_pubrt (id, v)
    WITH (publish_generated_columns = stored);
CREATE TABLE test_schema.rv_r3g_pubrtp (
    id integer NOT NULL,
    k  integer NOT NULL,
    a  bigint,
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
    a  bigint,
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
    g  bigint GENERATED ALWAYS AS (test_schema.rv_r3g_keep_fn(v)) STORED,
    o  bigint GENERATED ALWAYS AS (v OPERATOR(test_schema.#@#) 1) STORED,
    b  bigint GENERATED ALWAYS AS (abs(v)) STORED
);
INSERT INTO test_schema.rv_r3g_pg14 (id, v) VALUES (1, 5);
CREATE INDEX rv_r3g_pg14_g ON test_schema.rv_r3g_pg14 (g);
CREATE VIEW test_schema.rv_r3g_pg14_v AS SELECT id, g, o FROM test_schema.rv_r3g_pg14;
GRANT SELECT (g) ON test_schema.rv_r3g_pg14 TO pgc_pg19_r3g_reader;
-- (5) ALTER COLUMN ... TYPE of a column an RLS policy or a trigger (UPDATE
-- OF, WHEN) uses: they are dropped ahead of the retype and created again.
CREATE TABLE test_schema.rv_r3g_rt_docs (id integer PRIMARY KEY, tenant_id bigint, body text);
ALTER TABLE test_schema.rv_r3g_rt_docs ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_docs_pol ON test_schema.rv_r3g_rt_docs
    USING (tenant_id = current_setting('pgc.rv_r3g_tenant', true)::integer);
CREATE TABLE test_schema.rv_r3g_rt_orders (id integer PRIMARY KEY, status varchar(20));
CREATE TRIGGER rv_r3g_rt_orders_upd BEFORE UPDATE OF status ON test_schema.rv_r3g_rt_orders
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE TABLE test_schema.rv_r3g_rt_ev (id integer PRIMARY KEY, kind varchar(30));
CREATE TRIGGER rv_r3g_rt_ev_when BEFORE UPDATE ON test_schema.rv_r3g_rt_ev
    FOR EACH ROW WHEN (new.kind IS DISTINCT FROM old.kind)
    EXECUTE FUNCTION test_schema.rv_r3g_trg();
CREATE TABLE test_schema.rv_r3g_rt_part (id integer NOT NULL, k integer NOT NULL, tenant_id bigint)
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
CREATE FUNCTION test_schema.rv_r3g_items_top() RETURNS bigint
    LANGUAGE sql STABLE
    AS $$ SELECT coalesce(max(g), 0)::bigint FROM test_schema.rv_r3g_items $$;
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
CREATE FUNCTION test_schema.rv_r3g_plus3(a integer, b integer) RETURNS integer
    LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 3 $$;
CREATE OPERATOR test_schema.#@@# (LEFTARG = integer, RIGHTARG = integer,
    FUNCTION = test_schema.rv_r3g_plus3);
CREATE FUNCTION test_schema.rv_r3g_uses_op() RETURNS integer
    LANGUAGE sql IMMUTABLE
    BEGIN ATOMIC SELECT (1 OPERATOR(test_schema.#@@#) 2); END;
-- What only TO has on a generated column rv_r3g_fn's CASCADE takes: the
-- table phase creates it first, the CASCADE takes it again, and it comes back
-- with the re-add.
CREATE TABLE test_schema.rv_r3g_newdep (
    id integer PRIMARY KEY,
    a  integer,
    g  bigint GENERATED ALWAYS AS (test_schema.rv_r3g_fn(a)) STORED
);
INSERT INTO test_schema.rv_r3g_newdep (id, a) VALUES (1, 3);
CREATE INDEX rv_r3g_newdep_g ON test_schema.rv_r3g_newdep (g);
ALTER TABLE test_schema.rv_r3g_newdep ADD CONSTRAINT rv_r3g_newdep_chk CHECK (g > 0);
ALTER TABLE test_schema.rv_r3g_newdep ADD CONSTRAINT rv_r3g_newdep_uk UNIQUE (id, g);
CREATE TABLE test_schema.rv_r3g_newdep_ref (
    id integer,
    g  bigint,
    FOREIGN KEY (id, g) REFERENCES test_schema.rv_r3g_newdep (id, g)
);
ALTER TABLE test_schema.rv_r3g_newdep ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_newdep_pol ON test_schema.rv_r3g_newdep AS RESTRICTIVE USING (g > 0);
-- A policy of another table reading rv_r3g_rt_docs.tenant_id through a
-- subquery, which TO changes; the table is altered before rv_r3g_rt_docs, so
-- its alter re-created the policy ahead of the retype. And a new table with
-- such a policy, which its CREATE TABLE made ahead of the retype.
CREATE TABLE test_schema.rv_r3g_rt_a_reader (id integer PRIMARY KEY, x integer);
ALTER TABLE test_schema.rv_r3g_rt_a_reader ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_a_reader_pol ON test_schema.rv_r3g_rt_a_reader
    USING (EXISTS (SELECT 1 FROM test_schema.rv_r3g_rt_docs d WHERE d.tenant_id = x AND d.id > 0));
CREATE TABLE test_schema.rv_r3g_rt_a_new (id integer PRIMARY KEY, x integer);
ALTER TABLE test_schema.rv_r3g_rt_a_new ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3g_rt_a_new_pol ON test_schema.rv_r3g_rt_a_new
    USING (EXISTS (SELECT 1 FROM test_schema.rv_r3g_rt_docs d WHERE d.tenant_id = x));
-- PostgreSQL 14: generated columns over a function made plain. There the
-- column keeps depending on the function after DROP EXPRESSION, so a type
-- change (g) still fails, and dropping the function (rv_r3g_gone_fn, h) with
-- CASCADE takes the column and its data. The migration rewrites such a
-- column, keeping its values; later servers use ALTER TYPE / DROP
-- EXPRESSION.
CREATE TABLE test_schema.rv_r3g_plain (
    id integer PRIMARY KEY,
    v  integer,
    g  bigint,
    h  integer
);
INSERT INTO test_schema.rv_r3g_plain (id, v, g, h) VALUES (1, 5, 15, 25);
CREATE INDEX rv_r3g_plain_g ON test_schema.rv_r3g_plain (g);
CREATE VIEW test_schema.rv_r3g_plain_v AS SELECT id, g, h FROM test_schema.rv_r3g_plain;
GRANT SELECT (g, h) ON test_schema.rv_r3g_plain TO pgc_pg19_r3g_reader;
COMMENT ON COLUMN test_schema.rv_r3g_plain.h IS 'rv_r3g kept';
CREATE TABLE test_schema.rv_r3g_plainp (
    id integer NOT NULL,
    k  integer NOT NULL,
    v  integer,
    g  bigint
) PARTITION BY LIST (k);
CREATE TABLE test_schema.rv_r3g_plainp_1 PARTITION OF test_schema.rv_r3g_plainp FOR VALUES IN (1);
INSERT INTO test_schema.rv_r3g_plainp (id, k, v, g) VALUES (1, 1, 2, 6);
CREATE INDEX rv_r3g_plainp_1_g ON test_schema.rv_r3g_plainp_1 (g);

-- -----------------------------------------------------------------------------
-- r3-constraints (rv_r3c_*): CHECK copies a table only inherits, ATTACH /
-- DETACH PARTITION with the parents' CHECKs, and NOT NULL name drift (TO side)
-- -----------------------------------------------------------------------------
-- (1) See schema_a.sql.
CREATE TABLE test_schema.rv_r3c_ie_p (id integer, a integer);
ALTER TABLE test_schema.rv_r3c_ie_p ADD CONSTRAINT rv_r3c_ie_p_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie_c () INHERITS (test_schema.rv_r3c_ie_p);
ALTER TABLE ONLY test_schema.rv_r3c_ie_c ALTER CONSTRAINT rv_r3c_ie_p_ck ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie_pt (id integer, a integer) PARTITION BY RANGE (id);
ALTER TABLE test_schema.rv_r3c_ie_pt ADD CONSTRAINT rv_r3c_ie_pt_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie_pt_1 PARTITION OF test_schema.rv_r3c_ie_pt FOR VALUES FROM (0) TO (10);
ALTER TABLE ONLY test_schema.rv_r3c_ie_pt_1 ALTER CONSTRAINT rv_r3c_ie_pt_ck ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie_pt_2 PARTITION OF test_schema.rv_r3c_ie_pt FOR VALUES FROM (10) TO (20);
CREATE TABLE test_schema.rv_r3c_ie_pt_3 PARTITION OF test_schema.rv_r3c_ie_pt FOR VALUES FROM (20) TO (30);
ALTER TABLE ONLY test_schema.rv_r3c_ie_pt_3 ALTER CONSTRAINT rv_r3c_ie_pt_ck ENFORCED;
-- (2) Every table is a partition of its parent here.
CREATE TABLE test_schema.rv_r3c_at (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_at_ck CHECK (a > 0))
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_at_1 PARTITION OF test_schema.rv_r3c_at FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_at_same PARTITION OF test_schema.rv_r3c_at FOR VALUES FROM (10) TO (20);
CREATE TABLE test_schema.rv_r3c_at_miss PARTITION OF test_schema.rv_r3c_at FOR VALUES FROM (20) TO (30);
CREATE TABLE test_schema.rv_r3c_at_diff PARTITION OF test_schema.rv_r3c_at FOR VALUES FROM (30) TO (40);
CREATE TABLE test_schema.rv_r3c_at_like PARTITION OF test_schema.rv_r3c_at FOR VALUES FROM (40) TO (50);
CREATE TABLE test_schema.rv_r3c_atn (id integer NOT NULL, a integer) PARTITION BY RANGE (id);
ALTER TABLE test_schema.rv_r3c_atn ADD CONSTRAINT rv_r3c_atn_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_atn_x PARTITION OF test_schema.rv_r3c_atn FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_mv_op (
    id integer NOT NULL,
    a  integer,
    CONSTRAINT rv_r3c_mv_ck CHECK (a > 0),
    CONSTRAINT rv_r3c_mv_old CHECK (a < 1000)
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_mv_np (id integer NOT NULL, a integer, CONSTRAINT rv_r3c_mv_ck CHECK (a > 0))
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_mv_x PARTITION OF test_schema.rv_r3c_mv_np FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_mv_dt (id integer NOT NULL, a integer);
-- (3) The same NOT NULL columns, under PostgreSQL's names — except
-- rv_r3c_nn_named, whose NOT NULL TO names itself (renamed to).
CREATE TABLE test_schema.rv_r3c_nn_col (b integer NOT NULL, v text);
CREATE TABLE test_schema.rv_r3c_nn_tab (id integer CONSTRAINT rv_r3c_nn_tab_pk PRIMARY KEY, a integer NOT NULL);
CREATE TABLE test_schema.rv_r3c_nn_ident (y integer GENERATED ALWAYS AS IDENTITY, v text);
CREATE TABLE test_schema.rv_r3c_nn_named (b integer CONSTRAINT rv_r3c_nn_named_nn NOT NULL);
CREATE TABLE test_schema.rv_r3c_nn_custom (b integer NOT NULL);
-- Review fixes (rv_r3c_*), TO side: see schema_a.sql.
-- (4) rv_r3c_ie3_c1 follows the NOT ENFORCED parent; rv_r3c_ie3_c2's merged
-- copy stays enforced.
CREATE TABLE test_schema.rv_r3c_ie3_p (a integer);
ALTER TABLE test_schema.rv_r3c_ie3_p ADD CONSTRAINT rv_r3c_ie3_ck CHECK (a > 0) NOT ENFORCED;
CREATE TABLE test_schema.rv_r3c_ie3_c1 () INHERITS (test_schema.rv_r3c_ie3_p);
CREATE TABLE test_schema.rv_r3c_ie3_c2 (a integer) INHERITS (test_schema.rv_r3c_ie3_c1);
ALTER TABLE test_schema.rv_r3c_ie3_c2 ADD CONSTRAINT rv_r3c_ie3_ck CHECK (a > 0);
-- (5) Classical inheritance joined and left.
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
) INHERITS (test_schema.rv_r3c_inh_p);
CREATE TABLE test_schema.rv_r3c_inh_join () INHERITS (test_schema.rv_r3c_inh_p);
INSERT INTO test_schema.rv_r3c_inh_join VALUES (2, 2, NULL);
CREATE TABLE test_schema.rv_r3c_inh_keep (
    id integer NOT NULL,
    a  integer,
    b  integer,
    CONSTRAINT rv_r3c_inh_g_ck CHECK (a > 0),
    CONSTRAINT rv_r3c_inh_p_ck CHECK (b > 0)
);
CREATE TABLE test_schema.rv_r3c_inh_drop (id integer, a integer, b integer);
CREATE TABLE test_schema.rv_r3c_inh_leave (id integer, a integer, b integer);
CREATE TABLE test_schema.rv_r3c_inh_q1 (k integer, CONSTRAINT rv_r3c_inh_q1_ck CHECK (k > 0));
CREATE TABLE test_schema.rv_r3c_inh_q2 (k integer, CONSTRAINT rv_r3c_inh_q2_ck CHECK (k < 100));
CREATE TABLE test_schema.rv_r3c_inh_r () INHERITS (test_schema.rv_r3c_inh_q2, test_schema.rv_r3c_inh_q1);
-- (6) Every table is a partition of rv_r3c_atc here.
CREATE TABLE test_schema.rv_r3c_atc (id integer NOT NULL, a integer, c integer, CONSTRAINT rv_r3c_atc_ck CHECK (a > 0))
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_atc_x PARTITION OF test_schema.rv_r3c_atc FOR VALUES FROM (0) TO (10);
CREATE TABLE test_schema.rv_r3c_atc_y PARTITION OF test_schema.rv_r3c_atc FOR VALUES FROM (10) TO (20);
CREATE TABLE test_schema.rv_r3c_atc_sub PARTITION OF test_schema.rv_r3c_atc FOR VALUES FROM (20) TO (30)
    PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r3c_atc_sub_1 PARTITION OF test_schema.rv_r3c_atc_sub FOR VALUES FROM (20) TO (25);
CREATE INDEX rv_r3c_atc_sub_1_c ON test_schema.rv_r3c_atc_sub_1 (c);
ALTER TABLE test_schema.rv_r3c_atc_sub_1 ADD CONSTRAINT rv_r3c_atc_sub_1_ck CHECK (c > 1);
-- (7) The NOT NULL names moved and swapped.
CREATE TABLE test_schema.rv_r3c_nn_move (a integer NOT NULL, b integer CONSTRAINT rv_r3c_nn_move_x NOT NULL);
CREATE TABLE test_schema.rv_r3c_nn_swap (
    a integer CONSTRAINT rv_r3c_nn_swap_q NOT NULL,
    b integer CONSTRAINT rv_r3c_nn_swap_p NOT NULL
);

-- ==== END pg19:tables ====

-- ==== BEGIN pg19:statistics — extended statistics: virtual generated columns, expression statistics ====

-- Tables carry a `t_` in front of the pg19_ prefix: pgc's table query skips
-- every name LIKE 'pg_%' (an unescaped `_`), which would hide a table named
-- pg19_... from both dumps.

-- -----------------------------------------------------------------------------
-- Expression statistics (PG14+), TO side.
-- -----------------------------------------------------------------------------
-- pg19_stat_expr_kinds switches to (mcv); pg19_stat_expr_onlist moves its
-- EXTRACT from created_on to shipped_on; pg19_stat_expr_same is unchanged;
-- pg19_stat_expr_from_only is gone.
-- pg19_stat_expr_single: TO-only univariate expression statistics; the CREATE
--   must carry no kind list.
-- pg19_stat_expr_tricky: TO-only; a ',' inside a string literal and a FROM
--   inside EXTRACT(), both of which the old ON ... FROM text split mangled.
CREATE TABLE test_schema.t_pg19_stat_expr (
    id         integer PRIMARY KEY,
    code       text,
    qty        integer,
    created_on date,
    shipped_on date
);

CREATE STATISTICS test_schema.pg19_stat_expr_kinds (mcv)
    ON qty, (lower(code)) FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_expr_onlist
    ON qty, (extract(year from shipped_on)) FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_expr_same (mcv)
    ON qty, (code || ',') FROM test_schema.t_pg19_stat_expr;

-- pg19_stat_expr_target: 100 -> 250; pg19_stat_expr_reset: 300 -> default.
CREATE STATISTICS test_schema.pg19_stat_expr_target (ndistinct)
    ON qty, (lower(code)) FROM test_schema.t_pg19_stat_expr;
ALTER STATISTICS test_schema.pg19_stat_expr_target SET STATISTICS 250;

CREATE STATISTICS test_schema.pg19_stat_expr_reset (ndistinct)
    ON qty, (upper(code)) FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_expr_single
    ON (lower(code)) FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_expr_tricky
    ON qty, (coalesce(code, ',')), (extract(year from created_on))
    FROM test_schema.t_pg19_stat_expr;

-- -----------------------------------------------------------------------------
-- Statistics CASCADE-dropped with a column or a table (PG14+), TO side.
-- -----------------------------------------------------------------------------
-- These statistics objects are unchanged, except pg19_stat_pc_kind ((mcv)
-- here) and the TO-only pg19_stat_fn_items_new and pg19_stat_mv_new; only
-- what they sit on changes.
CREATE TABLE test_schema.t_pg19_stat_conv (
    id      integer PRIMARY KEY,
    qty     integer,
    dbl     integer GENERATED ALWAYS AS (qty * 2) STORED,
    tripled integer GENERATED ALWAYS AS (qty * 4) STORED
);

CREATE STATISTICS test_schema.pg19_stat_conv_st (ndistinct)
    ON qty, dbl FROM test_schema.t_pg19_stat_conv;
ALTER STATISTICS test_schema.pg19_stat_conv_st SET STATISTICS 200;

CREATE STATISTICS test_schema.pg19_stat_conv_setexpr (ndistinct)
    ON id, tripled FROM test_schema.t_pg19_stat_conv;
ALTER STATISTICS test_schema.pg19_stat_conv_setexpr SET STATISTICS 250;

CREATE TABLE test_schema.t_pg19_stat_part (
    a integer,
    b integer,
    c integer
) PARTITION BY RANGE (b);

CREATE STATISTICS test_schema.pg19_stat_part_st (ndistinct)
    ON a, b FROM test_schema.t_pg19_stat_part;
ALTER STATISTICS test_schema.pg19_stat_part_st SET STATISTICS 400;

CREATE FUNCTION test_schema.pg19_stat_fn_compute(x integer)
RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
    SELECT (x + 1)::bigint;
$$;

CREATE TABLE test_schema.t_pg19_stat_fn_items (
    id        integer PRIMARY KEY,
    value     integer NOT NULL,
    gen_total bigint GENERATED ALWAYS AS (test_schema.pg19_stat_fn_compute(value)) STORED
);

CREATE STATISTICS test_schema.pg19_stat_fn_items_st (ndistinct)
    ON value, gen_total FROM test_schema.t_pg19_stat_fn_items;
ALTER STATISTICS test_schema.pg19_stat_fn_items_st SET STATISTICS 600;

CREATE TABLE test_schema.t_pg19_stat_fn_part (
    id        integer,
    value     integer NOT NULL,
    gen_total bigint GENERATED ALWAYS AS (test_schema.pg19_stat_fn_compute(value)) STORED
) PARTITION BY RANGE (id);

CREATE TABLE test_schema.t_pg19_stat_fn_part1 PARTITION OF test_schema.t_pg19_stat_fn_part
    FOR VALUES FROM (0) TO (1000);

CREATE STATISTICS test_schema.pg19_stat_fn_part1_st (ndistinct)
    ON value, gen_total FROM test_schema.t_pg19_stat_fn_part1;
ALTER STATISTICS test_schema.pg19_stat_fn_part1_st SET STATISTICS 700;

CREATE STATISTICS test_schema.pg19_stat_fn_items_new (mcv)
    ON value, gen_total FROM test_schema.t_pg19_stat_fn_items;

CREATE TABLE test_schema.t_pg19_stat_pc (
    id integer,
    v  bigint,
    w  integer
) PARTITION BY RANGE (id);

CREATE TABLE test_schema.t_pg19_stat_pc1 PARTITION OF test_schema.t_pg19_stat_pc
    FOR VALUES FROM (0) TO (1000);

CREATE STATISTICS test_schema.pg19_stat_pc_keep (ndistinct)
    ON v, w FROM test_schema.t_pg19_stat_pc1;
ALTER STATISTICS test_schema.pg19_stat_pc_keep SET STATISTICS 450;

CREATE STATISTICS test_schema.pg19_stat_pc_kind (mcv)
    ON w, v FROM test_schema.t_pg19_stat_pc1;

CREATE STATISTICS test_schema.pg19_stat_pc_parent (ndistinct)
    ON v, w FROM test_schema.t_pg19_stat_pc;
ALTER STATISTICS test_schema.pg19_stat_pc_parent SET STATISTICS 350;

CREATE MATERIALIZED VIEW test_schema.mv_pg19_stat_rebuilt AS
    SELECT id, qty, code FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_mv_rebuilt (ndistinct)
    ON id, qty FROM test_schema.mv_pg19_stat_rebuilt;
ALTER STATISTICS test_schema.pg19_stat_mv_rebuilt SET STATISTICS 800;

CREATE MATERIALIZED VIEW test_schema.mv_pg19_stat_new AS
    SELECT id, code FROM test_schema.t_pg19_stat_expr;

CREATE STATISTICS test_schema.pg19_stat_mv_new (ndistinct)
    ON id, code FROM test_schema.mv_pg19_stat_new;

-- -----------------------------------------------------------------------------
-- PG19 feature test: extended statistics on virtual generated columns (PG19+)
-- -----------------------------------------------------------------------------
-- `doubled` changes its expression (qty * 2 -> qty * 3; SET EXPRESSION) and
-- `readded` its type and expression (drop + re-add); the statistics on them
-- are unchanged. pg19_stat_vcol_kinds takes the default kinds;
-- pg19_stat_vcol_from_only is gone; pg19_stat_vcol_to_only is new (univariate).
-- Note: requires PostgreSQL 19+ (the table itself needs 18+).
CREATE TABLE test_schema.t_pg19_stat_vcol (
    id      integer PRIMARY KEY,
    qty     integer,
    price   integer,
    total   integer GENERATED ALWAYS AS (qty * price) VIRTUAL,
    doubled integer GENERATED ALWAYS AS (qty * 3) VIRTUAL,
    readded bigint GENERATED ALWAYS AS (qty + 2) VIRTUAL
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

CREATE STATISTICS test_schema.pg19_stat_vcol_kinds
    ON qty, total FROM test_schema.t_pg19_stat_vcol;

CREATE STATISTICS test_schema.pg19_stat_vcol_to_only
    ON total FROM test_schema.t_pg19_stat_vcol;

-- ==== END pg19:statistics ====

-- ==== BEGIN pg19:misc — misc: text search dictionaries, extensions, new PG19 syntax coverage ====
-- Naming: non-table objects are pg19_misc_*; TABLES are misc_pg19_* (see
-- schema_a.sql: the tables query's unescaped `pg_%` filter hides pg19_* tables).

-- -----------------------------------------------------------------------------
-- PG19 misc 1/5: Snowball Dutch stemmer rewritten, old one kept as 'dutch_porter'
-- -----------------------------------------------------------------------------
-- MODIFIED (PG19+): pg19_misc_dict_nl language 'dutch' -> 'dutch_porter'
--   (stopwords unchanged). Expected: one in-place
--   ALTER TEXT SEARCH DICTIONARY ... (language = 'dutch_porter', stopwords = 'dutch');
--   the dependent configuration, generated column and index survive.
-- On PG14-18 'dutch_porter' does not exist, so the dictionary keeps 'dutch'
-- there and the section produces no diff.
DO $$
BEGIN
    IF current_setting('server_version_num')::int >= 190000 THEN
        CREATE TEXT SEARCH DICTIONARY test_schema.pg19_misc_dict_nl (
            TEMPLATE = pg_catalog.snowball,
            language = 'dutch_porter',
            stopwords = 'dutch'
        );
    ELSE
        CREATE TEXT SEARCH DICTIONARY test_schema.pg19_misc_dict_nl (
            TEMPLATE = pg_catalog.snowball,
            language = 'dutch',
            stopwords = 'dutch'
        );
    END IF;
END;
$$;
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
-- MODIFIED: misc_pg19_ts_notes gains a STORED generated column over the
--   configuration (every version). On PG19 the rows FROM already holds are
--   computed when the column is added, so the dictionary ALTER above has to
--   come before it: pgc emits in-place option changes before the tables.
CREATE TABLE test_schema.misc_pg19_ts_notes (
    id       integer PRIMARY KEY,
    note     text,
    note_tsv tsvector GENERATED ALWAYS AS (to_tsvector('test_schema.pg19_misc_tsconfig_nl'::regconfig, note)) STORED
);
-- ADDED (PG19+): the new Polish stemmer and a copy of the new built-in
-- Esperanto configuration. No polish.stop / esperanto.stop ships, so no
-- STOPWORDS. Both fail harmlessly on PG14-18.
CREATE TEXT SEARCH DICTIONARY test_schema.pg19_misc_dict_pl (
    TEMPLATE = pg_catalog.snowball,
    language = 'polish'
);
CREATE TEXT SEARCH CONFIGURATION test_schema.pg19_misc_tsconfig_eo (
    COPY = pg_catalog.esperanto
);

-- -----------------------------------------------------------------------------
-- PG19 misc 2/5: oid8 data type (PG19+)
-- -----------------------------------------------------------------------------
-- TO: bigint -> oid8 (implicit cast), oid -> oid8 (assignment cast),
--     oid8 -> bigint (binary-coercible assignment cast), new oid8 / oid8[]
--     columns with defaults, a CHECK, a hash index, a domain, a range type,
--     a function and a view using the oid8 min/max aggregates.
-- Every oid8 statement fails on PG14-18 (type "oid8" does not exist), which
-- leaves the base table identical to the FROM side there.
CREATE TABLE test_schema.misc_pg19_oid8_refs (
    id          bigint PRIMARY KEY,
    legacy_ref  oid,
    big_ref     bigint,
    back_ref    bigint
);
ALTER TABLE test_schema.misc_pg19_oid8_refs ALTER COLUMN id TYPE oid8;
ALTER TABLE test_schema.misc_pg19_oid8_refs ALTER COLUMN legacy_ref TYPE oid8;
ALTER TABLE test_schema.misc_pg19_oid8_refs ALTER COLUMN big_ref TYPE oid8;
ALTER TABLE test_schema.misc_pg19_oid8_refs ADD COLUMN ref_list oid8[] DEFAULT '{1,2}';
ALTER TABLE test_schema.misc_pg19_oid8_refs ADD COLUMN max_ref oid8 DEFAULT '18446744073709551615'::oid8;
ALTER TABLE test_schema.misc_pg19_oid8_refs ADD CONSTRAINT chk_misc_pg19_oid8_refs_id CHECK (id > '0'::oid8);
CREATE INDEX idx_misc_pg19_oid8_refs_max_ref ON test_schema.misc_pg19_oid8_refs USING hash (max_ref);
CREATE DOMAIN test_schema.pg19_misc_oid8_nonzero AS oid8 CHECK (VALUE <> '0'::oid8);
CREATE TYPE test_schema.pg19_misc_oid8_span AS RANGE (subtype = oid8);
ALTER TABLE test_schema.misc_pg19_oid8_refs ADD COLUMN nonzero_ref test_schema.pg19_misc_oid8_nonzero;
ALTER TABLE test_schema.misc_pg19_oid8_refs ADD COLUMN ref_span test_schema.pg19_misc_oid8_span;
CREATE FUNCTION test_schema.pg19_misc_oid8_or_default(x oid8 DEFAULT '7'::oid8)
RETURNS oid8 LANGUAGE sql IMMUTABLE AS 'SELECT x';
CREATE VIEW test_schema.pg19_misc_oid8_refs_summary AS
SELECT max(id) AS max_id, min(max_ref) AS min_ref FROM test_schema.misc_pg19_oid8_refs;

-- -----------------------------------------------------------------------------
-- PG19 misc 3/5: window-function null treatment IGNORE NULLS / RESPECT NULLS
-- -----------------------------------------------------------------------------
-- See schema_a.sql for the case matrix. On PG14-18 the IGNORE/RESPECT NULLS
-- statements fail to parse and the view is simply absent from this side.
CREATE TABLE test_schema.misc_pg19_readings (
    id      integer PRIMARY KEY,
    sensor  integer NOT NULL,
    reading numeric
);
CREATE VIEW test_schema.pg19_misc_v_null_treatment AS
SELECT id, sensor,
       lag(reading) IGNORE NULLS OVER (PARTITION BY sensor ORDER BY id) AS prev_reading
FROM test_schema.misc_pg19_readings;
CREATE VIEW test_schema.pg19_misc_v_null_treatment_off AS
SELECT id,
       last_value(reading) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known
FROM test_schema.misc_pg19_readings;
-- RESPECT NULLS is the default and is not deparsed: no diff against FROM on PG19.
CREATE VIEW test_schema.pg19_misc_v_respect_nulls AS
SELECT id,
       first_value(reading) RESPECT NULLS OVER (ORDER BY id) AS first_reading
FROM test_schema.misc_pg19_readings;
CREATE MATERIALIZED VIEW test_schema.pg19_misc_mv_null_treatment AS
SELECT sensor, id,
       lead(reading, 1, 0) IGNORE NULLS OVER (PARTITION BY sensor ORDER BY id) AS next_reading
FROM test_schema.misc_pg19_readings;
-- TO-only view mixing a named window with IGNORE NULLS.
CREATE VIEW test_schema.pg19_misc_v_ignore_nulls_new AS
SELECT id,
       nth_value(reading, 2) IGNORE NULLS OVER w AS second_reading
FROM test_schema.misc_pg19_readings
WINDOW w AS (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);

-- -----------------------------------------------------------------------------
-- PG19 misc 4/5: JSON_ARRAY(query) deparse and empty-result semantics
-- -----------------------------------------------------------------------------
-- See schema_a.sql. pg19_misc_ja_view and pg19_misc_ja_mv move from the
-- pre-19 JSON_ARRAYAGG subquery spelling to JSON_ARRAY(SELECT ...): on PG19 a
-- real change (NULL -> '[]' on an empty table), on PG16-18 the same view.
CREATE TABLE test_schema.misc_pg19_ja_items (
    id    integer PRIMARY KEY,
    label text
);
CREATE VIEW test_schema.pg19_misc_ja_view AS
SELECT JSON_ARRAY(SELECT label FROM test_schema.misc_pg19_ja_items ORDER BY id) AS labels;
CREATE MATERIALIZED VIEW test_schema.pg19_misc_ja_mv AS
SELECT JSON_ARRAY(SELECT id FROM test_schema.misc_pg19_ja_items RETURNING jsonb) AS ids;
CREATE VIEW test_schema.pg19_misc_ja_stable AS
SELECT JSON_ARRAY(SELECT id FROM test_schema.misc_pg19_ja_items ORDER BY id) AS ids;

-- -----------------------------------------------------------------------------
-- PG19 misc 5/5: heap reloptions autovacuum_parallel_workers and
-- log_autoanalyze_min_duration (PG19+)
-- -----------------------------------------------------------------------------
-- On PG14-18 every ALTER ... SET below fails with "unrecognized parameter";
-- the objects are then identical to FROM (the new table is created without
-- options) and the round trip stays idempotent.
CREATE TABLE test_schema.misc_pg19_reloptions_test (
    id    serial PRIMARY KEY,
    value text
) WITH (fillfactor = 80);
ALTER TABLE test_schema.misc_pg19_reloptions_test SET (
    autovacuum_parallel_workers = 4,
    log_autoanalyze_min_duration = 1000
);
-- TO only: new table, created with the PG19 options in its WITH (...) clause.
CREATE TABLE test_schema.misc_pg19_reloptions_new (
    id integer PRIMARY KEY
);
ALTER TABLE test_schema.misc_pg19_reloptions_new SET (
    autovacuum_parallel_workers = 0,
    log_autoanalyze_min_duration = 250
);
CREATE TABLE test_schema.misc_pg19_reloptions_part (
    id    integer,
    value text
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.misc_pg19_reloptions_part_1
    PARTITION OF test_schema.misc_pg19_reloptions_part FOR VALUES FROM (0) TO (100);
ALTER TABLE test_schema.misc_pg19_reloptions_part_1 SET (autovacuum_parallel_workers = 1);
CREATE MATERIALIZED VIEW test_schema.pg19_misc_reloptions_mv AS SELECT 1 AS x;
ALTER MATERIALIZED VIEW test_schema.pg19_misc_reloptions_mv SET (log_autoanalyze_min_duration = 10);

-- -----------------------------------------------------------------------------
-- PG19 misc: new contrib extension pg_stash_advice (PG19+)
-- -----------------------------------------------------------------------------
-- TO only. Needs no shared_preload_libraries; its member objects are
-- extension-owned and never dumped. Expected on PG19:
--   create extension if not exists pg_stash_advice with schema public version '1.0';
-- (the version is installable on the migrated server, so it stays pinned).
-- Fails harmlessly on PG14-18 ("is not available"), absent on both sides.
CREATE EXTENSION IF NOT EXISTS pg_stash_advice WITH SCHEMA public;

-- A table whose name starts with `pg` plus a non-underscore character must be
-- dumped. The tables query filtered `tablename not like 'pg_%'`, where `_` is a
-- LIKE wildcard, so pgc never saw this table and never created it (TO only).
CREATE TABLE test_schema.pgx_like_escape (id integer PRIMARY KEY, note text);

-- Quoted table and column names containing spaces (TO only). CREATE TABLE
-- used to cut each column script at its first space to drop the name, so
-- `"Col X"` came out as `"Col X" X" integer` — an unterminated identifier that
-- made psql swallow the rest of the migration.
CREATE TABLE test_schema."spaced Columns" (
    "Col X"       integer NOT NULL,
    "a b c"       text DEFAULT 'x y',
    plain_col     integer
);

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

-- TO side.
CREATE TABLE test_schema.rv_r3d_coll_alter (
    id         integer PRIMARY KEY,
    c_retag    text COLLATE test_schema.rv_r3d_ci,
    c_reschema text COLLATE test_schema.rv_r3d_ci,
    c_kept     text COLLATE "POSIX",
    c_added    text COLLATE test_schema."rv_r3d_ci""; create table public.rv_r3d_pwned(x int); --"
);
-- TO-only table: its CREATE TABLE carries the COLLATE clauses.
CREATE TABLE test_schema.rv_r3d_collated (
    id        integer PRIMARY KEY,
    c_hostile text COLLATE test_schema."rv_r3d_ci""; create table public.rv_r3d_pwned(x int); --",
    c_schema  text COLLATE test_schema.rv_r3d_ci,
    c_catalog text COLLATE "C"
);
ALTER EVENT TRIGGER "rv_r3d_Keep Trg" DISABLE;
COMMENT ON EVENT TRIGGER "rv_r3d_Keep Trg" IS 'r3-ddl kept';
CREATE EVENT TRIGGER "rv_r3d_To ""Only""" ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE FUNCTION test_schema.rv_r3d_evt_fn();
-- TO-only table whose primary key columns hold `)`, `,`, a doubled quote and
-- a keyword. CREATE TABLE's inline PRIMARY KEY was cut from the constraint
-- definition at the first `)` and split at every `,`, so it named columns
-- that do not exist and the table was never created.
CREATE TABLE test_schema.rv_r3d_pk_hostile (
    "Code (x)"      integer,
    "a,b"           integer,
    "select"        integer,
    "say ""hi"")"   integer,
    note            text,
    PRIMARY KEY ("Code (x)", "a,b", "select", "say ""hi"")")
);
-- TO-only table whose primary key carries INCLUDE and DEFERRABLE: the inline
-- PRIMARY KEY kept only the key column list, so the table came out with a
-- different key and the next compare dropped and re-added it.
CREATE TABLE test_schema.rv_r3d_pk_include (
    id integer,
    x  integer,
    CONSTRAINT rv_r3d_pk_include_pkey PRIMARY KEY (id) INCLUDE (x)
        DEFERRABLE INITIALLY DEFERRED
);
-- rv_r3d_products keeps its key without ON DELETE CASCADE (changed);
-- rv_r3d_orders loses its key with the table it referenced.
CREATE TABLE test_schema.rv_r3d_products (
    id                    integer PRIMARY KEY,
    rv_r3d_log_catalog_id integer,
    CONSTRAINT rv_r3d_products_catalog_fk FOREIGN KEY (rv_r3d_log_catalog_id)
        REFERENCES test_schema.rv_r3d_catalogs(id)
);
CREATE TABLE test_schema.rv_r3d_orders (
    id        integer PRIMARY KEY,
    legacy_id integer
);

-- r3-ddl review. Both sides: the trigger function, and a table whose policy
-- reads a column of rv_r3d_retype in a subquery (PostgreSQL records that as
-- a dependency on the column too).
CREATE OR REPLACE FUNCTION test_schema.rv_r3d_trg_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RETURN NEW;
END;
$$;

-- TO side.
-- A collation only TO has, used by a kept column, a new table and a new
-- domain: it is created before the types and tables.
CREATE COLLATION test_schema.rv_r3d_new_ci (provider = libc, locale = 'POSIX');
CREATE DOMAIN test_schema.rv_r3d_new_dom AS text COLLATE test_schema.rv_r3d_new_ci;
CREATE TABLE test_schema.rv_r3d_to_new_coll (
    id integer PRIMARY KEY,
    c  text COLLATE test_schema.rv_r3d_new_ci
);
CREATE TABLE test_schema.rv_r3d_new_coll_tbl (
    id integer PRIMARY KEY,
    c  text COLLATE test_schema.rv_r3d_new_ci,
    d  test_schema.rv_r3d_new_dom
);
-- The policies and triggers in the way of the retype are dropped ahead of
-- the table alters and put back from TO: rv_r3d_retype_code changes, and
-- rv_r3d_retype_gone is gone.
CREATE TABLE test_schema.rv_r3d_retype (
    id     integer PRIMARY KEY,
    owner  text COLLATE "POSIX",
    code   varchar(20),
    status text COLLATE "POSIX"
);
ALTER TABLE test_schema.rv_r3d_retype ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r3d_retype_owner ON test_schema.rv_r3d_retype
    USING (owner = current_user);
CREATE POLICY rv_r3d_retype_code ON test_schema.rv_r3d_retype FOR SELECT
    USING (code <> 'y');
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
CREATE TABLE test_schema.rv_r3d_pkey_coll (id integer, c text COLLATE "POSIX")
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

-- Expected: drop attribute b, gone and c; alter attribute a type bigint; add
-- bb, c, added (in this order), so the attribute order matches.
CREATE TYPE test_schema.rv_r3t_comp AS (a bigint, bb integer, c text, added text);
-- Expected: add attribute z (the column keeps its value).
CREATE TYPE test_schema.rv_r3t_used AS (x integer, y text, z date);
CREATE TABLE test_schema.rv_r3t_used_tbl (id integer PRIMARY KEY, v test_schema.rv_r3t_used);
INSERT INTO test_schema.rv_r3t_used_tbl VALUES (1, ROW(1, 'one', NULL));
-- Expected: drop / alter / add attribute ... cascade, then the table's own
-- default and NOT NULL on extra; no ALTER TABLE ... ADD/DROP COLUMN.
CREATE TYPE test_schema.rv_r3t_typed_t AS (id bigint, keep text, extra integer);
CREATE TABLE test_schema.rv_r3t_typed OF test_schema.rv_r3t_typed_t (
    PRIMARY KEY (id),
    extra WITH OPTIONS NOT NULL DEFAULT 7
);
-- Expected: the row a has gets extra = 7 before SET NOT NULL.
INSERT INTO test_schema.rv_r3t_typed (id, keep) VALUES (1, 'kept');

-- Expected: alter sequence ... as bigint / as integer before the bounds.
CREATE TABLE test_schema.rv_r3t_ser (id bigserial PRIMARY KEY, v text);
CREATE TABLE test_schema.rv_r3t_bigser (id serial PRIMARY KEY, v text);
INSERT INTO test_schema.rv_r3t_ser (v) VALUES ('one');
CREATE SEQUENCE test_schema.rv_r3t_seq AS bigint;
CREATE TABLE test_schema.rv_r3t_ident (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY);

-- Expected: alter table ... owner to pgc_pg19_r3t_owner only; no alter
-- sequence ... owner to for their sequences.
CREATE TABLE test_schema.rv_r3t_own_ser (id serial PRIMARY KEY, v text);
CREATE TABLE test_schema.rv_r3t_own_ident (id integer GENERATED ALWAYS AS IDENTITY, v text);
ALTER TABLE test_schema.rv_r3t_own_ser OWNER TO pgc_pg19_r3t_owner;
ALTER TABLE test_schema.rv_r3t_own_ident OWNER TO pgc_pg19_r3t_owner;

-- Expected: alter type public.rv_r3t_mis_multirange set schema test_schema.
CREATE TYPE test_schema.rv_r3t_mis AS RANGE (subtype = integer);
-- TO only: a new range and a table using its multirange. The range was
-- created with an unqualified multirange_type_name, so the table failed
-- ("type test_schema.rv_r3t_new_rng_multirange does not exist").
CREATE TYPE test_schema.rv_r3t_new_rng AS RANGE (subtype = bigint);
CREATE TABLE test_schema.rv_r3t_mr_use (id integer, r test_schema.rv_r3t_new_rng_multirange);

-- Review fixes. Expected: drop view rv_r3t_tv_v, then alter attribute "id"
-- set data type bigint cascade, then the view again.
CREATE TYPE test_schema.rv_r3t_tv_t AS (id bigint, name text);
CREATE TABLE test_schema.rv_r3t_tv OF test_schema.rv_r3t_tv_t;
INSERT INTO test_schema.rv_r3t_tv VALUES (1, 'one');
CREATE VIEW test_schema.rv_r3t_tv_v AS SELECT id, name FROM test_schema.rv_r3t_tv;
-- Expected: alter sequence ... owned by none, then owner to, then (after
-- the tables) owned by rv_r3t_mv_b.id.
CREATE TABLE test_schema.rv_r3t_mv_a (id bigint);
CREATE TABLE test_schema.rv_r3t_mv_b (id bigint);
ALTER TABLE test_schema.rv_r3t_mv_b OWNER TO pgc_pg19_r3t_owner;
CREATE SEQUENCE test_schema.rv_r3t_mv_seq;
ALTER SEQUENCE test_schema.rv_r3t_mv_seq OWNER TO pgc_pg19_r3t_owner;
ALTER SEQUENCE test_schema.rv_r3t_mv_seq OWNED BY test_schema.rv_r3t_mv_b.id;


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
-- TO: rv_r3c_dict on the snowball template (FROM: simple), with another comment.
CREATE TEXT SEARCH DICTIONARY test_schema.rv_r3c_dict (TEMPLATE = pg_catalog.snowball, language = 'english');
COMMENT ON TEXT SEARCH DICTIONARY test_schema.rv_r3c_dict IS 'rv_r3c dictionary (snowball)';
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
-- TO side.
CREATE FUNCTION test_schema.rv_r4t_trg() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
CREATE TYPE test_schema.rv_r4t_pt AS (id integer, amount bigint, note text);
CREATE TABLE test_schema.rv_r4t_tp OF test_schema.rv_r4t_pt (id WITH OPTIONS NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r4t_tp1 PARTITION OF test_schema.rv_r4t_tp FOR VALUES FROM (0) TO (100);
ALTER TABLE test_schema.rv_r4t_tp1 ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_tp1_pol ON test_schema.rv_r4t_tp1 USING (amount > 0);
CREATE TRIGGER rv_r4t_tp1_trg BEFORE UPDATE OF amount ON test_schema.rv_r4t_tp1
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r4t_trg();
INSERT INTO test_schema.rv_r4t_tp VALUES (1, 5, 'a');
CREATE TYPE test_schema.rv_r4t_it AS (id integer, amount bigint, note text);
CREATE TABLE test_schema.rv_r4t_tt OF test_schema.rv_r4t_it (id WITH OPTIONS PRIMARY KEY);
CREATE TABLE test_schema.rv_r4t_ti (extra integer, g bigint GENERATED ALWAYS AS (amount * 2) STORED)
    INHERITS (test_schema.rv_r4t_tt);
ALTER TABLE test_schema.rv_r4t_ti ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_ti_pol ON test_schema.rv_r4t_ti USING (amount > 0);
CREATE POLICY rv_r4t_ti_gpol ON test_schema.rv_r4t_ti USING (g > 0);
INSERT INTO test_schema.rv_r4t_ti (id, amount, note, extra) VALUES (1, 5, 'a', 3);
CREATE TABLE test_schema.rv_r4t_other (id integer, x bigint);
ALTER TABLE test_schema.rv_r4t_other ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_other_pol ON test_schema.rv_r4t_other
    USING (x IN (SELECT amount FROM test_schema.rv_r4t_tt));
CREATE TYPE test_schema.rv_r4t_dt AS (id integer, amount bigint);
CREATE TYPE test_schema.rv_r4t_rt AS (id integer, amount bigint, note text);
CREATE TABLE test_schema.rv_r4t_rp OF test_schema.rv_r4t_rt (id WITH OPTIONS NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.rv_r4t_rp1 PARTITION OF test_schema.rv_r4t_rp FOR VALUES FROM (0) TO (10);
ALTER TABLE test_schema.rv_r4t_rp ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_rp_pol ON test_schema.rv_r4t_rp USING (amount > 0);
CREATE TABLE test_schema.rv_r4t_reader (id integer, x bigint);
ALTER TABLE test_schema.rv_r4t_reader ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_reader_pol ON test_schema.rv_r4t_reader
    USING (x IN (SELECT amount FROM test_schema.rv_r4t_rp));
CREATE TYPE test_schema.rv_r4t_ct AS (id integer, note text COLLATE "C", memo text);
CREATE TABLE test_schema.rv_r4t_coll OF test_schema.rv_r4t_ct (id WITH OPTIONS PRIMARY KEY);
ALTER TABLE test_schema.rv_r4t_coll ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_coll_pol ON test_schema.rv_r4t_coll USING (note <> '' AND memo <> '');
CREATE TRIGGER rv_r4t_coll_trg BEFORE UPDATE OF note ON test_schema.rv_r4t_coll
    FOR EACH ROW EXECUTE FUNCTION test_schema.rv_r4t_trg();
INSERT INTO test_schema.rv_r4t_coll VALUES (1, 'a', 'b');
CREATE TABLE test_schema.rv_r4t_a_t (id integer PRIMARY KEY, n bigint, g bigint GENERATED ALWAYS AS (n * 2) STORED);
CREATE TABLE test_schema.rv_r4t_z (id integer, n bigint);
CREATE TABLE test_schema.rv_r4t_o (id integer, x bigint);
ALTER TABLE test_schema.rv_r4t_o ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_o_pol ON test_schema.rv_r4t_o
    USING (x IN (SELECT g FROM test_schema.rv_r4t_a_t) AND x IN (SELECT n FROM test_schema.rv_r4t_z));
INSERT INTO test_schema.rv_r4t_a_t (id, n) VALUES (1, 4);
INSERT INTO test_schema.rv_r4t_z VALUES (1, 4);
CREATE TYPE test_schema.rv_r4t_newc AS (a integer, b text COLLATE "C");
CREATE TABLE test_schema.rv_r4t_new OF test_schema.rv_r4t_it (
    id WITH OPTIONS PRIMARY KEY,
    note WITH OPTIONS DEFAULT 'n/a' NOT NULL,
    CONSTRAINT rv_r4t_new_ck CHECK (amount >= 0)
);
CREATE TABLE test_schema.rv_r4t_bare OF test_schema.rv_r4t_it;
CREATE TABLE test_schema.rv_r4t_nn OF test_schema.rv_r4t_it (amount WITH OPTIONS CONSTRAINT rv_r4t_nn_amount NOT NULL);
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
CREATE TYPE test_schema.rv_r4t_qt AS (id integer, "order" bigint, "Amt" bigint);
CREATE TABLE test_schema.rv_r4t_qtt OF test_schema.rv_r4t_qt;
ALTER TABLE test_schema.rv_r4t_qtt ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_qtt_pol ON test_schema.rv_r4t_qtt USING ("order" > 0);
CREATE TABLE test_schema.rv_r4t_qti (g bigint GENERATED ALWAYS AS ("Amt" * 2) STORED)
    INHERITS (test_schema.rv_r4t_qtt);
INSERT INTO test_schema.rv_r4t_qti (id, "order", "Amt") VALUES (1, 2, 3);
CREATE TYPE test_schema.rv_r4t_lt AS (id integer, amount integer);
CREATE TABLE test_schema.rv_r4t_ltt OF test_schema.rv_r4t_lt;
CREATE TABLE test_schema.rv_r4t_lti (id integer, amount bigint, extra integer);
ALTER TABLE test_schema.rv_r4t_lti ENABLE ROW LEVEL SECURITY;
CREATE POLICY rv_r4t_lti_pol ON test_schema.rv_r4t_lti USING (amount > 0);
INSERT INTO test_schema.rv_r4t_lti VALUES (1, 5, 3);
CREATE TYPE test_schema.rv_r4t_3t AS (id integer, amount bigint);
CREATE TABLE test_schema.rv_r4t_3tt OF test_schema.rv_r4t_3t;
CREATE TYPE test_schema.rv_r4t_5t AS (id integer, amount bigint);
CREATE TABLE test_schema.rv_r4t_5tt OF test_schema.rv_r4t_5t;
CREATE TABLE test_schema.rv_r4t_5c1 (g bigint) INHERITS (test_schema.rv_r4t_5tt);
CREATE SCHEMA IF NOT EXISTS rv_r4t_outside;
CREATE TYPE rv_r4t_outside.rv_r4t_xt AS (id integer, amount bigint);
CREATE TABLE test_schema.rv_r4t_xtt OF rv_r4t_outside.rv_r4t_xt;
CREATE VIEW test_schema.rv_r4t_xv AS SELECT id, amount FROM test_schema.rv_r4t_xtt;
CREATE PUBLICATION rv_r4t_xpub FOR TABLE test_schema.rv_r4t_xtt (id, amount) WHERE (amount > 0);

-- ==== END pg19:misc ====

-- ═════════════════════════════════════════════════════════════════════
-- Pre-existing bug fixes shipped with pgc v1.1.0 (all supported versions).
-- ═════════════════════════════════════════════════════════════════════

-- ==== BEGIN fix:sqlbody — SQL-standard routine bodies (BEGIN ATOMIC / RETURN) ====

-- SQL-standard routine bodies (BEGIN ATOMIC ... END / RETURN expr, PG14+).
-- TO: see schema_a.sql for the FROM side. pg19_sqlbody_upsert uses PG19's
-- INSERT ... ON CONFLICT DO SELECT, a syntax error before 19: on PG14-18 it
-- fails under ON_ERROR_STOP=0, is absent from TO, and the diff drops it.
CREATE TABLE test_schema.pg19_sqlbody_target (
    id  integer PRIMARY KEY,
    val text
);
CREATE TABLE public.pg19_sqlbody_public_target (
    id integer PRIMARY KEY
);

CREATE FUNCTION test_schema.pg19_sqlbody_unchanged(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT p + 1;
END;

CREATE FUNCTION test_schema.pg19_sqlbody_return(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN p + 2;

-- PG19+: ON CONFLICT DO SELECT with a lock strength and a WHERE filter.
CREATE FUNCTION test_schema.pg19_sqlbody_upsert(p integer)
RETURNS SETOF test_schema.pg19_sqlbody_target
LANGUAGE sql
BEGIN ATOMIC
    INSERT INTO test_schema.pg19_sqlbody_target VALUES (p, 'x')
    ON CONFLICT (id) DO SELECT FOR UPDATE WHERE pg19_sqlbody_target.val IS NOT NULL
    RETURNING *;
END;

CREATE PROCEDURE test_schema.pg19_sqlbody_proc_changed(p integer)
LANGUAGE sql
BEGIN ATOMIC
    INSERT INTO test_schema.pg19_sqlbody_target VALUES (p, 'b')
    ON CONFLICT (id) DO UPDATE SET val = excluded.val;
    DELETE FROM test_schema.pg19_sqlbody_target WHERE val IS NULL;
END;

CREATE FUNCTION test_schema.pg19_sqlbody_to_atomic(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT p * 2;
END;

CREATE FUNCTION test_schema.pg19_sqlbody_to_string(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
AS $$ SELECT p * 3 $$;

-- TO-only: every option goes before the body (flags, COST, SET, a default).
CREATE FUNCTION test_schema.pg19_sqlbody_options(p integer DEFAULT 1) RETURNS integer
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE COST 5
SET search_path = pg_catalog
RETURN p * 4;

-- TO-only procedure with SECURITY DEFINER and a comment.
CREATE PROCEDURE test_schema.pg19_sqlbody_proc_to_only(p integer)
LANGUAGE sql SECURITY DEFINER
BEGIN ATOMIC
    INSERT INTO test_schema.pg19_sqlbody_target VALUES (p, 'c')
    ON CONFLICT (id) DO NOTHING;
END;
COMMENT ON PROCEDURE test_schema.pg19_sqlbody_proc_to_only(integer) IS 'SQL-standard body procedure';

-- TO-only pair: a SQL-standard body is parsed at CREATE time even with
-- check_function_bodies off, so the callee (which sorts last by name) must be
-- created before its caller.
CREATE FUNCTION test_schema.pg19_sqlbody_z_callee(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN p - 1;
CREATE FUNCTION test_schema.pg19_sqlbody_a_caller(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN test_schema.pg19_sqlbody_z_callee(p) * 10;

-- TO-only: literals that must survive byte for byte, a CR LF (the deparsed
-- body carries it raw) and a "$$" that is no dollar quote.
CREATE FUNCTION test_schema.pg19_sqlbody_literals() RETURNS text
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT E'line1\r\nline2' || '$$ not a delimiter $$';
END;

-- TO-only in public: the body is deparsed with an empty search_path, so the
-- public table stays qualified (under the default search_path it printed
-- unqualified, as in a view).
CREATE FUNCTION public.pg19_sqlbody_public_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM public.pg19_sqlbody_public_target);

-- ---- Hard dependencies: see schema_a.sql for the FROM side. The legacy
-- table, sequence and readers are FROM-only.
CREATE TABLE test_schema.pg19_sqlbody_cols (id integer, v bigint);
CREATE FUNCTION test_schema.pg19_sqlbody_cols_old() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(id) FROM test_schema.pg19_sqlbody_cols);
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
CREATE TABLE test_schema.pg19_sqlbody_rule_log (id integer);
CREATE RULE pg19_sqlbody_rule_max AS ON INSERT TO test_schema.pg19_sqlbody_rule_log
    DO ALSO SELECT test_schema.pg19_sqlbody_cols_max();

CREATE TABLE test_schema.pg19_sqlbody_shape (id integer, a integer, b integer);
CREATE VIEW test_schema.pg19_sqlbody_shape_view AS
SELECT id, b FROM test_schema.pg19_sqlbody_shape;
CREATE FUNCTION test_schema.pg19_sqlbody_shape_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM test_schema.pg19_sqlbody_shape_view);

CREATE FUNCTION test_schema.pg19_sqlbody_rt_callee(p integer) RETURNS bigint
LANGUAGE sql IMMUTABLE
RETURN p + 1;
CREATE FUNCTION test_schema.pg19_sqlbody_rt_caller(p integer) RETURNS bigint
LANGUAGE sql IMMUTABLE
RETURN test_schema.pg19_sqlbody_rt_callee(p) * 2;

CREATE TABLE test_schema.pg19_sqlbody_parted (id integer, k integer) PARTITION BY RANGE (k);
CREATE FUNCTION test_schema.pg19_sqlbody_parted_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM test_schema.pg19_sqlbody_parted);

CREATE FUNCTION test_schema.pg19_sqlbody_eq(integer, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN $1 = $2;
CREATE FUNCTION test_schema.pg19_sqlbody_eq_or_null(integer, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN $1 = $2 OR $1 IS NULL;
CREATE OPERATOR test_schema.=== (
    LEFTARG = integer, RIGHTARG = integer, FUNCTION = test_schema.pg19_sqlbody_eq_or_null
);
CREATE FUNCTION test_schema.pg19_sqlbody_uses_op(p integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN p OPERATOR(test_schema.===) 1;
CREATE TABLE test_schema.pg19_sqlbody_op_default (
    id integer,
    ok boolean DEFAULT test_schema.pg19_sqlbody_uses_op(1)
);

-- TO-only: routines using an operator and a cast that only exist once the
-- operator and cast phases (which follow the routine phase) have run. A
-- SQL-standard body is resolved at CREATE time, so they, and the view calling
-- one, are created after those phases.
CREATE FUNCTION test_schema.pg19_sqlbody_ne(integer, integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN $1 <> $2;
CREATE OPERATOR test_schema.<=> (
    LEFTARG = integer, RIGHTARG = integer, FUNCTION = test_schema.pg19_sqlbody_ne
);
CREATE FUNCTION test_schema.pg19_sqlbody_uses_new_op(p integer) RETURNS boolean
LANGUAGE sql IMMUTABLE
RETURN p OPERATOR(test_schema.<=>) 0;
CREATE VIEW test_schema.pg19_sqlbody_new_op_view AS
SELECT test_schema.pg19_sqlbody_uses_new_op(1) AS nonzero;
CREATE TYPE test_schema.pg19_sqlbody_pair AS (a integer, b integer);
CREATE FUNCTION test_schema.pg19_sqlbody_int_to_pair(integer) RETURNS test_schema.pg19_sqlbody_pair
LANGUAGE sql IMMUTABLE
AS $$ SELECT ($1, $1)::test_schema.pg19_sqlbody_pair $$;
CREATE CAST (integer AS test_schema.pg19_sqlbody_pair)
    WITH FUNCTION test_schema.pg19_sqlbody_int_to_pair(integer);
CREATE FUNCTION test_schema.pg19_sqlbody_uses_cast(p integer) RETURNS test_schema.pg19_sqlbody_pair
LANGUAGE sql IMMUTABLE
RETURN p::test_schema.pg19_sqlbody_pair;

-- TO-only in public: the body is deparsed with an empty search_path, so the
-- public names stay qualified and the dependency scan sees them. Without that
-- the caller (first by name) and the view reader (routines sort before views)
-- were created ahead of what they use.
CREATE FUNCTION public.pg19_sqlbody_z_pub_callee(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
RETURN p * 2;
CREATE FUNCTION public.pg19_sqlbody_a_pub_caller(p integer) RETURNS integer
LANGUAGE sql IMMUTABLE
BEGIN ATOMIC
    SELECT public.pg19_sqlbody_z_pub_callee(p) + 1;
END;
CREATE VIEW public.pg19_sqlbody_pub_view AS
SELECT id FROM public.pg19_sqlbody_public_target;
CREATE FUNCTION public.pg19_sqlbody_pub_view_count() RETURNS bigint
LANGUAGE sql STABLE
RETURN (SELECT count(*) FROM public.pg19_sqlbody_pub_view);


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
CREATE TABLE test_schema.rv_cascade_codes (code varchar(10) PRIMARY KEY);
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
CREATE FUNCTION test_schema.rv_cascade_score(x integer) RETURNS bigint
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
-- TO only: a CHECK, an expression index and a generated column (with an
-- index of its own) calling rv_cascade_score. The table phase adds them
-- before the routine phase drops the routine with CASCADE, which takes them
-- again: they are put back after it.
ALTER TABLE test_schema.rv_cascade_scores
    ADD CONSTRAINT rv_cascade_scores_qty_chk CHECK (test_schema.rv_cascade_score(qty) > -100),
    ADD COLUMN doubled bigint GENERATED ALWAYS AS (test_schema.rv_cascade_score(qty) * 2) STORED;
CREATE INDEX rv_cascade_scores_score_idx ON test_schema.rv_cascade_scores (test_schema.rv_cascade_score(qty));
CREATE INDEX rv_cascade_scores_doubled_idx ON test_schema.rv_cascade_scores (doubled);
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
-- TO adds a column whose default calls rv_cascade_is_valid, which is
-- dropped ahead of the tables: it is set once the routine is back.
CREATE FOREIGN TABLE test_schema.rv_cascade_ft (
    id      integer,
    code    text DEFAULT test_schema.rv_cascade_first_code(),
    preset  bigint DEFAULT test_schema.rv_cascade_score(9),
    checked boolean DEFAULT test_schema.rv_cascade_is_valid('ok')
) SERVER rv_cascade_srv;
CREATE FUNCTION test_schema.rv_cascade_score_reader() RETURNS bigint
LANGUAGE sql STABLE
RETURN test_schema.rv_cascade_score_or();
CREATE VIEW test_schema.rv_cascade_score_view AS
    SELECT test_schema.rv_cascade_score_reader() AS r;
-- The table phase fills the existing row with rv_cascade_score_or(), which
-- rv_cascade_score's CASCADE takes in the routine phase: it is not dropped
-- ahead of the tables, and the default comes back with it.
ALTER TABLE test_schema.rv_cascade_scores
    ADD COLUMN bonus bigint DEFAULT test_schema.rv_cascade_score_or();
ALTER TABLE test_schema.rv_cascade_orders
    ADD CONSTRAINT rv_cascade_orders_id_chk CHECK (id > 0);
-- TO only, calling the routines dropped ahead of the tables: the table phase
-- leaves them out and they are created once the routines are back.
ALTER TABLE test_schema.rv_cascade_orders
    ADD CONSTRAINT rv_cascade_orders_code_chk CHECK (test_schema.rv_cascade_is_valid(code));
ALTER TABLE test_schema.rv_cascade_orders
    ALTER COLUMN code SET DEFAULT test_schema.rv_cascade_first_code();
CREATE INDEX rv_cascade_orders_norm_idx
    ON test_schema.rv_cascade_orders (test_schema.rv_cascade_norm(code));
CREATE POLICY rv_cascade_orders_pol ON test_schema.rv_cascade_orders
    USING (test_schema.rv_cascade_is_valid(code));
CREATE TABLE test_schema.rv_cascade_fresh (
    code text DEFAULT test_schema.rv_cascade_first_code()
        CONSTRAINT rv_cascade_fresh_chk CHECK (test_schema.rv_cascade_is_valid(code))
);
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
CREATE FUNCTION data.rv_pcascade_plain_ret(x integer) RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$ SELECT x::bigint $$;
CREATE FUNCTION data.rv_pcascade_plain_new() RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
ALTER DEFAULT PRIVILEGES IN SCHEMA data GRANT EXECUTE ON FUNCTIONS TO pgc_pg19_pcascade_exec;
-- Another role's default privileges in the schema: the migration does not
-- know which role creates a routine, so it plans for this one too.
ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_pcascade_owner IN SCHEMA data
    GRANT EXECUTE ON FUNCTIONS TO pgc_pg19_pcascade_reader;
CREATE FUNCTION data.rv_pcascade_norm(t text) RETURNS varchar
LANGUAGE sql IMMUTABLE AS $$ SELECT lower(t)::varchar $$;
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
-- TO only: a new routine starts with the default privileges too.
CREATE FUNCTION data.rv_pcascade_new() RETURNS integer
LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;
REVOKE EXECUTE ON FUNCTION data.rv_pcascade_new() FROM pgc_pg19_pcascade_exec;
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
-- TO side (see schema_a.sql for the FROM side and why each case exists).
CREATE DOMAIN test_schema.pg19_domains_body AS integer
    CONSTRAINT pg19_domains_body_range CHECK (VALUE > 5);          -- MODIFIED: > 0 -> > 5
CREATE DOMAIN test_schema.pg19_domains_gain AS text               -- ADDED constraint (quoted name)
    CONSTRAINT "pg19_domains Gain ""q""" CHECK (VALUE <> '');
CREATE DOMAIN test_schema.pg19_domains_lose AS text;              -- REMOVED constraint
CREATE DOMAIN test_schema.pg19_domains_validate AS integer        -- VALIDATED
    CONSTRAINT pg19_domains_validate_nonneg CHECK (VALUE >= 0);
CREATE DOMAIN test_schema.pg19_domains_invalidate AS integer;     -- now NOT VALID
ALTER DOMAIN test_schema.pg19_domains_invalidate
    ADD CONSTRAINT pg19_domains_invalidate_nonneg CHECK (VALUE >= 0) NOT VALID;
-- Unchanged (declared in the opposite order to FROM).
CREATE DOMAIN test_schema.pg19_domains_same AS integer
    CONSTRAINT pg19_domains_same_max CHECK (VALUE < 100)
    CONSTRAINT "Pg19_domains_same_min" CHECK (VALUE > 1);
CREATE TYPE test_schema.pg19_domains_level AS ENUM ('low', 'medium', 'high');  -- MODIFIED: 'medium' inserted
-- MODIFIED: the CHECK now calls a routine only TO has (the old one is gone).
CREATE FUNCTION test_schema.pg19_domains_is_sku(v text) RETURNS boolean
    LANGUAGE sql IMMUTABLE AS $$ SELECT v ~ '^[A-Z]+$' $$;
CREATE DOMAIN test_schema.pg19_domains_sku AS text
    CONSTRAINT pg19_domains_sku_fmt CHECK (test_schema.pg19_domains_is_sku(VALUE));
-- old_qty dropped; level defaults to the new enum label, which a
-- single-transaction migration can only use once the label is committed.
CREATE TABLE test_schema.pg19_domains_usage (
    id      integer PRIMARY KEY,
    qty     test_schema.pg19_domains_body,
    code    test_schema.pg19_domains_gain,
    tag     test_schema.pg19_domains_lose,
    level   test_schema.pg19_domains_level DEFAULT 'medium',
    sku     test_schema.pg19_domains_sku
);
INSERT INTO test_schema.pg19_domains_usage VALUES (1, 10, 'ok', 'short', 'high', 'ABC');
-- PG19: the IS JSON CHECK changes ARRAY -> OBJECT (drop + re-add).
CREATE DOMAIN test_schema.pg19_domains_jsonb AS jsonb;
CREATE DOMAIN test_schema.pg19_domains_json_doc AS test_schema.pg19_domains_jsonb;
ALTER DOMAIN test_schema.pg19_domains_json_doc
    ADD CONSTRAINT pg19_domains_json_doc_shape CHECK (VALUE IS JSON OBJECT);

-- ==== END fix:domains ====

-- ==== BEGIN fix:grants — REVOKE of privileges granted by a non-owner (GRANTED BY) ====
-- =============================================================================
-- Grantor chains in full grants mode (TO side)
-- =============================================================================
-- TO side of the grantor-chain case; see schema_a.sql. The delegates keep
-- their grant options (except on pg19_grants_opt) and everything they granted
-- is gone.
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
GRANT SELECT, UPDATE ON public.pg19_grants_chain TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.pg19_grants_chain TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.pg19_grants_opt TO pgc_pg19_grants_delegate;  -- MODIFIED: grant option removed
GRANT SELECT ON public.pg19_grants_opt TO pgc_pg19_grants_reader;    -- MODIFIED: granted by the owner
GRANT USAGE ON SEQUENCE public.pg19_grants_seq TO "pgc_pg19_Grants_Mixed" WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION public.pg19_grants_fn() TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT USAGE ON SCHEMA public TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.pg19_grants_granted_by, public.pg19_grants_same
    TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT ON public.pg19_grants_same TO pgc_pg19_grants_reader;  -- grantor-only change on PG19: no diff
-- Table-level REVOKEs reach the columns (see schema_a.sql)
CREATE TABLE public.pg19_grants_colkeep (id integer PRIMARY KEY, note text);
ALTER TABLE public.pg19_grants_colkeep OWNER TO pgc_pg19_grants_owner;
CREATE TABLE public.pg19_grants_coldep (id integer PRIMARY KEY, note text);
ALTER TABLE public.pg19_grants_coldep OWNER TO pgc_pg19_grants_owner;
GRANT SELECT ON public.pg19_grants_colkeep TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.pg19_grants_colkeep
    TO pgc_pg19_grants_reader, pgc_pg19_grants_sub;                   -- MODIFIED: column grants only, from the owner
GRANT SELECT ON public.pg19_grants_coldep TO pgc_pg19_grants_delegate; -- MODIFIED: table-level grant option removed
GRANT SELECT (note) ON public.pg19_grants_coldep TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.pg19_grants_coldep TO pgc_pg19_grants_reader; -- MODIFIED: granted by the owner
-- Privileges the table DDL itself takes away (reg-misc; see schema_a.sql)
CREATE TABLE public.rv_regmisc_grants_readd (
    id integer PRIMARY KEY, qty integer,
    g integer GENERATED ALWAYS AS (qty * 2) STORED,     -- MODIFIED: plain -> generated
    note text
);
ALTER TABLE public.rv_regmisc_grants_readd OWNER TO pgc_pg19_grants_owner;
GRANT SELECT (g) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_reader;
GRANT SELECT (g) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (note) ON public.rv_regmisc_grants_readd TO pgc_pg19_grants_reader;
CREATE TABLE public.rv_regmisc_grants_rebuilt (id integer NOT NULL, k integer NOT NULL, code text)
    PARTITION BY RANGE (k);                             -- MODIFIED: partition key
ALTER TABLE public.rv_regmisc_grants_rebuilt OWNER TO pgc_pg19_grants_owner;
GRANT SELECT ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_delegate WITH GRANT OPTION;
GRANT SELECT (code) ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_reader;
GRANT SELECT (code) ON public.rv_regmisc_grants_rebuilt TO pgc_pg19_grants_delegate WITH GRANT OPTION;
CREATE TABLE public.rv_regmisc_grants_pt (id integer NOT NULL, v bigint) PARTITION BY RANGE (id); -- MODIFIED: v bigint
CREATE TABLE public.rv_regmisc_grants_pt1 PARTITION OF public.rv_regmisc_grants_pt FOR VALUES FROM (0) TO (100);
ALTER TABLE public.rv_regmisc_grants_pt OWNER TO pgc_pg19_grants_owner;
ALTER TABLE public.rv_regmisc_grants_pt1 OWNER TO pgc_pg19_grants_owner;
GRANT SELECT ON public.rv_regmisc_grants_pt1 TO pgc_pg19_grants_delegate WITH GRANT OPTION;

-- pre-grants (rv_pgrants_*): see schema_a.sql.
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
GRANT SELECT ON public.rv_pgrants_tbl TO "pgc_pg19_Pgrants_Reader";        -- MODIFIED: added
GRANT UPDATE (note) ON public.rv_pgrants_tbl TO "pgc_pg19_Pgrants_Reader"; -- MODIFIED: added
GRANT INSERT ON public.rv_pgrants_tbl TO "pgc_pg19_pgrants reader";        -- MODIFIED: added
CREATE TABLE public.rv_pgrants_tbl_revoke (id integer PRIMARY KEY);         -- MODIFIED: grant removed
CREATE TABLE public.rv_pgrants_chain (id integer PRIMARY KEY);              -- MODIFIED: grants removed
CREATE FUNCTION public.rv_pgrants_secret() RETURNS integer LANGUAGE sql SECURITY DEFINER AS 'SELECT 42';
REVOKE EXECUTE ON FUNCTION public.rv_pgrants_secret() FROM PUBLIC;         -- MODIFIED
CREATE DOMAIN public.rv_pgrants_dom AS integer;
REVOKE USAGE ON DOMAIN public.rv_pgrants_dom FROM PUBLIC;                   -- MODIFIED
CREATE TYPE public.rv_pgrants_enum AS ENUM ('a', 'b');
REVOKE USAGE ON TYPE public.rv_pgrants_enum FROM PUBLIC;                    -- MODIFIED
CREATE FUNCTION public.rv_pgrants_granted() RETURNS integer LANGUAGE sql AS 'SELECT 1';
GRANT EXECUTE ON FUNCTION public.rv_pgrants_granted() TO "pgc_pg19_Pgrants_Reader"; -- MODIFIED
REVOKE EXECUTE ON FUNCTION public.rv_pgrants_granted() FROM PUBLIC;         -- MODIFIED
CREATE FUNCTION public.rv_pgrants_reopen() RETURNS integer LANGUAGE sql AS 'SELECT 1'; -- MODIFIED: NULL ACL
CREATE FUNCTION public.rv_pgrants_kept() RETURNS integer LANGUAGE sql AS 'SELECT 1';   -- MODIFIED: NULL ACL
CREATE FUNCTION public.rv_pgrants_emptied() RETURNS integer LANGUAGE sql AS 'SELECT 1'; -- MODIFIED: NULL ACL
CREATE FUNCTION public.rv_pgrants_new_fn() RETURNS integer LANGUAGE sql AS 'SELECT 1';  -- NEW
REVOKE EXECUTE ON FUNCTION public.rv_pgrants_new_fn() FROM PUBLIC;
CREATE DOMAIN public.rv_pgrants_new_dom AS text;                            -- NEW
REVOKE USAGE ON DOMAIN public.rv_pgrants_new_dom FROM PUBLIC;
-- NEW. FOR ROLE pgc_pg19_grants_owner, which creates nothing here, so no other
-- object picks it up.
ALTER DEFAULT PRIVILEGES FOR ROLE pgc_pg19_grants_owner IN SCHEMA public
    GRANT SELECT ON TABLES TO "pgc_pg19_Pgrants_Reader";

-- pre-grants review: see schema_a.sql. The default privileges are unchanged.
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
CREATE FUNCTION test_order.rv_pgrants_app_fr() RETURNS bigint LANGUAGE sql SECURITY DEFINER AS 'SELECT 1::bigint'; -- MODIFIED: returns bigint
REVOKE EXECUTE ON FUNCTION test_order.rv_pgrants_app_fr() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION test_order.rv_pgrants_app_fr() TO "pgc_pg19_Pgrants_Reader";
ALTER FUNCTION test_order.rv_pgrants_app_fr() OWNER TO pgc_pg19_pgrants_app;
CREATE FUNCTION test_order.rv_pgrants_app_new() RETURNS integer LANGUAGE sql AS 'SELECT 1'; -- NEW
REVOKE EXECUTE ON FUNCTION test_order.rv_pgrants_app_new() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION test_order.rv_pgrants_app_new() TO "pgc_pg19_Pgrants_Reader";
ALTER FUNCTION test_order.rv_pgrants_app_new() OWNER TO pgc_pg19_pgrants_app;
CREATE DOMAIN public.rv_pgrants_app_dom AS integer;                      -- NEW
REVOKE USAGE ON DOMAIN public.rv_pgrants_app_dom FROM PUBLIC;
ALTER DOMAIN public.rv_pgrants_app_dom OWNER TO pgc_pg19_pgrants_app;
CREATE TYPE public.rv_pgrants_range AS RANGE (subtype = float8);         -- MODIFIED: no subtype_diff
REVOKE USAGE ON TYPE public.rv_pgrants_range FROM PUBLIC;
GRANT USAGE ON TYPE public.rv_pgrants_range TO "pgc_pg19_Pgrants_Reader";

-- ==== END fix:grants ====

-- ==== BEGIN fix:replident — REPLICA IDENTITY USING INDEX ====
-- TO side of the REPLICA IDENTITY USING INDEX transitions (see schema_a.sql).
CREATE TABLE test_schema.pg19_replident_enter (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_enter_code_uk ON test_schema.pg19_replident_enter (code);
ALTER TABLE test_schema.pg19_replident_enter REPLICA IDENTITY USING INDEX pg19_replident_enter_code_uk;
CREATE TABLE test_schema.pg19_replident_switch (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_switch_a_uk ON test_schema.pg19_replident_switch (a);
CREATE UNIQUE INDEX pg19_replident_switch_b_uk ON test_schema.pg19_replident_switch (b);
ALTER TABLE test_schema.pg19_replident_switch REPLICA IDENTITY USING INDEX pg19_replident_switch_b_uk;
CREATE TABLE test_schema.pg19_replident_leave (id INT PRIMARY KEY, code INT);
CREATE UNIQUE INDEX pg19_replident_leave_code_uk ON test_schema.pg19_replident_leave (code);
CREATE TABLE test_schema.pg19_replident_full (id INT PRIMARY KEY, code INT NOT NULL);
ALTER TABLE test_schema.pg19_replident_full REPLICA IDENTITY FULL;
CREATE TABLE test_schema.pg19_replident_rebuild (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_rebuild_uk ON test_schema.pg19_replident_rebuild (a, b);
ALTER TABLE test_schema.pg19_replident_rebuild REPLICA IDENTITY USING INDEX pg19_replident_rebuild_uk;
CREATE TABLE test_schema.pg19_replident_ukey (id INT PRIMARY KEY, code INT NOT NULL CONSTRAINT pg19_replident_ukey_code_key UNIQUE);
ALTER TABLE test_schema.pg19_replident_ukey REPLICA IDENTITY USING INDEX pg19_replident_ukey_code_key;
CREATE TABLE test_schema.pg19_replident_ukey_rebuild (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, CONSTRAINT pg19_replident_ukey_rebuild_key UNIQUE (a, b));
ALTER TABLE test_schema.pg19_replident_ukey_rebuild REPLICA IDENTITY USING INDEX pg19_replident_ukey_rebuild_key;
CREATE TABLE test_schema.pg19_replident_pkey (id INT CONSTRAINT pg19_replident_pkey_pk PRIMARY KEY, code INT);
ALTER TABLE test_schema.pg19_replident_pkey REPLICA IDENTITY USING INDEX pg19_replident_pkey_pk;
CREATE TABLE test_schema.pg19_replident_orphan (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_orphan_code_uk ON test_schema.pg19_replident_orphan (code);
ALTER TABLE test_schema.pg19_replident_orphan REPLICA IDENTITY USING INDEX pg19_replident_orphan_code_uk;
-- The inline named NOT NULL works on every version (a pg_constraint row on
-- PG18+, name ignored before), so TO is validated everywhere.
CREATE TABLE test_schema.pg19_replident_notvalid (id INT PRIMARY KEY, code INT CONSTRAINT pg19_replident_notvalid_code_nn NOT NULL);
CREATE UNIQUE INDEX pg19_replident_notvalid_code_uk ON test_schema.pg19_replident_notvalid (code);
ALTER TABLE test_schema.pg19_replident_notvalid REPLICA IDENTITY USING INDEX pg19_replident_notvalid_code_uk;
CREATE TABLE test_schema.pg19_replident_part (id INT NOT NULL, k INT NOT NULL) PARTITION BY RANGE (id);
CREATE TABLE test_schema.pg19_replident_part_1 PARTITION OF test_schema.pg19_replident_part FOR VALUES FROM (0) TO (100);
CREATE UNIQUE INDEX pg19_replident_part_uk ON test_schema.pg19_replident_part (id, k);
ALTER TABLE test_schema.pg19_replident_part REPLICA IDENTITY USING INDEX pg19_replident_part_uk;
-- TO-only tables created with USING INDEX: on a plain unique index (built
-- after commit under --output-for-production, so USING INDEX follows it
-- there) and on a UNIQUE constraint's index (built by CREATE TABLE).
CREATE TABLE test_schema.pg19_replident_new (id INT PRIMARY KEY, code INT NOT NULL);
CREATE UNIQUE INDEX pg19_replident_new_code_uk ON test_schema.pg19_replident_new (code);
ALTER TABLE test_schema.pg19_replident_new REPLICA IDENTITY USING INDEX pg19_replident_new_code_uk;
CREATE TABLE test_schema.pg19_replident_new_ukey (id INT PRIMARY KEY, code INT NOT NULL CONSTRAINT pg19_replident_new_ukey_code_key UNIQUE);
ALTER TABLE test_schema.pg19_replident_new_ukey REPLICA IDENTITY USING INDEX pg19_replident_new_ukey_code_key;
CREATE TABLE test_schema.pg19_replident_genrewrite (id INT PRIMARY KEY, code INT GENERATED ALWAYS AS (id * 2) STORED NOT NULL);
CREATE UNIQUE INDEX pg19_replident_genrewrite_code_uk ON test_schema.pg19_replident_genrewrite (code);
ALTER TABLE test_schema.pg19_replident_genrewrite REPLICA IDENTITY USING INDEX pg19_replident_genrewrite_code_uk;
CREATE TABLE test_schema.pg19_replident_unrel (id INT PRIMARY KEY, code INT NOT NULL, g INT GENERATED ALWAYS AS (code * 2) STORED);
CREATE UNIQUE INDEX pg19_replident_unrel_code_uk ON test_schema.pg19_replident_unrel (code);
ALTER TABLE test_schema.pg19_replident_unrel REPLICA IDENTITY USING INDEX pg19_replident_unrel_code_uk;
-- The parent index is on a column named so that the index PostgreSQL creates
-- for each partition (<partition>_<column>_idx) and the one
-- --output-for-production builds concurrently (<partition>_<parent index>) get
-- the same name, and the fixture converges in both modes.
CREATE TABLE test_schema.pg19_replident_pp (pg19_replident_ppk INT NOT NULL, v INT) PARTITION BY RANGE (pg19_replident_ppk);
CREATE TABLE test_schema.pg19_replident_pp_p1 PARTITION OF test_schema.pg19_replident_pp FOR VALUES FROM (0) TO (100);
CREATE UNIQUE INDEX pg19_replident_ppk_idx ON test_schema.pg19_replident_pp (pg19_replident_ppk);
CREATE TABLE test_schema.pg19_replident_pp_p2 PARTITION OF test_schema.pg19_replident_pp FOR VALUES FROM (100) TO (200);
ALTER TABLE test_schema.pg19_replident_pp_p1 REPLICA IDENTITY USING INDEX pg19_replident_pp_p1_pg19_replident_ppk_idx;
ALTER TABLE test_schema.pg19_replident_pp_p2 REPLICA IDENTITY USING INDEX pg19_replident_pp_p2_pg19_replident_ppk_idx;
CREATE TABLE test_schema.pg19_replident_pkpart (id INT NOT NULL, v INT, CONSTRAINT pg19_replident_pkpart_pkey PRIMARY KEY (id)) PARTITION BY RANGE (id);
CREATE TABLE test_schema.pg19_replident_pkpart_k1 PARTITION OF test_schema.pg19_replident_pkpart FOR VALUES FROM (0) TO (100);
CREATE TABLE test_schema.pg19_replident_pkpart_k2 PARTITION OF test_schema.pg19_replident_pkpart FOR VALUES FROM (100) TO (200);
ALTER TABLE test_schema.pg19_replident_pkpart_k1 REPLICA IDENTITY USING INDEX pg19_replident_pkpart_k1_pkey;
ALTER TABLE test_schema.pg19_replident_pkpart_k2 REPLICA IDENTITY USING INDEX pg19_replident_pkpart_k2_pkey;
-- ==== END fix:replident ====

-- ==== BEGIN fix:rules — rule changes via CREATE OR REPLACE RULE ====

-- TO side; see the FROM side for what each case covers.
CREATE TABLE test_schema.pg19_rules_src (
    id  integer PRIMARY KEY,
    val text,
    qty bigint
);

CREATE TABLE test_schema.pg19_rules_log (
    id integer,
    op text
);

CREATE RULE pg19_rules_chg AS ON INSERT TO test_schema.pg19_rules_src
    DO INSTEAD (
        INSERT INTO test_schema.pg19_rules_log (id, op) VALUES (NEW.id, 'v2');
        INSERT INTO test_schema.pg19_rules_log (id, op) VALUES (NEW.id, 'v2-second');
    );

CREATE TABLE test_schema."Pg19 Rules Mixed" (
    id   integer,
    flag boolean
);
CREATE RULE "Pg19 Rules Where" AS ON DELETE TO test_schema."Pg19 Rules Mixed"
    WHERE NOT OLD.flag DO INSTEAD NOTHING;
COMMENT ON RULE "Pg19 Rules Where" ON test_schema."Pg19 Rules Mixed" IS 'rule comment (to)';

CREATE RULE pg19_rules_qty AS ON UPDATE TO test_schema.pg19_rules_src
    WHERE NEW.qty < 0 DO INSTEAD NOTHING;

CREATE TABLE test_schema.pg19_rules_vbase (
    id    integer PRIMARY KEY,
    name  text,
    extra text
);
CREATE VIEW test_schema.pg19_rules_view AS
SELECT id, name FROM test_schema.pg19_rules_vbase;
CREATE RULE pg19_rules_view_ins AS ON INSERT TO test_schema.pg19_rules_view
    DO INSTEAD INSERT INTO test_schema.pg19_rules_vbase (id, name) VALUES (NEW.id, NEW.name);

CREATE TABLE test_schema.pg19_rules_part (
    id integer,
    k  integer
) PARTITION BY LIST (k);
CREATE RULE pg19_rules_part_del AS ON DELETE TO test_schema.pg19_rules_part
    DO INSTEAD NOTHING;

CREATE FUNCTION test_schema.pg19_rules_tag(p integer) RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$ SELECT p * 10::bigint $$;
CREATE RULE pg19_rules_tagged AS ON UPDATE TO test_schema.pg19_rules_log
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    VALUES (test_schema.pg19_rules_tag(NEW.id), 'tagged');

CREATE VIEW test_schema.pg19_rules_tag_view AS
SELECT id, op FROM test_schema.pg19_rules_log
WHERE test_schema.pg19_rules_tag(id) > 0;
CREATE RULE pg19_rules_tag_view_del AS ON DELETE TO test_schema.pg19_rules_tag_view
    DO INSTEAD DELETE FROM test_schema.pg19_rules_log WHERE pg19_rules_log.id = OLD.id;

-- Added: a rule on a TO-only view whose action calls a TO-only function.
-- Both only exist once the Routines & Views section has run, so rules are
-- created after it.
CREATE FUNCTION test_schema.pg19_rules_fn(p integer) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT 'n' || p::text $$;
CREATE VIEW test_schema.pg19_rules_new_view AS
SELECT id, val FROM test_schema.pg19_rules_src;
CREATE RULE pg19_rules_new_view_ins AS ON INSERT TO test_schema.pg19_rules_new_view
    DO INSTEAD INSERT INTO test_schema.pg19_rules_log (id, op)
    VALUES (NEW.id, test_schema.pg19_rules_fn(NEW.id));

CREATE FUNCTION test_schema.pg19_rules_tag(p text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT p || '!' $$;
CREATE VIEW test_schema.pg19_rules_tag_txt_view AS
SELECT id, test_schema.pg19_rules_tag(op) AS tagged FROM test_schema.pg19_rules_log;
CREATE RULE pg19_rules_tag_txt_del AS ON DELETE TO test_schema.pg19_rules_tag_txt_view
    DO INSTEAD DELETE FROM test_schema.pg19_rules_log WHERE pg19_rules_log.id = OLD.id;

CREATE TABLE test_schema.pg19_rules_reader (
    id integer
);
CREATE RULE pg19_rules_reads_tag_view AS ON INSERT TO test_schema.pg19_rules_reader
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    SELECT id, 'via view' FROM test_schema.pg19_rules_tag_view;

-- pg19_rules_tv is a view here (a table in FROM), with the same rule.
CREATE VIEW test_schema.pg19_rules_tv AS
SELECT id FROM test_schema.pg19_rules_log;
CREATE RULE pg19_rules_tv_notify AS ON INSERT TO test_schema.pg19_rules_tv
    DO ALSO NOTIFY pg19_rules_chan;

-- pg19_rules_ft is gone; the rule that read it now uses NEW.
CREATE RULE pg19_rules_ft_read AS ON UPDATE TO test_schema.pg19_rules_reader
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op) VALUES (NEW.id, 'ft');
CREATE FOREIGN TABLE test_schema.pg19_rules_ft2 (
    id integer,
    v  bigint
) SERVER test_foreign_server
OPTIONS (schema_name 'public', table_name 'pg19_rules_ft2');
CREATE RULE pg19_rules_ft2_read AS ON DELETE TO test_schema.pg19_rules_reader
    DO ALSO INSERT INTO test_schema.pg19_rules_log (id, op)
    SELECT v, 'ft2' FROM test_schema.pg19_rules_ft2;

CREATE TABLE test_schema.pg19_rules_upsert_target (
    id  integer PRIMARY KEY,
    val text
);
CREATE TABLE test_schema.pg19_rules_upsert_front (
    id  integer PRIMARY KEY,
    val text
);
-- PG19+: ON CONFLICT DO SELECT with a lock strength in a rule action. A
-- syntax error before PostgreSQL 19 (fails harmlessly under ON_ERROR_STOP=0).
CREATE RULE pg19_rules_upsert AS ON INSERT TO test_schema.pg19_rules_upsert_front
    DO INSTEAD INSERT INTO test_schema.pg19_rules_upsert_target (id, val)
    VALUES (NEW.id, NEW.val) ON CONFLICT (id) DO SELECT FOR UPDATE
    RETURNING pg19_rules_upsert_target.id, pg19_rules_upsert_target.val;

-- ==== END fix:rules ====

-- ==== BEGIN fix:serial — serial detection for non-integer nextval columns ====
-- TO side. pgc writes a new table's column as serial / bigserial / smallserial
-- only when that pseudo-type recreates the owned sequence exactly; every other
-- owned sequence is created before the tables and gets OWNED BY after them.
-- Only the ALTER ... TYPE oid8 needs PG19; on PG14-18 it fails, the column
-- stays bigint, and that table is an ordinary bigserial.

-- numeric column, owned sequence with the serial-looking name
CREATE SEQUENCE test_schema.pg19_serial_num_id_seq;
CREATE TABLE test_schema.pg19_serial_num (
    id    numeric NOT NULL DEFAULT nextval('test_schema.pg19_serial_num_id_seq'),
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_num_id_seq OWNED BY test_schema.pg19_serial_num.id;

-- bigint column, custom-named owned sequence
CREATE SEQUENCE test_schema.pg19_serial_custom_counter;
CREATE TABLE test_schema.pg19_serial_custom (
    id    bigint NOT NULL DEFAULT nextval('test_schema.pg19_serial_custom_counter'),
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_custom_counter OWNED BY test_schema.pg19_serial_custom.id;

-- oid8 column (PG19+): cannot be an identity column, so an owned sequence is
-- the way to auto-number it
CREATE SEQUENCE test_schema.pg19_serial_oid8_id_seq;
CREATE TABLE test_schema.pg19_serial_oid8 (
    id    bigint NOT NULL DEFAULT nextval('test_schema.pg19_serial_oid8_id_seq'),
    label text
);
ALTER TABLE test_schema.pg19_serial_oid8 ALTER COLUMN id TYPE oid8;
ALTER SEQUENCE test_schema.pg19_serial_oid8_id_seq OWNED BY test_schema.pg19_serial_oid8.id;

-- integer column, bigint owned sequence
CREATE SEQUENCE test_schema.pg19_serial_mismatch_id_seq AS bigint;
CREATE TABLE test_schema.pg19_serial_mismatch (
    id    integer NOT NULL DEFAULT nextval('test_schema.pg19_serial_mismatch_id_seq'),
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_mismatch_id_seq OWNED BY test_schema.pg19_serial_mismatch.id;

-- genuine serial / bigserial / smallserial (output unchanged: pseudo-types)
CREATE TABLE test_schema.pg19_serial_genuine (
    id    serial PRIMARY KEY,
    big   bigserial,
    small smallserial,
    label text
);

-- existing table gains a serial column
CREATE TABLE test_schema.pg19_serial_add (
    label text,
    id    serial
);

-- existing column becomes serial
CREATE TABLE test_schema.pg19_serial_alter (
    id    serial,
    label text
);

-- serial whose sequence was tuned and commented afterwards
CREATE TABLE test_schema.pg19_serial_tuned (
    id    serial PRIMARY KEY,
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_tuned_id_seq INCREMENT BY 10 CACHE 5;
COMMENT ON SEQUENCE test_schema.pg19_serial_tuned_id_seq IS 'Tuned serial sequence';

-- serial made nullable afterwards
CREATE TABLE test_schema.pg19_serial_nullable (
    id    serial,
    label text
);
ALTER TABLE test_schema.pg19_serial_nullable ALTER COLUMN id DROP NOT NULL;

-- serial of a renamed table: the sequence keeps the old name
CREATE TABLE test_schema.pg19_serial_renamed_old (
    id    serial,
    label text
);
ALTER TABLE test_schema.pg19_serial_renamed_old RENAME TO pg19_serial_renamed;

-- serial sequences whose persistence was changed on their own (PG15+, where a
-- serial sequence takes its table's persistence; on PG14 the ALTER SEQUENCE
-- fails and both are ordinary serials)
CREATE TABLE test_schema.pg19_serial_seq_unlogged (
    id    serial,
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_seq_unlogged_id_seq SET UNLOGGED;
CREATE UNLOGGED TABLE test_schema.pg19_serial_seq_logged (
    id    serial,
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_seq_logged_id_seq SET LOGGED;

-- owned sequence on a column without a nextval default
CREATE SEQUENCE test_schema.pg19_serial_plain_seq;
CREATE TABLE test_schema.pg19_serial_plain (
    id    integer,
    label text
);
ALTER SEQUENCE test_schema.pg19_serial_plain_seq OWNED BY test_schema.pg19_serial_plain.id;

-- partition with its own serial-shaped owned sequence: CREATE TABLE ...
-- PARTITION OF has no column types, so it cannot declare a serial column
CREATE TABLE test_schema.pg19_serial_part (
    id    bigint NOT NULL,
    label text
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.pg19_serial_part_p1 PARTITION OF test_schema.pg19_serial_part
    FOR VALUES FROM (0) TO (1000000);
CREATE SEQUENCE test_schema.pg19_serial_part_p1_id_seq;
ALTER TABLE test_schema.pg19_serial_part_p1
    ALTER COLUMN id SET DEFAULT nextval('test_schema.pg19_serial_part_p1_id_seq');
ALTER SEQUENCE test_schema.pg19_serial_part_p1_id_seq OWNED BY test_schema.pg19_serial_part_p1.id;

-- genuine bigserial of a partitioned table: its partition repeats the default
-- and the pseudo-type is still used (output unchanged)
CREATE TABLE test_schema.pg19_serial_gpart (
    id    bigserial,
    label text
) PARTITION BY RANGE (id);
CREATE TABLE test_schema.pg19_serial_gpart_p1 PARTITION OF test_schema.pg19_serial_gpart
    FOR VALUES FROM (0) TO (1000000);

-- identity column that also owns an ordinary sequence: only the identity
-- sequence comes with the column
CREATE TABLE test_schema.pg19_serial_ident (
    id    integer GENERATED ALWAYS AS IDENTITY,
    label text
);
CREATE SEQUENCE test_schema.pg19_serial_ident_extra OWNED BY test_schema.pg19_serial_ident.id;

-- the same shape in FROM (schema_a.sql) with a second owned sequence that TO
-- drops; only the identity sequence goes with the identity
CREATE TABLE test_schema.pg19_serial_ident_drop (
    id    integer GENERATED ALWAYS AS IDENTITY,
    label text
);

-- serial whose sequence another new table's default names; that table sorts
-- first, so the sequence has to exist before every table
CREATE TABLE test_schema.pg19_serial_shared (
    id    serial PRIMARY KEY,
    label text
);
CREATE TABLE test_schema.pg19_serial_a_shared_ref (
    id    integer NOT NULL DEFAULT nextval('test_schema.pg19_serial_shared_id_seq'),
    label text
);

-- ==== END fix:serial ====

-- ==== BEGIN fix:comments — names containing newlines or comment delimiters in generated comments ====
-- TO side; see schema_a.sql. The victim is identical on both sides, so the
-- migration must leave it alone.
CREATE TABLE public.pg19_comments_victim (id integer PRIMARY KEY);
INSERT INTO public.pg19_comments_victim VALUES (1);

CREATE TABLE test_schema."pg19_comments_t*/drop table public.pg19_comments_victim;/*" (
    id                  integer PRIMARY KEY,
    "pg19_comments_c/*" integer,
    added               text
);
CREATE VIEW test_schema."pg19_comments_v/*" AS SELECT 1 AS a;
CREATE FUNCTION test_schema."pg19_comments_f*/"(a integer) RETURNS bigint
    LANGUAGE sql AS 'SELECT a::bigint';
-- created: a table whose identity sequence is named after it, and an enum
CREATE TABLE test_schema."pg19_comments_n/*" (
    id integer GENERATED ALWAYS AS IDENTITY
);
CREATE TYPE test_schema."pg19_comments_e*/" AS ENUM ('a', 'b');

-- Names in statements (see schema_a.sql).
CREATE MATERIALIZED VIEW test_schema."pg19_comments_Mv*/" AS SELECT 1 AS a, 2 AS b;
CREATE INDEX "pg19_comments_Mv_a" ON test_schema."pg19_comments_Mv*/" (a);
COMMENT ON COLUMN test_schema."pg19_comments_Mv*/".b IS 'added column';
-- created: a mixed-case materialized view
CREATE MATERIALIZED VIEW test_schema."pg19_comments_MvNew" AS SELECT 1 AS a;
-- The parent index is named <column>_idx, so the index PostgreSQL creates for
-- the partition (<partition>_<column>_idx) and the one --output-for-production
-- builds concurrently (<partition>_<parent index>) share one name.
CREATE TABLE test_schema.pg19_comments_rp (pg19_comments_rpk integer NOT NULL, v integer)
    PARTITION BY RANGE (pg19_comments_rpk);
CREATE UNIQUE INDEX pg19_comments_rpk_idx ON test_schema.pg19_comments_rp (pg19_comments_rpk);
CREATE TABLE test_schema."pg19_comments_rp$pgc$1" PARTITION OF test_schema.pg19_comments_rp
    FOR VALUES FROM (0) TO (100);
ALTER TABLE test_schema."pg19_comments_rp$pgc$1"
    REPLICA IDENTITY USING INDEX "pg19_comments_rp$pgc$1_pg19_comments_rpk_idx";
CREATE FOREIGN TABLE test_schema.pg19_comments_ft (id integer)
    SERVER test_foreign_server OPTIONS (schema_name 'public', table_name 'b', updatable 'false');

-- ==== END fix:comments ====
