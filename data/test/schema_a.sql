-- Schema A: Complex PostgreSQL schema for testing database comparison
-- This schema represents the "FROM" database in comparisons

-- Create schemas
CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE SCHEMA IF NOT EXISTS shared_schema;

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

CREATE OR REPLACE FUNCTION test_schema.get_users_by_status(p_status test_schema.status_type)
RETURNS TABLE(user_id integer, username varchar, email varchar)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY SELECT id, username, email FROM test_schema.users WHERE status = p_status;
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
COMMENT ON FUNCTION test_schema.get_users_by_status(test_schema.status_type) IS 'Returns users filtered by status (FROM)';

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

CREATE TABLE test_schema.composite_fk (
    id SERIAL PRIMARY KEY,
    ref_part_one INT,
    ref_part_two INT,
    CONSTRAINT fk_composite FOREIGN KEY (ref_part_one, ref_part_two) 
        REFERENCES test_schema.composite_pk (part_one, part_two)
);

-- Identity column test (Schema exists, table missing)
CREATE SCHEMA IF NOT EXISTS data;

-- Identity column update test (FROM)
CREATE TABLE test_schema.identity_update_test (
    id INT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1),
    payload TEXT
);

-- Partition bound change test (FROM: value 'active')
CREATE TABLE data.partition_bound_test (
    id int,
    status text,
    CONSTRAINT partition_bound_test_pkey PRIMARY KEY (id, status)
) PARTITION BY LIST (status);

CREATE TABLE data.partition_bound_test_active PARTITION OF data.partition_bound_test FOR VALUES IN ('active');

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

