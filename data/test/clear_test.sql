-- Clear Command Test Schema
-- This schema creates a representative set of PostgreSQL objects across
-- multiple schemas to verify that the "pgc --command clear" command
-- generates a correct drop-all script in the proper dependency order.
--
-- Apply this script to a fresh database, then run:
--   pgc --command clear --database <db> --scheme "clear_app|clear_shared" \
--       --output clear_result.sql --use-single-transaction --use-comments
--
-- The resulting clear_result.sql should drop every object below in the
-- correct dependency-safe order.

-- ============================================================
-- 1. Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS clear_app;
CREATE SCHEMA IF NOT EXISTS clear_shared;

-- ============================================================
-- 2. Extensions
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS "pg_trgm"   WITH SCHEMA public;

-- ============================================================
-- 3. Custom Types
-- ============================================================

-- Enum type
CREATE TYPE clear_app.order_status AS ENUM ('new', 'processing', 'shipped', 'delivered', 'cancelled');

-- Composite type
CREATE TYPE clear_app.full_name AS (
    first_name VARCHAR(50),
    last_name  VARCHAR(50)
);

-- Domain type
CREATE DOMAIN clear_app.positive_int AS INTEGER CHECK (VALUE > 0);

-- ============================================================
-- 4. Sequences
-- ============================================================
CREATE SEQUENCE clear_app.customer_id_seq
    START WITH 1000
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 10;

CREATE SEQUENCE clear_shared.audit_id_seq
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    CACHE 1;

-- ============================================================
-- 5. Tables
-- ============================================================

-- Parent tables (no FK dependencies)
CREATE TABLE clear_app.customers (
    id INTEGER PRIMARY KEY DEFAULT nextval('clear_app.customer_id_seq'),
    name clear_app.full_name NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    status clear_app.order_status DEFAULT 'new',
    loyalty_points clear_app.positive_int DEFAULT 1,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE clear_app.categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE
);

-- Child table with foreign keys to customers and categories
CREATE TABLE clear_app.orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id INTEGER NOT NULL REFERENCES clear_app.customers(id),
    category_id INTEGER REFERENCES clear_app.categories(id) ON DELETE SET NULL,
    amount DECIMAL(12, 2) NOT NULL CHECK (amount >= 0),
    status clear_app.order_status DEFAULT 'new',
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Grandchild table with FK to orders
CREATE TABLE clear_app.order_items (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES clear_app.orders(id) ON DELETE CASCADE,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    total DECIMAL(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);

-- Table in shared schema
CREATE TABLE clear_shared.audit_log (
    id INTEGER PRIMARY KEY DEFAULT nextval('clear_shared.audit_id_seq'),
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_id VARCHAR(255),
    old_values JSONB,
    new_values JSONB,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Self-referencing table
CREATE TABLE clear_app.employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    manager_id INTEGER REFERENCES clear_app.employees(id),
    department VARCHAR(50)
);

-- ============================================================
-- 6. Indexes
-- ============================================================
CREATE INDEX idx_customers_email       ON clear_app.customers(email);
CREATE INDEX idx_customers_status      ON clear_app.customers(status);
CREATE INDEX idx_customers_metadata    ON clear_app.customers USING GIN(metadata);
CREATE INDEX idx_orders_customer_id    ON clear_app.orders(customer_id);
CREATE INDEX idx_orders_status         ON clear_app.orders(status);
CREATE INDEX idx_orders_created_at     ON clear_app.orders(created_at);
CREATE INDEX idx_order_items_order_id  ON clear_app.order_items(order_id);
CREATE INDEX idx_audit_log_table_name  ON clear_shared.audit_log(table_name);
CREATE INDEX idx_audit_log_changed_at  ON clear_shared.audit_log(changed_at);
CREATE INDEX idx_employees_manager     ON clear_app.employees(manager_id);
CREATE INDEX idx_employees_department  ON clear_app.employees(department);

-- ============================================================
-- 7. Functions
-- ============================================================

-- Trigger function
CREATE OR REPLACE FUNCTION clear_app.update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Regular function
CREATE OR REPLACE FUNCTION clear_app.get_customer_order_total(p_customer_id INTEGER)
RETURNS DECIMAL AS $$
BEGIN
    RETURN COALESCE((
        SELECT SUM(amount)
        FROM clear_app.orders
        WHERE customer_id = p_customer_id
    ), 0);
END;
$$ LANGUAGE plpgsql STABLE;

-- Function in shared schema
CREATE OR REPLACE FUNCTION clear_shared.format_audit_entry(p_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    SELECT table_name || ':' || operation || ':' || record_id
    INTO result
    FROM clear_shared.audit_log
    WHERE id = p_id;
    RETURN COALESCE(result, 'NOT FOUND');
END;
$$ LANGUAGE plpgsql STABLE;

-- SQL function
CREATE OR REPLACE FUNCTION clear_app.active_customer_count()
RETURNS BIGINT AS $$
    SELECT count(*) FROM clear_app.customers WHERE status = 'new';
$$ LANGUAGE sql STABLE;

-- ============================================================
-- 8. Procedures
-- ============================================================
CREATE OR REPLACE PROCEDURE clear_app.cleanup_old_orders(p_days INTEGER DEFAULT 365)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM clear_app.order_items
    WHERE order_id IN (
        SELECT id FROM clear_app.orders
        WHERE created_at < CURRENT_TIMESTAMP - (p_days || ' days')::INTERVAL
    );
    DELETE FROM clear_app.orders
    WHERE created_at < CURRENT_TIMESTAMP - (p_days || ' days')::INTERVAL;
END;
$$;

-- ============================================================
-- 9. Triggers
-- ============================================================
CREATE TRIGGER trg_customers_timestamp
    BEFORE INSERT ON clear_app.customers
    FOR EACH ROW
    EXECUTE FUNCTION clear_app.update_timestamp();

CREATE TRIGGER trg_orders_timestamp
    BEFORE INSERT ON clear_app.orders
    FOR EACH ROW
    EXECUTE FUNCTION clear_app.update_timestamp();

-- ============================================================
-- 10. Views
-- ============================================================

-- Regular view
CREATE VIEW clear_app.v_customer_summary AS
SELECT
    c.id,
    (c.name).first_name || ' ' || (c.name).last_name AS full_name,
    c.email,
    c.status,
    COUNT(o.id) AS order_count,
    COALESCE(SUM(o.amount), 0) AS total_spent
FROM clear_app.customers c
LEFT JOIN clear_app.orders o ON o.customer_id = c.id
GROUP BY c.id, c.name, c.email, c.status;

-- View depending on another view
CREATE VIEW clear_app.v_top_customers AS
SELECT *
FROM clear_app.v_customer_summary
WHERE total_spent > 1000
ORDER BY total_spent DESC;

-- Materialized view
CREATE MATERIALIZED VIEW clear_app.mv_daily_orders AS
SELECT
    DATE(created_at) AS order_date,
    COUNT(*)         AS order_count,
    SUM(amount)      AS total_amount
FROM clear_app.orders
GROUP BY DATE(created_at);

-- Materialized view in shared schema
CREATE MATERIALIZED VIEW clear_shared.mv_audit_stats AS
SELECT
    table_name,
    operation,
    COUNT(*) AS op_count
FROM clear_shared.audit_log
GROUP BY table_name, operation;

-- ============================================================
-- 11. Comments
-- ============================================================
COMMENT ON SCHEMA clear_app IS 'Application schema for clear command testing';
COMMENT ON SCHEMA clear_shared IS 'Shared schema for clear command testing';
COMMENT ON TABLE clear_app.customers IS 'Customer master table';
COMMENT ON TABLE clear_app.orders IS 'Customer orders';
COMMENT ON COLUMN clear_app.customers.loyalty_points IS 'Points earned by the customer';
COMMENT ON FUNCTION clear_app.get_customer_order_total(INTEGER) IS 'Returns total order amount for a customer';
COMMENT ON VIEW clear_app.v_customer_summary IS 'Aggregated customer statistics';
COMMENT ON MATERIALIZED VIEW clear_app.mv_daily_orders IS 'Daily order aggregation';
