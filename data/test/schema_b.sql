-- Schema B: Modified PostgreSQL schema for testing database comparison
-- This schema represents the "TO" database in comparisons
-- Contains differences to test all comparison scenarios

-- Create schemas (one removed, one added)
CREATE SCHEMA IF NOT EXISTS test_schema;
CREATE SCHEMA IF NOT EXISTS shared_schema;
CREATE SCHEMA IF NOT EXISTS new_reporting_schema;  -- NEW SCHEMA

-- Extensions (modified list)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
-- pgcrypto removed
CREATE EXTENSION IF NOT EXISTS "pg_trgm" WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS "hstore" WITH SCHEMA public;  -- NEW EXTENSION

-- Custom types (some modified, some removed, some added)
CREATE TYPE test_schema.status_type AS ENUM ('active', 'inactive', 'pending', 'suspended');  -- MODIFIED: added 'suspended'
-- priority_type removed
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
