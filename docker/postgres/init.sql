-- ============================================================================
-- PostgreSQL Initialization for Debezium CDC Lab
-- ============================================================================

-- 1. Create dedicated CDC replication user
CREATE USER cdc_user WITH REPLICATION PASSWORD 'cdc_password';

-- 2. Grant permissions on source database and schemas
GRANT CONNECT ON DATABASE inventory_db TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;

-- 3. Create domain tables
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    order_status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    total_amount NUMERIC(10, 2) NOT NULL,
    order_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Create Debezium Signal Table for Incremental & Ad-Hoc Snapshots
-- Allows triggering snapshots via SQL INSERT without connector restart or downtime
CREATE TABLE IF NOT EXISTS debezium_signal (
    id VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048) NULL
);

-- 5. REPLICA IDENTITY Configuration (Crucial Lead Interview Concept)
-- DEFAULT: Only old values of primary key columns are recorded on UPDATE/DELETE
-- FULL: Entire previous row is recorded in WAL, allowing Debezium to populate the "before" envelope field!
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;

-- 6. Grant access to replication user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT INSERT, UPDATE, DELETE ON TABLE debezium_signal TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- 7. Create Publication for Logical Replication
-- Debezium uses this publication to filter which tables stream into the WAL slot
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- 8. Seed initial baseline records (for snapshot phase observation)
INSERT INTO customers (first_name, last_name, email, status) VALUES
    ('Alice', 'Smith', 'alice.smith@example.com', 'ACTIVE'),
    ('Bob', 'Johnson', 'bob.johnson@example.com', 'ACTIVE'),
    ('Carol', 'Williams', 'carol.williams@example.com', 'SUSPENDED');

INSERT INTO products (name, category, price, stock_quantity) VALUES
    ('Mechanical Keyboard', 'Electronics', 129.99, 45),
    ('Ergonomic Mouse', 'Electronics', 79.50, 120),
    ('USB-C Dual Hub', 'Accessories', 39.99, 200);

INSERT INTO orders (customer_id, order_status, total_amount) VALUES
    (1, 'COMPLETED', 129.99),
    (2, 'PROCESSING', 79.50);
