
-- Project 2 PostgreSQL Database Schema

-- Create the customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(30),
    city VARCHAR(100),
    registration_date DATE,
    customer_type VARCHAR(30)
);

-- Create the products table
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    is_active BOOLEAN
);

-- Create the orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    product_id INTEGER REFERENCES products(product_id),
    order_date DATE,
    order_status VARCHAR(30),
    payment_method VARCHAR(30),
    total_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    shipping_cost DECIMAL(12,2)
);

-- Create the ETL audit table
CREATE TABLE IF NOT EXISTS etl_audit (
    audit_id SERIAL PRIMARY KEY,
    dag_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(30),
    records_processed INTEGER
);

-- Create the customer analytics table
CREATE TABLE IF NOT EXISTS customer_analytics (
    customer_id INTEGER PRIMARY KEY REFERENCES customers(customer_id),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(30),
    city VARCHAR(100),
    registration_date DATE,
    customer_type VARCHAR(30),
    lifetime_spending DECIMAL(12,2),
    order_frequency INTEGER,
    average_order_value DECIMAL(12,2)
);

-- Create the product analytics table
CREATE TABLE IF NOT EXISTS product_analytics (
    product_id INTEGER PRIMARY KEY REFERENCES products(product_id),
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    unit_price DECIMAL(10,2),
    stock_quantity INTEGER,
    is_active BOOLEAN,
    product_revenue DECIMAL(12,2),
    times_ordered INTEGER
);

-- Create the category performance table
CREATE TABLE IF NOT EXISTS category_performance (
    category VARCHAR(100) PRIMARY KEY,
    category_revenue DECIMAL(14,2),
    category_orders INTEGER
);

-- Create the average product price table
CREATE TABLE IF NOT EXISTS average_product_price (
    category VARCHAR(100) PRIMARY KEY,
    average_unit_price DECIMAL(10,2)
);

-- Create the inventory status table
CREATE TABLE IF NOT EXISTS inventory_status (
    is_active BOOLEAN PRIMARY KEY,
    product_count INTEGER,
    total_stock BIGINT
);

-- Create the daily sales table
CREATE TABLE IF NOT EXISTS daily_sales (
    order_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2)
);

-- Create the monthly sales table
CREATE TABLE IF NOT EXISTS monthly_sales (
    order_month VARCHAR(7) PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2)
);

-- Create the order status summary table
CREATE TABLE IF NOT EXISTS order_status_summary (
    order_status VARCHAR(30) PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2)
);

-- Create the payment summary table
CREATE TABLE IF NOT EXISTS payment_summary (
    payment_method VARCHAR(30) PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2)
);

-- Create the discount summary table
CREATE TABLE IF NOT EXISTS discount_summary (
    average_discount_rate DECIMAL(5,2)
);

-- Create the revenue by customer type table
CREATE TABLE IF NOT EXISTS revenue_by_customer_type (
    customer_type VARCHAR(30) PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2)
);

-- Create the categories by customer type table
CREATE TABLE IF NOT EXISTS categories_by_customer_type (
    customer_type VARCHAR(30),
    category VARCHAR(100),
    total_orders INTEGER,
    total_revenue DECIMAL(14,2),
    PRIMARY KEY (customer_type, category)
);

-- Create the city revenue table
CREATE TABLE IF NOT EXISTS city_revenue (
    city VARCHAR(100) PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2)
);
