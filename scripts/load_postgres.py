
import os
import psycopg2

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
)

print("Connected to PostgreSQL successfully!")

# Check how many tables exist
cursor = connection.cursor()

cursor.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE'
""")

table_count = cursor.fetchone()[0]
print("PostgreSQL tables found:", table_count)

# Read cleaned customer data
import pandas as pd

customers = pd.read_parquet("/data/cleaned/customers")

print("Cleaned customer records ready to load:", len(customers))
print("Customer columns:", list(customers.columns))

# Load cleaned customers into PostgreSQL
from psycopg2.extras import execute_values

customer_columns = [
    "customer_id", "first_name", "last_name", "email",
    "phone", "city", "registration_date", "customer_type"
]

customer_rows = list(
    customers[customer_columns].itertuples(index=False, name=None)
)

insert_customers = """
    INSERT INTO customers (
        customer_id, first_name, last_name, email,
        phone, city, registration_date, customer_type
    )
    VALUES %s
    ON CONFLICT (customer_id) DO NOTHING
"""

execute_values(
    cursor,
    insert_customers,
    customer_rows,
    page_size=1000
)

connection.commit()

# Verify the database count
cursor.execute("SELECT COUNT(*) FROM customers")
print("Customers loaded into PostgreSQL:", cursor.fetchone()[0])

# Read cleaned product data
products = pd.read_parquet("/data/cleaned/products")

print("Cleaned product records ready to load:", len(products))

# Prepare product records
product_columns = [
    "product_id", "product_name", "category", "brand",
    "unit_price", "stock_quantity", "is_active"
]

product_rows = list(
    products[product_columns].itertuples(index=False, name=None)
)

# Insert products into PostgreSQL
insert_products = """
    INSERT INTO products (
        product_id, product_name, category, brand,
        unit_price, stock_quantity, is_active
    )
    VALUES %s
    ON CONFLICT (product_id) DO NOTHING
"""

execute_values(
    cursor,
    insert_products,
    product_rows,
    page_size=1000
)

connection.commit()

# Verify the database count
cursor.execute("SELECT COUNT(*) FROM products")
print("Products loaded into PostgreSQL:", cursor.fetchone()[0])

# Read cleaned order data
orders = pd.read_parquet("/data/cleaned/orders")

print("Cleaned order records ready to load:", len(orders))

# Prepare order records
order_columns = [
    "order_id", "customer_id", "product_id",
    "order_date", "order_status", "payment_method",
    "total_amount", "discount_amount", "shipping_cost"
]

order_rows = list(
    orders[order_columns].itertuples(index=False, name=None)
)

# Insert orders into PostgreSQL
insert_orders = """
    INSERT INTO orders (
        order_id, customer_id, product_id,
        order_date, order_status, payment_method,
        total_amount, discount_amount, shipping_cost
    )
    VALUES %s
    ON CONFLICT (order_id) DO NOTHING
"""

execute_values(
    cursor,
    insert_orders,
    order_rows,
    page_size=1000
)

connection.commit()

# Verify the database count
cursor.execute("SELECT COUNT(*) FROM orders")
print("Orders loaded into PostgreSQL:", cursor.fetchone()[0])

# Function to load analytics tables from Parquet
def load_analytics(table_name, columns):
    df = pd.read_parquet("/data/analytics/" + table_name)

    # Convert pandas missing values to PostgreSQL NULL
    df = df[columns].astype(object).where(pd.notnull(df[columns]), None)

    rows = list(
    	df.itertuples(index=False, name=None)
    )


    column_names = ", ".join(columns)

    insert_query = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES %s
    """

    cursor.execute(f"DELETE FROM {table_name}")

    execute_values(
        cursor,
        insert_query,
        rows,
        page_size=1000
    )

    connection.commit()

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    print(table_name, "records loaded:", cursor.fetchone()[0])

# Load customer analytics
load_analytics("customer_analytics", [
    "customer_id", "first_name", "last_name", "email",
    "phone", "city", "registration_date", "customer_type",
    "lifetime_spending", "order_frequency", "average_order_value"
])

# Load product analytics
load_analytics("product_analytics", [
    "product_id", "product_name", "category", "brand",
    "unit_price", "stock_quantity", "is_active",
    "product_revenue", "times_ordered"
])

# Load product category performance
load_analytics("category_performance", [
    "category", "category_revenue", "category_orders"
])

# Load average product prices
load_analytics("average_product_price", [
    "category", "average_unit_price"
])

# Load inventory status
load_analytics("inventory_status", [
    "is_active", "product_count", "total_stock"
])

# Load daily sales analytics
load_analytics("daily_sales", [
    "order_date", "total_orders", "total_revenue"
])

# Load monthly sales analytics
load_analytics("monthly_sales", [
    "order_month", "total_orders", "total_revenue"
])

# Load order status summary
load_analytics("order_status_summary", [
    "order_status", "total_orders", "total_revenue"
])

# Load payment method summary
load_analytics("payment_summary", [
    "payment_method", "total_orders", "total_revenue"
])

# Load average discount summary
load_analytics("discount_summary", [
    "average_discount_rate"
])

# Load revenue by customer type
load_analytics("revenue_by_customer_type", [
    "customer_type", "total_orders", "total_revenue"
])

# Load categories by customer type
load_analytics("categories_by_customer_type", [
    "customer_type", "category", "total_orders", "total_revenue"
])

# Load city-level revenue
load_analytics("city_revenue", [
    "city", "total_orders", "total_revenue"
])

# Record the successful database load in the ETL audit table
from datetime import datetime

cursor.execute("""
    INSERT INTO etl_audit (
        dag_name, start_time, end_time,
        status, records_processed
    )
    VALUES (%s, %s, %s, %s, %s)
""", (
    "etl_spark_pipeline",
    datetime.now(),
    datetime.now(),
    "SUCCESS",
    len(customers) + len(products) + len(orders)
))

connection.commit()

print("ETL audit record saved successfully")

# Close the connection
cursor.close()
connection.close()
