
import os
import pandas as pd
import psycopg2

# Read cleaned Parquet files to get expected counts
expected_counts = {}

for table in ["customers", "products", "orders"]:
    df = pd.read_parquet("/data/cleaned/" + table)
    expected_counts[table] = len(df)

# Connect to PostgreSQL
connection = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
)

cursor = connection.cursor()

# Compare PostgreSQL counts with cleaned Parquet counts
for table, expected in expected_counts.items():
    cursor.execute("SELECT COUNT(*) FROM " + table)
    actual = cursor.fetchone()[0]

    print(table, "Expected:", expected, "Actual:", actual)

    if actual != expected:
        raise ValueError(table + " record count does not match!")

# Check that all orders reference existing customers and products
cursor.execute("""
    SELECT COUNT(*)
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products p ON o.product_id = p.product_id
    WHERE c.customer_id IS NULL OR p.product_id IS NULL
""")

invalid_references = cursor.fetchone()[0]
print("Orders with invalid references:", invalid_references)

if invalid_references != 0:
    raise ValueError("Invalid customer or product references found!")

cursor.close()
connection.close()

print("PostgreSQL data validation passed!")
