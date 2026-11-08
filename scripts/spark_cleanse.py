

import sys

# Choose which dataset Airflow should clean
dataset = sys.argv[1] if len(sys.argv) > 1 else "all"
from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("Project2_Data_Cleansing") \
    .getOrCreate()

# Read the raw customer data
customers = spark.read.csv(
    "/data/raw/customers.csv",
    header=True,
    inferSchema=True
)

# Check the data before cleaning
print("Customer records before cleaning:", customers.count())
customers.show(5, truncate=False)

# Remove duplicate customer IDs
customers_clean = customers.dropDuplicates(["customer_id"])

# Check the results after removing duplicates
print("Customer records after removing duplicates:", customers_clean.count())
print("Duplicate records removed:", customers.count() - customers_clean.count())

# Check for missing customer email addresses
missing_emails = customers_clean.filter(
    customers_clean["email"].isNull()
)

print("Customers with missing emails:", missing_emails.count())

# Remove customers with missing email addresses
customers_clean = customers_clean.dropna(subset=["email"])

print("Customer records after removing missing emails:",
      customers_clean.count())

# Clean extra spaces and inconsistent capitalization
from pyspark.sql.functions import trim, initcap

customers_clean = customers_clean.withColumn(
    "first_name", initcap(trim(customers_clean["first_name"]))
)

customers_clean = customers_clean.withColumn(
    "last_name", initcap(trim(customers_clean["last_name"]))
)

customers_clean = customers_clean.withColumn(
    "city", initcap(trim(customers_clean["city"]))
)

# Display some cleaned city names
customers_clean.select("customer_id", "first_name", "city").show(10)

# Check the customers with intentionally messy cities
print("Cleaned city capitalization:")
customers_clean.filter(
    customers_clean["customer_id"].between(31, 60)
).select("customer_id", "city").orderBy("customer_id").show(30)

# Check the customers with extra spaces in their names
print("Cleaned customer names:")
customers_clean.filter(
    customers_clean["customer_id"].between(61, 70)
).select("customer_id", "first_name").orderBy("customer_id").show(10)

# Standardize registration dates
from pyspark.sql.functions import coalesce, to_date

customers_clean = customers_clean.withColumn(
    "registration_date",
    coalesce(
        to_date(customers_clean["registration_date"], "yyyy-MM-dd"),
        to_date(customers_clean["registration_date"], "MM/dd/yyyy"),
        to_date(customers_clean["registration_date"], "MMM dd, yyyy")
    )
)

# Check standardized registration dates
print("Standardized registration dates:")
customers_clean.filter(
    customers_clean["customer_id"].between(81, 100)
).select("customer_id", "registration_date").orderBy("customer_id").show(20)

# Save cleaned customer data as Parquet
customers_clean.write.mode("overwrite").parquet(
    "/data/cleaned/customers"
)

print("Cleaned customer data saved as Parquet")

# Stop after customer cleansing if only customers were requested
if dataset == "customers":
    print("Customer cleansing completed")
    spark.stop()
    sys.exit(0)

# Read the raw product data
products = spark.read.csv(
    "/data/raw/products.csv",
    header=True,
    inferSchema=True
)

# Check products before cleaning
print("Product records before cleaning:", products.count())
products.show(5, truncate=False)

# Remove duplicate product IDs
products_clean = products.dropDuplicates(["product_id"])

# Check the results after removing duplicates
print("Product records after removing duplicates:",
      products_clean.count())

print("Duplicate product records removed:",
      products.count() - products_clean.count())

# Check for missing product names
missing_products = products_clean.filter(
    products_clean["product_name"].isNull()
)

print("Products with missing names:", missing_products.count())

# Remove products with missing names
products_clean = products_clean.dropna(subset=["product_name"])

print("Product records after removing missing names:",
      products_clean.count())

# Check for invalid negative product prices
invalid_prices = products_clean.filter(
    products_clean["unit_price"] < 0
)

print("Products with negative prices:", invalid_prices.count())

# Keep only products with valid prices
products_clean = products_clean.filter(
    products_clean["unit_price"] >= 0
)

print("Product records after removing invalid prices:",
      products_clean.count())

# Clean product names, categories, and brands
products_clean = products_clean.withColumn(
    "product_name", initcap(trim(products_clean["product_name"]))
)

products_clean = products_clean.withColumn(
    "category", initcap(trim(products_clean["category"]))
)

products_clean = products_clean.withColumn(
    "brand", initcap(trim(products_clean["brand"]))
)

# Display cleaned product information
print("Cleaned product information:")
products_clean.select(
    "product_id", "product_name", "category", "brand"
).show(10)

# Save cleaned product data as Parquet
products_clean.write.mode("overwrite").parquet(
    "/data/cleaned/products"
)

print("Cleaned product data saved as Parquet")

# Stop after product cleansing if only products were requested
if dataset == "products":
    print("Product cleansing completed")
    spark.stop()
    sys.exit(0)

# Read the raw order data
orders = spark.read.csv(
    "/data/raw/orders.csv",
    header=True,
    inferSchema=True
)

# Check orders before cleaning
print("Order records before cleaning:", orders.count())
orders.show(5, truncate=False)

# Remove duplicate order IDs
orders_clean = orders.dropDuplicates(["order_id"])

# Check the results after removing duplicates
print("Order records after removing duplicates:",
      orders_clean.count())

print("Duplicate order records removed:",
      orders.count() - orders_clean.count())

# Check for missing order amounts
missing_orders = orders_clean.filter(
    orders_clean["total_amount"].isNull()
)

print("Orders with missing amounts:", missing_orders.count())

# Remove orders with missing amounts
orders_clean = orders_clean.dropna(subset=["total_amount"])

print("Order records after removing missing amounts:",
      orders_clean.count())

# Check for invalid order amounts
invalid_orders = orders_clean.filter(
    (orders_clean["total_amount"] < 0) |
    (orders_clean["discount_amount"] > orders_clean["total_amount"])
)

print("Orders with invalid amounts:", invalid_orders.count())

# Keep orders with valid amounts
orders_clean = orders_clean.filter(
    (orders_clean["total_amount"] >= 0) &
    (orders_clean["discount_amount"] >= 0) &
    (orders_clean["discount_amount"] <= orders_clean["total_amount"]) &
    (orders_clean["shipping_cost"] >= 0)
)

print("Order records after removing invalid amounts:",
      orders_clean.count())

# Check for orders with missing customer references
orphan_orders = orders_clean.join(
    customers_clean.select("customer_id"),
    on="customer_id",
    how="left_anti"
)

print("Orders referencing missing customers:",
      orphan_orders.count())

# Keep only orders with valid customer IDs
orders_clean = orders_clean.join(
    customers_clean.select("customer_id"),
    on="customer_id",
    how="inner"
)

print("Orders after customer validation:",
      orders_clean.count())

# Check for orders with missing product references
orphan_products = orders_clean.join(
    products_clean.select("product_id"),
    on="product_id",
    how="left_anti"
)

print("Orders referencing missing products:",
      orphan_products.count())

# Keep only orders with valid product IDs
orders_clean = orders_clean.join(
    products_clean.select("product_id"),
    on="product_id",
    how="inner"
)

print("Orders after product validation:",
      orders_clean.count())

# Clean order statuses and payment methods
orders_clean = orders_clean.withColumn(
    "order_status", initcap(trim(orders_clean["order_status"]))
)

orders_clean = orders_clean.withColumn(
    "payment_method", initcap(trim(orders_clean["payment_method"]))
)

# Keep the correct PayPal capitalization
from pyspark.sql.functions import when

orders_clean = orders_clean.withColumn(
    "payment_method",
    when(
        orders_clean["payment_method"] == "Paypal",
        "PayPal"
    ).otherwise(orders_clean["payment_method"])
)

# Display cleaned order information
print("Cleaned order information:")
orders_clean.select(
    "order_id", "order_status", "payment_method"
).show(10)

# Standardize order dates
orders_clean = orders_clean.withColumn(
    "order_date",
    to_date(orders_clean["order_date"])
)

# Check the standardized order dates
print("Standardized order dates:")
orders_clean.select(
    "order_id", "order_date"
).orderBy("order_id").show(10)

# Save cleaned order data as Parquet
orders_clean.write.mode("overwrite").parquet(
    "/data/cleaned/orders"
)

print("Cleaned order data saved as Parquet")

# Display final data cleansing summary
print("===== DATA CLEANSING SUMMARY =====")

print("Customers:")
print("Raw records:", customers.count())
print("Cleaned records:", customers_clean.count())
print("Records removed:", customers.count() - customers_clean.count())

print("Products:")
print("Raw records:", products.count())
print("Cleaned records:", products_clean.count())
print("Records removed:", products.count() - products_clean.count())

print("Orders:")
print("Raw records:", orders.count())
print("Cleaned records:", orders_clean.count())
print("Records removed:", orders.count() - orders_clean.count())

# Save a data-quality summary to a text file
from pathlib import Path

log_folder = Path("/data/logs")
log_folder.mkdir(parents=True, exist_ok=True)

with open(log_folder / "cleansing_summary.txt", "w") as file:
    file.write("PROJECT 2 DATA CLEANSING SUMMARY\n")

    for name, raw, cleaned in [
        ("Customers", customers, customers_clean),
        ("Products", products, products_clean),
        ("Orders", orders, orders_clean)
    ]:
        raw_count = raw.count()
        clean_count = cleaned.count()

        file.write(f"\n{name}\n")
        file.write(f"Raw records: {raw_count}\n")
        file.write(f"Cleaned records: {clean_count}\n")
        file.write(f"Records removed: {raw_count - clean_count}\n")


# Add the reasons records were removed
with open(log_folder / "cleansing_summary.txt", "a") as file:
    file.write("\nRECORD REMOVAL REASONS\n")

    file.write("\nCustomers:\n")
    file.write(f"Duplicate customer IDs: {customers.count() - customers.dropDuplicates(['customer_id']).count()}\n")
    file.write(f"Missing email addresses: {missing_emails.count()}\n")

    file.write("\nProducts:\n")
    file.write(f"Duplicate product IDs: {products.count() - products.dropDuplicates(['product_id']).count()}\n")
    file.write(f"Missing product names: {missing_products.count()}\n")
    file.write(f"Negative product prices: {invalid_prices.count()}\n")

    file.write("\nOrders:\n")
    file.write(f"Duplicate order IDs: {orders.count() - orders.dropDuplicates(['order_id']).count()}\n")
    file.write(f"Missing order amounts: {missing_orders.count()}\n")
    file.write(f"Invalid order amounts: {invalid_orders.count()}\n")
    file.write(f"Missing customer references: {orphan_orders.count()}\n")
    file.write(f"Missing product references: {orphan_products.count()}\n")

print("Data-quality summary saved to /data/logs/cleansing_summary.txt")

# Stop Spark
spark.stop()
