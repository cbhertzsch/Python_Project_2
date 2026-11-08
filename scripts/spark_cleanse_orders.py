
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, trim, when, to_date

# Start Spark
spark = SparkSession.builder \
    .appName("Project2_Data_Cleansing") \
    .getOrCreate()
# Read the cleaned customers and products for order validation
customers_clean = spark.read.parquet("/data/cleaned/customers")
products_clean = spark.read.parquet("/data/cleaned/products")

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

# Stop Spark
spark.stop()
