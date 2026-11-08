
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Project2_Cleansing_Log").getOrCreate()

raw_folder = Path("/data/raw")
cleaned_folder = Path("/data/cleaned")
log_folder = Path("/data/logs")
log_folder.mkdir(parents=True, exist_ok=True)

with open(log_folder / "cleansing_summary.txt", "w") as file:
    file.write("PROJECT 2 DATA CLEANSING SUMMARY\n")

    for name in ["customers", "products", "orders"]:
        raw = spark.read.csv(
            str(raw_folder / f"{name}.csv"),
            header=True,
            inferSchema=True
        )

        cleaned = spark.read.parquet(
            str(cleaned_folder / name)
        )

        raw_count = raw.count()
        clean_count = cleaned.count()

        file.write(f"\n{name.title()}\n")
        file.write(f"Raw records: {raw_count}\n")
        file.write(f"Cleaned records: {clean_count}\n")
        file.write(f"Records removed: {raw_count - clean_count}\n")

# Count why records were removed
customers = spark.read.csv(
    "/data/raw/customers.csv", header=True, inferSchema=True
)
products = spark.read.csv(
    "/data/raw/products.csv", header=True, inferSchema=True
)
orders = spark.read.csv(
    "/data/raw/orders.csv", header=True, inferSchema=True
)

# Customer issues
customer_duplicates = customers.count() - customers.dropDuplicates(
    ["customer_id"]
).count()

customers_check = customers.dropDuplicates(["customer_id"])
missing_emails = customers_check.filter(
    customers_check["email"].isNull()
).count()

# Product issues
product_duplicates = products.count() - products.dropDuplicates(
    ["product_id"]
).count()

products_check = products.dropDuplicates(["product_id"])
missing_names = products_check.filter(
    products_check["product_name"].isNull()
).count()

products_check = products_check.dropna(subset=["product_name"])
negative_prices = products_check.filter(
    products_check["unit_price"] < 0
).count()

# Order issues
order_duplicates = orders.count() - orders.dropDuplicates(
    ["order_id"]
).count()

orders_check = orders.dropDuplicates(["order_id"])
missing_amounts = orders_check.filter(
    orders_check["total_amount"].isNull()
).count()

orders_check = orders_check.dropna(subset=["total_amount"])

invalid_amounts = orders_check.filter(
    (orders_check["total_amount"] < 0) |
    (orders_check["discount_amount"] > orders_check["total_amount"])
).count()

# Apply the same validity rules as the original script
orders_check = orders_check.filter(
    (orders_check["total_amount"] >= 0) &
    (orders_check["discount_amount"] >= 0) &
    (orders_check["discount_amount"] <= orders_check["total_amount"]) &
    (orders_check["shipping_cost"] >= 0)
)

valid_customers = spark.read.parquet("/data/cleaned/customers")
valid_products = spark.read.parquet("/data/cleaned/products")

orphan_customers = orders_check.join(
    valid_customers.select("customer_id"),
    on="customer_id",
    how="left_anti"
).count()

orders_check = orders_check.join(
    valid_customers.select("customer_id"),
    on="customer_id",
    how="inner"
)

orphan_products = orders_check.join(
    valid_products.select("product_id"),
    on="product_id",
    how="left_anti"
).count()

# Add detailed reasons to the existing log
with open(log_folder / "cleansing_summary.txt", "a") as file:
    file.write("\nRECORD REMOVAL REASONS\n")

    file.write("\nCustomers:\n")
    file.write(f"Duplicate customer IDs: {customer_duplicates}\n")
    file.write(f"Missing email addresses: {missing_emails}\n")

    file.write("\nProducts:\n")
    file.write(f"Duplicate product IDs: {product_duplicates}\n")
    file.write(f"Missing product names: {missing_names}\n")
    file.write(f"Negative product prices: {negative_prices}\n")

    file.write("\nOrders:\n")
    file.write(f"Duplicate order IDs: {order_duplicates}\n")
    file.write(f"Missing order amounts: {missing_amounts}\n")
    file.write(f"Invalid order amounts: {invalid_amounts}\n")
    file.write(f"Missing customer references: {orphan_customers}\n")
    file.write(f"Missing product references: {orphan_products}\n")

print("Data-quality summary saved to /data/logs/cleansing_summary.txt")

spark.stop()
