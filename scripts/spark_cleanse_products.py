

import sys

# Choose which dataset Airflow should clean
dataset = sys.argv[1] if len(sys.argv) > 1 else "all"
from pyspark.sql import SparkSession
from pyspark.sql.functions import initcap, trim

# Start Spark
spark = SparkSession.builder \
    .appName("Project2_Data_Cleansing") \
    .getOrCreate()

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

