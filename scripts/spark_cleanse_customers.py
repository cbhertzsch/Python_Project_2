

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

