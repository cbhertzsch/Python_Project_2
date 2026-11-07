
from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("Project2_Spark_Analytics") \
    .getOrCreate()

# Read cleaned Parquet datasets
customers = spark.read.parquet("/data/cleaned/customers")
products = spark.read.parquet("/data/cleaned/products")
orders = spark.read.parquet("/data/cleaned/orders")

# Check the cleaned datasets
print("Cleaned customers:", customers.count())
print("Cleaned products:", products.count())
print("Cleaned orders:", orders.count())

# Calculate total spending per customer
from pyspark.sql.functions import sum

customer_spending = orders.groupBy("customer_id").agg(
    sum("total_amount").alias("lifetime_spending")
)

# Display the top 10 customers by spending
print("Top 10 customers by lifetime spending:")
customer_spending.orderBy(
    "lifetime_spending", ascending=False
).show(10)

# Calculate order frequency and average order value
from pyspark.sql.functions import count, avg, round

customer_metrics = orders.groupBy("customer_id").agg(
    count("order_id").alias("order_frequency"),
    round(avg("total_amount"), 2).alias("average_order_value")
)

# Display customer order metrics
print("Customer order frequency and average order value:")
customer_metrics.orderBy(
    "order_frequency", ascending=False
).show(10)

# Calculate customer type distribution
customer_types = customers.groupBy("customer_type").agg(
    count("customer_id").alias("customer_count")
)

# Display customer type distribution
print("Customer type distribution:")
customer_types.orderBy(
    "customer_count", ascending=False
).show()

# Combine customer spending and order metrics
customer_analytics = customers.join(
    customer_spending,
    on="customer_id",
    how="left"
).join(
    customer_metrics,
    on="customer_id",
    how="left"
)

# Display combined customer analytics
print("Combined customer analytics:")
customer_analytics.select(
    "customer_id",
    "customer_type",
    "lifetime_spending",
    "order_frequency",
    "average_order_value"
).show(10)

# Replace missing spending and order counts with zero
customer_analytics = customer_analytics.fillna({
    "lifetime_spending": 0,
    "order_frequency": 0
})

# Round spending amounts to two decimal places
customer_analytics = customer_analytics.withColumn(
    "lifetime_spending",
    round(customer_analytics["lifetime_spending"], 2)
)

# Save customer analytics as Parquet
customer_analytics.write.mode("overwrite").parquet(
    "/data/analytics/customer_analytics"
)

print("Customer analytics saved as Parquet")

# Calculate revenue and popularity by product
product_sales = orders.groupBy("product_id").agg(
    round(sum("total_amount"), 2).alias("product_revenue"),
    count("order_id").alias("times_ordered")
)

# Join sales results with product information
product_analytics = products.join(
    product_sales,
    on="product_id",
    how="left"
)

# Display the top 10 products by revenue
print("Top 10 products by revenue:")
product_analytics.select(
    "product_id",
    "product_name",
    "category",
    "product_revenue",
    "times_ordered"
).orderBy(
    "product_revenue", ascending=False
).show(10)

# Calculate revenue and orders by product category
category_performance = product_analytics.groupBy("category").agg(
    round(sum("product_revenue"), 2).alias("category_revenue"),
    sum("times_ordered").alias("category_orders")
)

# Display category performance
print("Product category performance:")
category_performance.orderBy(
    "category_revenue", ascending=False
).show()

# Calculate average product price by category
average_product_price = products.groupBy("category").agg(
    round(avg("unit_price"), 2).alias("average_unit_price")
)

# Display average product prices
print("Average product price by category:")
average_product_price.orderBy("category").show()

# Calculate active and inactive product inventory
inventory_status = products.groupBy("is_active").agg(
    count("product_id").alias("product_count"),
    sum("stock_quantity").alias("total_stock")
)

# Display inventory status
print("Active and inactive product inventory:")
inventory_status.orderBy("is_active").show()

# Replace missing product revenue and order counts with zero
product_analytics = product_analytics.fillna({
    "product_revenue": 0,
    "times_ordered": 0
})

# Save product analytics as Parquet
product_analytics.write.mode("overwrite").parquet(
    "/data/analytics/product_analytics"
)

print("Product analytics saved as Parquet")

# Save category and inventory summaries
category_performance.write.mode("overwrite").parquet(
    "/data/analytics/category_performance"
)

average_product_price.write.mode("overwrite").parquet(
    "/data/analytics/average_product_price"
)

inventory_status.write.mode("overwrite").parquet(
    "/data/analytics/inventory_status"
)

print("Product summary analytics saved as Parquet")

# Calculate daily sales
daily_sales = orders.groupBy("order_date").agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display daily sales
print("Daily sales:")
daily_sales.orderBy("order_date").show(10)

# Calculate monthly sales
from pyspark.sql.functions import date_format

monthly_sales = orders.withColumn(
    "order_month", date_format(orders["order_date"], "yyyy-MM")
).groupBy("order_month").agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display monthly sales
print("Monthly sales:")
monthly_sales.orderBy("order_month").show(15)

# Calculate order status breakdown
order_status_summary = orders.groupBy("order_status").agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display order status breakdown
print("Order status breakdown:")
order_status_summary.orderBy("total_orders", ascending=False).show()

# Calculate revenue by payment method
payment_summary = orders.groupBy("payment_method").agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display revenue by payment method
print("Revenue by payment method:")
payment_summary.orderBy(
    "total_revenue", ascending=False
).show()

# Calculate average discount rate
from pyspark.sql.functions import when

discount_summary = orders.select(
    when(
        orders["total_amount"] > 0,
        (orders["discount_amount"] / orders["total_amount"]) * 100
    ).alias("discount_rate")
).agg(
    round(avg("discount_rate"), 2).alias("average_discount_rate")
)

# Display average discount rate
print("Average discount rate:")
discount_summary.show()

# Save order analytics as Parquet
daily_sales.write.mode("overwrite").parquet(
    "/data/analytics/daily_sales"
)

monthly_sales.write.mode("overwrite").parquet(
    "/data/analytics/monthly_sales"
)

order_status_summary.write.mode("overwrite").parquet(
    "/data/analytics/order_status_summary"
)

payment_summary.write.mode("overwrite").parquet(
    "/data/analytics/payment_summary"
)

discount_summary.write.mode("overwrite").parquet(
    "/data/analytics/discount_summary"
)

print("Order analytics saved as Parquet")

# Join orders with customer information
customer_orders = orders.join(
    customers.select("customer_id", "customer_type", "city"),
    on="customer_id",
    how="inner"
)

# Calculate revenue by customer type
revenue_by_customer_type = customer_orders.groupBy(
    "customer_type"
).agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display revenue by customer type
print("Revenue by customer type:")
revenue_by_customer_type.orderBy(
    "total_revenue", ascending=False
).show()

# Join customer orders with product categories
customer_product_orders = customer_orders.join(
    products.select("product_id", "category"),
    on="product_id",
    how="inner"
)

# Calculate category revenue by customer type
categories_by_customer_type = customer_product_orders.groupBy(
    "customer_type", "category"
).agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display category performance for each customer type
print("Product categories by customer type:")
categories_by_customer_type.orderBy(
    "customer_type", "total_revenue",
    ascending=[True, False]
).show(20)

# Calculate revenue by customer city
city_revenue = customer_orders.groupBy("city").agg(
    count("order_id").alias("total_orders"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

# Display the top 10 cities by revenue
print("Top 10 cities by revenue:")
city_revenue.orderBy(
    "total_revenue", ascending=False
).show(10)

# Save cross-table analytics as Parquet
revenue_by_customer_type.write.mode("overwrite").parquet(
    "/data/analytics/revenue_by_customer_type"
)

categories_by_customer_type.write.mode("overwrite").parquet(
    "/data/analytics/categories_by_customer_type"
)

city_revenue.write.mode("overwrite").parquet(
    "/data/analytics/city_revenue"
)

print("Cross-table analytics saved as Parquet")

# Stop Spark
spark.stop()
