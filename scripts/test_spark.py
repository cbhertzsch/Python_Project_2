
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Project2_Connection_Test") \
    .getOrCreate()

data = [(1, "Customer A"), (2, "Customer B")]

df = spark.createDataFrame(data, ["customer_id", "name"])

df.show()

print("PROJECT 2 SPARK TEST SUCCESSFUL")

spark.stop()
