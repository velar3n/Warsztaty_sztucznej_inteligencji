from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test_job").getOrCreate()

data = [("Alice", 1), ("Bob", 2)]
df = spark.createDataFrame(data, ["name", "value"])

df.show()

print("Spark job executed successfully")

spark.stop()