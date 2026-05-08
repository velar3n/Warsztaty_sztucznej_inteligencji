"""
Simple PySpark job for testing the Spark cluster setup.
"""
from pyspark.sql import SparkSession
from datetime import datetime

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("TestSparkJob") \
        .getOrCreate()

    print(f"=== Spark Test Job Started at {datetime.now()} ===")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.sparkContext.master}")

    # Create a simple DataFrame
    data = [
        ("Alice", 25, "Engineer"),
        ("Bob", 30, "Data Scientist"),
        ("Charlie", 35, "Manager"),
        ("Diana", 28, "Analyst"),
        ("Eve", 32, "Developer")
    ]

    df = spark.createDataFrame(data, ["Name", "Age", "Role"])

    print("\n=== Sample Data ===")
    df.show()

    print("\n=== Age Statistics ===")
    df.describe("Age").show()

    print("\n=== Group by Role ===")
    df.groupBy("Role").count().show()

    spark.stop()
    print(f"=== Job Completed at {datetime.now()} ===")

if __name__ == "__main__":
    main()
