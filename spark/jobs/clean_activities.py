import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_cleaning():
    spark = SparkSession.builder.appName("Clean Activities").getOrCreate()
    logger.info("Spark session started for activities cleaning.")

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    # Read activities table with partitioning
    num_partitions = int(os.getenv("SPARK_NUMPARTITIONS_ACTIVITIES", "16"))

    logger.info(f"Reading activities table with {num_partitions} partitions...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table="activities",
        column="activity_id",
        lowerBound=1,
        upperBound=24267312,
        numPartitions=num_partitions,
        properties=properties
    )

    logger.info(f"Activities loaded: {df.count()} rows")

    # Select relevant columns
    df = df.select(
        "activity_id",
        "assay_id",
        "molregno",
        "standard_value",
        "standard_units",
        "standard_type",
        "standard_relation",
        "data_validity_comment",
        "potential_duplicate"
    )

    # Clean data - remove nulls and filter for IC50
    logger.info("Cleaning activities data...")
    df_clean = df.filter(
        col("standard_value").isNotNull() &
        col("standard_units").isNotNull() &
        (col("standard_type") == "IC50") &
        (col("data_validity_comment").isNull())  # Only valid data
    )

    # Remove duplicates based on activity_id
    df_clean = df_clean.dropDuplicates(["activity_id"])

    # Calculate pIC50 (-log10 of IC50 in nM)
    df_clean = df_clean.withColumn(
        "pIC50",
        -log10(col("standard_value") / 1e9)  # Convert nM to M then -log10
    )

    logger.info(f"Activities cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df_clean.write.parquet(
        f"{output_path}/activities_clean.parquet",
        mode="overwrite"
    )

    logger.info("Activities cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
