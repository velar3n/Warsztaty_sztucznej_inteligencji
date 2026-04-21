import logging
import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log10, when, lit

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
        col("activity_id").cast("int"),
        col("assay_id").cast("int"),
        col("molregno").cast("int"),
        col("standard_value").cast("float"),
        col("standard_units").cast("string"),
        col("standard_type").cast("string"),
        col("standard_relation").cast("string"),
        col("pchembl_value").cast("float"),
        col("data_validity_comment").cast("string"),
        col("potential_duplicate").cast("string")
    )

    # Clean data
    logger.info("Cleaning activities data...")

    # Remove nulls and filter for IC50
    df_clean = df.filter(
        col("standard_value").isNotNull() &
        (col("potential_duplicate").isNull() | (col("potential_duplicate") == 0))
    )

    # Convert data_validity_comment to binary numeric flag: 1 if not null, 0 if null
    df_clean = df_clean.withColumn(
        "data_validity_comment",
        when(col("data_validity_comment").isNotNull(), lit(1)).otherwise(lit(0)).cast("int")
    )

    # Impute missing nM units for values
    mask_missing = col("standard_units").isNull() & col("standard_value").isNotNull()
    mask_range = (col("standard_value") >= 0.01) & (col("standard_value") <= 1e6)
    df_clean = df_clean.withColumn(
        "standard_units",
        when(mask_missing & mask_range, lit("nM")).otherwise(col("standard_units"))
    )

    # Compute pIC50 for nM like:
    df_clean = df_clean.withColumn(
        "pIC50",
        when(
            (col("standard_units") == "nM") & col("standard_value").isNotNull(),
            -log10(col("standard_value") * 1e-9)
        ).otherwise(col("pchembl_value"))
    )

    logger.info(f"Activities cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet with timestamp 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    output_file = f"{output_path}/activities_clean_{timestamp}.parquet"
 
    df_clean.write.parquet(output_file, mode="errorifexists")
    logger.info(f"Activities cleaning completed and saved to: {output_file}")


if __name__ == "__main__":
    run_cleaning()
