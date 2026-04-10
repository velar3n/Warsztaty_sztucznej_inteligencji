import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_cleaning():
    spark = SparkSession.builder.appName("Clean Assays").getOrCreate()
    logger.info("Spark session started for assays cleaning.")

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    # Read assays table with partitioning
    num_partitions = int(os.getenv("SPARK_NUMPARTITIONS_ASSAYS", "8"))

    logger.info(f"Reading assays table with {num_partitions} partitions...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table="assays",
        column="assay_id",
        lowerBound=1,
        upperBound=1890749,
        numPartitions=num_partitions,
        properties=properties
    )

    logger.info(f"Assays loaded: {df.count()} rows")

    # Select relevant columns
    df = df.select(
        "assay_id",
        "assay_type",
        "assay_organism",
        "assay_tissue",
        "assay_cell_type",
        "assay_subcellular_fraction",
        "tid",  # target_id
        "confidence_score"
    )

    # Clean data - filter for high confidence assays
    logger.info("Cleaning assays data...")
    df_clean = df.filter(
        col("confidence_score").isNotNull() &
        (col("confidence_score") >= 7)  # Only high confidence assays
    )

    # Remove duplicates based on assay_id
    df_clean = df_clean.dropDuplicates(["assay_id"])

    logger.info(f"Assays cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df_clean.write.parquet(
        f"{output_path}/assays_clean.parquet",
        mode="overwrite"
    )

    logger.info("Assays cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
