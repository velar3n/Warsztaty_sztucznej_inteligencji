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
    spark = SparkSession.builder.appName("Clean Target Dictionary").getOrCreate()
    logger.info("Spark session started for target dictionary cleaning.")

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    logger.info("Reading target_dictionary table...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table="target_dictionary",
        properties=properties
    )

    logger.info(f"Target dictionary loaded: {df.count()} rows")

    # Select relevant columns
    df = df.select(
        "tid",
        "target_type",
        "pref_name",
        "organism",
        "tax_id"
    )

    # Clean data - keep only relevant target types
    logger.info("Cleaning target dictionary data...")
    df_clean = df.filter(
        col("target_type").isin(["SINGLE PROTEIN", "PROTEIN COMPLEX", "PROTEIN FAMILY"]) &
        col("pref_name").isNotNull()
    )

    # Remove duplicates based on tid
    df_clean = df_clean.dropDuplicates(["tid"])

    logger.info(f"Target dictionary cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df_clean.write.parquet(
        f"{output_path}/target_dictionary_clean.parquet",
        mode="overwrite"
    )

    logger.info("Target dictionary cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
