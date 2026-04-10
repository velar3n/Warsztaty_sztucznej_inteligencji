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
    spark = SparkSession.builder.appName("Clean Molecule Dictionary").getOrCreate()
    logger.info("Spark session started for molecule dictionary cleaning.")

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    logger.info("Reading molecule_dictionary table...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table="molecule_dictionary",
        properties=properties
    )

    logger.info(f"Molecule dictionary loaded: {df.count()} rows")

    # Select relevant columns
    df = df.select(
        "molregno",
        "chembl_id",
        "pref_name",
        "max_phase",           # Development phase (0-4)
        "molecule_type",
        "therapeutic_flag",
        "first_approval"
    )

    # Clean data - filter for small molecules
    logger.info("Cleaning molecule dictionary data...")
    df_clean = df.filter(
        col("molecule_type").isin(["Small molecule", "Unknown"]) &
        col("chembl_id").isNotNull()
    )

    # Remove duplicates based on molregno
    df_clean = df_clean.dropDuplicates(["molregno"])

    logger.info(f"Molecule dictionary cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df_clean.write.parquet(
        f"{output_path}/molecule_dictionary_clean.parquet",
        mode="overwrite"
    )

    logger.info("Molecule dictionary cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
