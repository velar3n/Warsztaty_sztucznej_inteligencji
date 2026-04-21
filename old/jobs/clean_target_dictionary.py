import logging
import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

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
        col("tid").cast("int"),
        col("chembl_id").cast("string"),
        col("pref_name").cast("string"),
        col("organism").cast("string"),
    )

    # Clean data - keep only relevant target types
    logger.info("Cleaning target dictionary data...")
    df_clean = df.filter(
        col("pref_name").isNotNull()
    )

    # Normalise organism to lowercase
    df_clean = df_clean.withColumn("organism", lower(col("organism")))

    # Save as Parquet with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    output_file = f"{output_path}/target_dictionary_clean_{timestamp}.parquet"
 
    df_clean.write.parquet(output_file, mode="errorifexists")
    logger.info(f"Target dictionary cleaning completed and saved to: {output_file}")


if __name__ == "__main__":
    run_cleaning()
