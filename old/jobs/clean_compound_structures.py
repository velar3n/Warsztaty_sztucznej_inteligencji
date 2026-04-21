import logging
import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_cleaning():
    spark = SparkSession.builder.appName("Clean Compound Structures").getOrCreate()
    logger.info("Spark session started for compound structures cleaning.")

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    # Read compound_structures table with partitioning
    num_partitions = int(os.getenv("SPARK_NUMPARTITIONS_COMPOUNDS", "8"))

    logger.info(f"Reading compound_structures table with {num_partitions} partitions...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table="compound_structures",
        column="molregno",
        lowerBound=1,
        upperBound=2854815,
        numPartitions=num_partitions,
        properties=properties
    )

    logger.info(f"Compound structures loaded: {df.count()} rows")

    # Select relevant columns
    df = df.select(
        col("molregno").cast("int"),
        col("canonical_smiles").cast("string")
    )

    # Clean data - remove compounds without SMILES
    logger.info("Cleaning compound structures data...")
    df_clean = df.filter(
        col("canonical_smiles").isNotNull() &
        (col("canonical_smiles") != "")
    )

    logger.info(f"Compound structures cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    output_file = f"{output_path}/compound_structures_clean_{timestamp}.parquet"

    df_clean.write.parquet(output_file, mode="errorifexists")
    logger.info(f"Compound structures cleaning completed and saved to: {output_file}")


if __name__ == "__main__":
    run_cleaning()
