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
    spark = SparkSession.builder.appName("Clean Compound Properties").getOrCreate()
    logger.info("Spark session started for compound properties cleaning.")

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    # Read compound_properties table with partitioning
    num_partitions = int(os.getenv("SPARK_NUMPARTITIONS_COMPOUNDS", "8"))

    logger.info(f"Reading compound_properties table with {num_partitions} partitions...")
    df = spark.read.jdbc(
        url=jdbc_url,
        table="compound_properties",
        column="molregno",
        lowerBound=1,
        upperBound=2858458,
        numPartitions=num_partitions,
        properties=properties
    )

    logger.info(f"Compound properties loaded: {df.count()} rows")

    # Select relevant columns for ML features
    df = df.select(
        col("molregno").cast("int"),
        col("mw_freebase").cast("float"),
        col("alogp").cast("float"),
        col("hba").cast("int"),
        col("hbd").cast("int"),
        col("psa").cast("float"),
        col("rtb").cast("int"),
        col("aromatic_rings").cast("int"),
    )


    logger.info(f"Compound properties cleaned: {df.count()} rows remaining")

    # Save as Parquet with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    output_file = f"{output_path}/compound_properties_clean_{timestamp}.parquet"

    df.write.parquet(output_file, mode="errorifexists")
    logger.info(f"Compound properties cleaning completed and saved to: {output_file}")


if __name__ == "__main__":
    run_cleaning()
