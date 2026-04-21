import logging
import os

from datetimeq import datetime
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
        col("assay_id").cast("int"),
        col("assay_type").cast("string"),
        col("assay_organism").cast("string"),
        col("tissue_id").cast("int"),
        col("cell_id").cast("int"),
        col("tid").cast("int"),
        col("confidence_score").cast("int")
    )

    logger.info(f"Assays cleaned: {df.count()} rows remaining")

    # Save as Parquet with timestamp 
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    output_file = f"{output_path}/assays_clean_{timestamp}.parquet"

    df.write.parquet(output_file, mode="errorifexists")
    logger.info(f"Assays cleaning completed and saved to: {output_file}")


if __name__ == "__main__":
    run_cleaning()
