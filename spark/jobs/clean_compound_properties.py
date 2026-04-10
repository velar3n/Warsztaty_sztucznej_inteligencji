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
        "molregno",
        "mw_freebase",         # Molecular weight
        "alogp",               # LogP (lipophilicity)
        "hba",                 # H-bond acceptors
        "hbd",                 # H-bond donors
        "psa",                 # Polar surface area
        "rtb",                 # Rotatable bonds
        "ro3_pass",            # Rule of 3
        "num_ro5_violations",  # Lipinski violations
        "aromatic_rings",
        "heavy_atoms",
        "num_alerts"           # Structural alerts
    )

    # Clean data - remove compounds with missing critical properties
    logger.info("Cleaning compound properties data...")
    df_clean = df.filter(
        col("mw_freebase").isNotNull() &
        col("alogp").isNotNull() &
        col("hba").isNotNull() &
        col("hbd").isNotNull()
    )

    # Remove duplicates based on molregno
    df_clean = df_clean.dropDuplicates(["molregno"])

    logger.info(f"Compound properties cleaned: {df_clean.count()} rows remaining")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df_clean.write.parquet(
        f"{output_path}/compound_properties_clean.parquet",
        mode="overwrite"
    )

    logger.info("Compound properties cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
