import logging
import os
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from utils.chembl_database import read_table_psql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_cleaning():
    spark = SparkSession.builder.appName("Clean Assays").getOrCreate()
    logger.info("Spark session started for assays cleaning.")

    query = """
    SELECT
        CAST(assay_id AS INTEGER) AS assay_id,
        CAST(assay_type AS TEXT) AS assay_type,
        CAST(assay_organism AS TEXT) AS assay_organism,
        CAST(relationship_type AS TEXT) AS assay_relationship,
        CAST(confidence_score AS INT) AS confidence_score
    FROM assays
    """

    logger.info(f"Reading assay table...")
    df = read_table_psql(
        spark=spark,
        query=query,
        partition_column="assay_id",
        lower_bound=1,
        upper_bound=1890749,
        num_partitions=8
    )

    logger.info(f"Assays loaded: {df.count()} rows")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df.write.parquet(
        f"{output_path}/assays_clean.parquet",
        mode="overwrite"
    )

    logger.info("Assays cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
