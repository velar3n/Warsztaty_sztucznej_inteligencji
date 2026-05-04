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
    spark = SparkSession.builder.appName("Clean Compound Structures").getOrCreate()
    logger.info("Spark session started for compound structures cleaning.")

    query = """
    SELECT
        CAST(molregno AS INTEGER) AS molregno,
        CAST(canonical_smiles AS TEXT) AS canonical_smiles
    FROM compound_structures
    """

    logger.info(f"Reading compound_structures table...")
    df = read_table_psql(
        spark=spark,
        query=query,
        partition_column="molregno",
        lower_bound=1,
        upper_bound=2854815,
        num_partitions=8
    )

    logger.info(f"Compound structures loaded: {df.count()} rows")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df.write.parquet(
        f"{output_path}/compound_structures_clean.parquet",
        mode="overwrite"
    )

    logger.info("Compound structures cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
