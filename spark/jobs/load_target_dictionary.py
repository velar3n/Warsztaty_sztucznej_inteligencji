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
    spark = SparkSession.builder.appName("Clean Target Dictionaries").getOrCreate()
    logger.info("Spark session started for target dictionaries cleaning.")

    query = """
    SELECT
        CAST(tid AS INTEGER) AS tid,
        CAST(chembl_id AS TEXT) AS chembl_id,
        LOWER(CAST(organism AS TEXT)) AS organism,
        LOWER(CAST(pref_name AS TEXT)) AS pref_name
    FROM target_dictionary
    """

    logger.info(f"Reading target_dictionary table...")
    df = read_table_psql(
        spark=spark,
        query=query,
        partition_column="tid",
        lower_bound=1,
        upper_bound=17803,
        num_partitions=1
    )

    logger.info(f"Target dictionaries loaded: {df.count()} rows")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df.write.parquet(
        f"{output_path}/target_dictionary_clean.parquet",
        mode="overwrite"
    )

    logger.info("Target dictionaries cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()