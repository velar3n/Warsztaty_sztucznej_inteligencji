import logging
import os
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, log10, lit
from utils.chembl_database import read_table_psql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_cleaning():
    spark = SparkSession.builder.appName("Clean Activities").getOrCreate()
    logger.info("Spark session started for activities cleaning.")

    query = """
    SELECT
        CAST(activity_id AS INTEGER) AS activity_id,
        CAST(assay_id AS INTEGER) AS assay_id,
        CAST(molregno AS INTEGER) AS molregno,
        CAST(standard_value AS REAL) AS standard_value,
        CAST(standard_units AS TEXT) AS standard_units,
        CAST(standard_type AS TEXT) AS standard_type,
        CAST(standard_relation AS TEXT) AS standard_relation,
        CAST(pchembl_value as REAL) as pchembl_value,
        CAST(data_validity_comment AS TEXT) AS data_validity_comment,
        CAST(potential_duplicate AS SMALLINT) AS potential_duplicate
    FROM activities
    """

    logger.info(f"Reading assay table...")
    df = read_table_psql(
        spark=spark,
        query=query,
        partition_column="activity_id",
        lower_bound=1,
        upper_bound=24267312,
        num_partitions=16
    )

    logger.info(f"Activities loaded: {df.count()} rows")

    # Transform data
    logger.info("Transforming activities data...")

    # a) Create boolean for data_validity_comment
    df = df.withColumn(
        "has_validity_comment",
        when(col("data_validity_comment").isNotNull(), lit(True)).otherwise(lit(False))
    )

    # b) Impute missing standard_units (assume nM for sensible ranges)
    df = df.withColumn(
        "standard_units",
        when(
            col("standard_units").isNull() &
            col("standard_value").isNotNull() &
            (col("standard_value") >= 0.01) &
            (col("standard_value") <= 1e6),
            lit("nM")
        ).otherwise(col("standard_units"))
    )

    # Compute pIC50 = -log10(M) for nM values, otherwise use pchembl_value
    df = df.withColumn(
        "pIC50",
        when(
            (col("standard_units") == "nM") &
            col("standard_value").isNotNull(),
            -log10(col("standard_value") * 1e-9)
        ).otherwise(col("pchembl_value"))
    )

    logger.info("Data transformation completed.")

    # Save as Parquet
    output_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    df.write.parquet(
        f"{output_path}/activities_clean.parquet",
        mode="overwrite"
    )

    logger.info("Activites cleaning completed and saved to Parquet!")


if __name__ == "__main__":
    run_cleaning()
