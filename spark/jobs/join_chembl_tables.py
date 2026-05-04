import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_joining():
    spark = SparkSession.builder.appName("Join ChEMBL Tables").getOrCreate()
    logger.info("Spark session started for joining ChEMBL tables.")

    data_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")

    # Load all cleaned parquet files
    logger.info("Loading cleaned parquet files...")
    activities = spark.read.parquet(f"{data_path}/activities_clean.parquet")
    assays = spark.read.parquet(f"{data_path}/assays_clean.parquet")
    target_dict = spark.read.parquet(f"{data_path}/target_dictionary_clean.parquet")
    structures = spark.read.parquet(f"{data_path}/compound_structures_clean.parquet")
    properties = spark.read.parquet(f"{data_path}/compound_properties_clean.parquet")

    logger.info(f"Activities loaded: {activities.count()} rows")
    logger.info(f"Assays loaded: {assays.count()} rows")
    logger.info(f"Target dictionary loaded: {target_dict.count()} rows")
    logger.info(f"Compound structures loaded: {structures.count()} rows")
    logger.info(f"Compound properties loaded: {properties.count()} rows")

    # Perform inner joins
    logger.info("Performing joins...")

    # Join activities with assays
    df = activities.join(assays, on="assay_id", how="inner")
    logger.info(f"After joining with assays: {df.count()} rows")

    # Join with target_dictionary (using tid from assays)
    df = df.join(target_dict, on="tid", how="inner")
    logger.info(f"After joining with target_dictionary: {df.count()} rows")

    # Join with compound_structures
    df = df.join(structures, on="molregno", how="inner")
    logger.info(f"After joining with compound_structures: {df.count()} rows")

    # Join with compound_properties
    df = df.join(properties, on="molregno", how="inner")
    logger.info(f"After joining with compound_properties: {df.count()} rows")

    # Apply data quality filters
    logger.info("Applying data quality filters...")

    # Filter 1: Remove potential duplicates
    df = df.filter((col("potential_duplicate").isNull()) | (col("potential_duplicate") == 0))
    logger.info(f"After removing potential duplicates: {df.count()} rows")

    # Filter 2: Remove invalid pIC50 values (NULL or infinite)
    df = df.filter(
        col("pIC50").isNotNull() &
        ~isnan(col("pIC50")) &
        (col("pIC50") != float('inf')) &
        (col("pIC50") != float('-inf'))
    )
    logger.info(f"After filtering invalid pIC50: {df.count()} rows")

    # Filter 3: Ensure canonical_smiles is present
    df = df.filter(col("canonical_smiles").isNotNull())
    logger.info(f"After filtering null SMILES: {df.count()} rows")

    # Select and rename columns for final output
    logger.info("Selecting and renaming columns...")
    df_final = df.select(
        # Identifiers
        col("activity_id"),
        col("molregno"),

        # Activity measurements
        col("standard_value"),
        col("standard_units"),
        col("standard_type"),
        col("standard_relation"),
        col("pchembl_value"),
        col("pIC50"),
        col("has_validity_comment"),

        # Assay information
        col("assay_type"),
        col("assay_organism"),
        col("assay_relationship"),
        col("confidence_score"),

        # Target information (with renames)
        col("chembl_id").alias("target_chembl_id"),
        col("pref_name").alias("target_name"),
        col("organism"),

        # Chemical structure
        col("canonical_smiles"),

        # Compound properties
        col("mw_freebase"),
        col("alogp"),
        col("hba"),
        col("hbd"),
        col("psa"),
        col("rtb"),
        col("aromatic_rings"),
        col("qed_weighted")
    )

    # Write output
    output_path = f"{data_path}/chembl_joined.parquet"
    logger.info(f"Writing joined data to {output_path}...")
    df_final.write.parquet(output_path, mode="overwrite")

    logger.info(f"Join completed! Final dataset: {df_final.count()} rows")
    logger.info("ChEMBL tables joined successfully!")


if __name__ == "__main__":
    run_joining()
