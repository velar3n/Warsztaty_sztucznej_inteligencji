import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_preparation():
    spark = SparkSession.builder.appName("Prepare MLP Training Data").getOrCreate()
    logger.info("Spark session started for MLP data preparation.")

    # Load paths from environment
    cleaned_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    training_path = os.getenv("DATA_TRAINING_PATH", "./data/training")

    # Load all cleaned tables
    logger.info("Loading cleaned tables...")
    activities = spark.read.parquet(f"{cleaned_path}/activities_clean.parquet")
    assays = spark.read.parquet(f"{cleaned_path}/assays_clean.parquet")
    compound_props = spark.read.parquet(f"{cleaned_path}/compound_properties_clean.parquet")
    target_dict = spark.read.parquet(f"{cleaned_path}/target_dictionary_clean.parquet")
    molecule_dict = spark.read.parquet(f"{cleaned_path}/molecule_dictionary_clean.parquet")

    logger.info("Joining tables for MLP training data...")

    # Join all tables
    mlp_data = activities \
        .join(assays, "assay_id", "left") \
        .join(compound_props, "molregno", "left") \
        .join(target_dict, assays["tid"] == target_dict["tid"], "left") \
        .join(molecule_dict, "molregno", "left")

    # Select only numerical/categorical features for MLP
    mlp_data = mlp_data.select(
        "activity_id",
        # Target variable
        "pIC50",
        # Molecular properties (numerical)
        "mw_freebase",
        "alogp",
        "hba",
        "hbd",
        "psa",
        "rtb",
        "aromatic_rings",
        "heavy_atoms",
        "num_alerts",
        "num_ro5_violations",
        # Assay features (categorical - will need encoding later)
        "assay_type",
        "confidence_score",
        # Target features (categorical)
        "target_type",
        # Molecule features
        "max_phase",
        "therapeutic_flag"
    )

    # Remove rows with any null values in critical features
    logger.info("Removing rows with null values in critical features...")
    mlp_data = mlp_data.dropna(subset=[
        "pIC50", "mw_freebase", "alogp", "hba", "hbd",
        "psa", "assay_type", "target_type"
    ])

    # Fill remaining nulls with defaults
    mlp_data = mlp_data.fillna({
        "rtb": 0,
        "aromatic_rings": 0,
        "heavy_atoms": 0,
        "num_alerts": 0,
        "num_ro5_violations": 0,
        "max_phase": 0,
        "therapeutic_flag": 0,
        "confidence_score": 0
    })

    logger.info(f"MLP training data prepared: {mlp_data.count()} rows")

    # Repartition for efficient storage
    repartition_size = int(os.getenv("SPARK_REPARTITION_SIZE", "16"))
    mlp_data = mlp_data.repartition(repartition_size)

    # Save as Parquet
    mlp_data.write.parquet(
        f"{training_path}/mlp_training_data.parquet",
        mode="overwrite"
    )

    logger.info("MLP training data saved successfully!")


if __name__ == "__main__":
    run_preparation()
