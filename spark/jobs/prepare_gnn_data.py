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


def run_preparation():
    spark = SparkSession.builder.appName("Prepare GNN Training Data").getOrCreate()
    logger.info("Spark session started for GNN data preparation.")

    # Load paths from environment
    cleaned_path = os.getenv("DATA_CLEANED_PATH", "./data/cleaned")
    training_path = os.getenv("DATA_TRAINING_PATH", "./data/training")

    # Load all cleaned tables
    logger.info("Loading cleaned tables...")
    activities = spark.read.parquet(f"{cleaned_path}/activities_clean.parquet")
    assays = spark.read.parquet(f"{cleaned_path}/assays_clean.parquet")
    compound_structures = spark.read.parquet(f"{cleaned_path}/compound_structures_clean.parquet")
    target_dict = spark.read.parquet(f"{cleaned_path}/target_dictionary_clean.parquet")
    molecule_dict = spark.read.parquet(f"{cleaned_path}/molecule_dictionary_clean.parquet")

    logger.info("Joining tables for GNN training data...")

    # Join all tables - GNN needs SMILES for graph construction
    gnn_data = activities \
        .join(assays, "assay_id", "left") \
        .join(compound_structures, "molregno", "inner") \
        .join(target_dict, assays["tid"] == target_dict["tid"], "left") \
        .join(molecule_dict, "molregno", "left")

    # Select features for GNN
    gnn_data = gnn_data.select(
        "activity_id",
        # Critical: SMILES for graph construction
        "canonical_smiles",
        # Target variable
        "pIC50",
        # Contextual features (will be added to node/graph features)
        "assay_type",
        "assay_organism",
        "assay_tissue",
        "assay_cell_type",
        "confidence_score",
        # Target information
        "target_type",
        col("target_dictionary_clean.organism").alias("target_organism"),
        # Molecule metadata
        "max_phase",
        "therapeutic_flag"
    )

    # Remove rows without SMILES or target
    logger.info("Removing rows with missing SMILES or pIC50...")
    gnn_data = gnn_data.filter(
        col("canonical_smiles").isNotNull() &
        (col("canonical_smiles") != "") &
        col("pIC50").isNotNull()
    )

    # Fill nulls in contextual features
    gnn_data = gnn_data.fillna({
        "assay_organism": "Unknown",
        "assay_tissue": "Unknown",
        "assay_cell_type": "Unknown",
        "target_organism": "Unknown",
        "max_phase": 0,
        "therapeutic_flag": 0,
        "confidence_score": 0
    })

    logger.info(f"GNN training data prepared: {gnn_data.count()} rows")

    # Repartition for efficient storage
    repartition_size = int(os.getenv("SPARK_REPARTITION_SIZE", "16"))
    gnn_data = gnn_data.repartition(repartition_size)

    # Save as Parquet
    gnn_data.write.parquet(
        f"{training_path}/gnn_training_data.parquet",
        mode="overwrite"
    )

    logger.info("GNN training data saved successfully!")


if __name__ == "__main__":
    run_preparation()
