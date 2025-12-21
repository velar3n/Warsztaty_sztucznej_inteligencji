import logging

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def add_prefix(df, prefix):
    """Add a prefix to all column names"""
    for c in df.columns:
        df = df.withColumnRenamed(c, f"{prefix}_{c}")
    return df


def run_etl():   
    spark = SparkSession.builder.appName("ChEMBL ETL").getOrCreate()
    logger.info("Spark session started.")

    # Connect to the db
    jdbc_url = "jdbc:postgresql://host.docker.internal:5432/chembl"
    properties = {"user": "postgres", "password": "password", "driver": "org.postgresql.Driver"}

    # Load tables as Spark DataFrames
    tables = [
        {"name": "activities", "column": "activity_id", "lowerBound": 1, "upperBound": 24267312, "numPartitions": 16},
        {"name": "assays", "column": "assay_id", "lowerBound": 1, "upperBound": 1890749, "numPartitions": 16},
        {"name": "compound_properties", "column": "molregno", "lowerBound": 1, "upperBound": 2858458, "numPartitions": 16},
        {"name": "compound_structures", "column": "molregno", "lowerBound": 1, "upperBound": 2854815, "numPartitions": 16},
    ]

    dfs = {}
    for table in tables:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table["name"],
            column=table["column"],
            lowerBound=table["lowerBound"],
            upperBound=table["upperBound"],
            numPartitions=table["numPartitions"],
            properties=properties
        )
        dfs[table["name"]] = df
        logger.info(f"Loaded {table['name']} with {len(df.columns)} columns.")
    logger.info("All tables loaded successfully.")

    # Select relevant rows from activities for activity model training (no missing / incomplete standard values)
    logger.info("Cleaning 'activities' table...")

    activities_clean = dfs["activities"].filter(
        col("standard_value").isNotNull() &
        col("standard_units").isNotNull() &
        (col("standard_type") == "IC50")
    )
    logger.info(f"'activities' table cleaned.")

    # Add prefixes
    activities_prefixed = add_prefix(activities_clean, "act")
    assays_prefixed = add_prefix(dfs["assays"], "assay")
    compound_props_prefixed = add_prefix(dfs["compound_properties"], "cp")
    compound_structs_prefixed = add_prefix(dfs["compound_structures"], "cs")

    # Join using prefixed keys
    joined_df = (
        activities_prefixed
        .join(
            assays_prefixed,
            activities_prefixed["act_assay_id"] == assays_prefixed["assay_assay_id"],
            "left"
        )
        .join(
            compound_props_prefixed,
            activities_prefixed["act_molregno"] == compound_props_prefixed["cp_molregno"],
            "left"
        )
        .join(
            compound_structs_prefixed,
            activities_prefixed["act_molregno"] == compound_structs_prefixed["cs_molregno"],
            "left"
        )
    )

    logger.info(f"Tables joined succesfully!")
    joined_df.printSchema()

    # Convert problematic decimal columns to string
    columns_to_drop = [
        "act_standard_value",
        "act_upper_value",
        "act_standard_upper_value",
        "act_value",
        "act_record_id",
        "act_activity_comment",
        "assay_assay_id",
        "assay_doc_id",
        "assay_description",
        "assay_assay_organism",
        "assay_assay_group",
        "cs_molregno",
        "cs_standard_inchi",
        "cs_standard_inchi_key",
        "cs_molfile",
        "cp_molregno",
        "cp_full_molformula",
        "act_bao_endpoint",
        "act_uo_units",
        "act_qudt_units",
        "act_units",
    ]

    joined_df = joined_df.drop(*columns_to_drop)

    # Save new table to database as base for training

    version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    target_table = f"activity_features_v{version}"

    target_jdbc_url = "jdbc:postgresql://host.docker.internal:5432/chembl_features"
    target_properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }

    joined_df = joined_df.repartition(16)  # adjust to cluster cores

    joined_df.write.mode("errorifexists").jdbc(
        url=target_jdbc_url,
        table=target_table,
        properties=target_properties
    )

    logger.info(f"ETL completed. Saved as {target_table}")


if __name__ == "__main__":
    run_etl()
