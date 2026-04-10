import logging
import os

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

    # Connect to the db - load from environment variables
    chembl_host = os.getenv("CHEMBL_DB_HOST", "host.docker.internal")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "password")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {"user": chembl_user, "password": chembl_password, "driver": "org.postgresql.Driver"}

    # Load tables as Spark DataFrames
    num_partitions_activities = int(os.getenv("SPARK_NUMPARTITIONS_ACTIVITIES", "16"))
    num_partitions_assays = int(os.getenv("SPARK_NUMPARTITIONS_ASSAYS", "8"))
    num_partitions_compounds = int(os.getenv("SPARK_NUMPARTITIONS_COMPOUNDS", "8"))

    tables = [
        {"name": "activities", "column": "activity_id", "lowerBound": 1, "upperBound": 24267312, "numPartitions": num_partitions_activities},
        {"name": "assays", "column": "assay_id", "lowerBound": 1, "upperBound": 1890749, "numPartitions": num_partitions_assays},
        {"name": "compound_properties", "column": "molregno", "lowerBound": 1, "upperBound": 2858458, "numPartitions": num_partitions_compounds},
        {"name": "compound_structures", "column": "molregno", "lowerBound": 1, "upperBound": 2854815, "numPartitions": num_partitions_compounds},
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

    activities_clean = dfs["activities"].filter( # assay type B F --> przyjrzeć się
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

    # Load target database configuration from environment variables
    features_host = os.getenv("CHEMBL_FEATURES_DB_HOST", "host.docker.internal")
    features_port = os.getenv("CHEMBL_FEATURES_DB_PORT", "5432")
    features_db = os.getenv("CHEMBL_FEATURES_DB_NAME", "chembl_features")
    features_user = os.getenv("CHEMBL_FEATURES_DB_USER", "postgres")
    features_password = os.getenv("CHEMBL_FEATURES_DB_PASSWORD", "password")

    target_jdbc_url = f"jdbc:postgresql://{features_host}:{features_port}/{features_db}"
    target_properties = {
        "user": features_user,
        "password": features_password,
        "driver": "org.postgresql.Driver"
    }

    repartition_size = int(os.getenv("SPARK_REPARTITION_SIZE", "16"))
    joined_df = joined_df.repartition(repartition_size) 

    joined_df.write.mode("errorifexists").jdbc(
        url=target_jdbc_url,
        table=target_table,
        properties=target_properties
    )

    logger.info(f"ETL completed. Saved as {target_table}")


if __name__ == "__main__":
    run_etl()
