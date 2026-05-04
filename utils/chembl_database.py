import os
from pyspark.sql import SparkSession

def read_table_psql(
    spark: SparkSession,
    query: str,
    partition_column: str,
    lower_bound: int,
    upper_bound: int,
    num_partitions: int
):

    # Load database configuration from environment
    chembl_host = os.getenv("CHEMBL_DB_HOST", "localhost")
    chembl_port = os.getenv("CHEMBL_DB_PORT", "5432")
    chembl_db = os.getenv("CHEMBL_DB_NAME", "chembl")
    chembl_user = os.getenv("CHEMBL_DB_USER", "postgres")
    chembl_password = os.getenv("CHEMBL_DB_PASSWORD", "postgres")

    jdbc_url = f"jdbc:postgresql://{chembl_host}:{chembl_port}/{chembl_db}"
    properties = {
        "user": chembl_user,
        "password": chembl_password,
        "driver": "org.postgresql.Driver"
    }

    # Wrap query as subquery for JDBC
    table_query = f"({query}) AS subquery"

    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_query,
        column=partition_column,
        lowerBound=lower_bound,
        upperBound=upper_bound,
        numPartitions=num_partitions,
        properties=properties
    )

    return df


