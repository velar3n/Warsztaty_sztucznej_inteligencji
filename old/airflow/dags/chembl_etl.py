from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Load configuration from environment variables
start_date_str = os.getenv("DAG_START_DATE", "2025-12-10")
schedule_interval = os.getenv("DAG_SCHEDULE_INTERVAL", "None")
schedule_interval = None if schedule_interval == "None" else schedule_interval
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

with DAG(
    'chembl_etl',
    start_date=start_date,
    schedule_interval=schedule_interval,
    catchup=False
) as dag:

    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_etl",
        application="/opt/airflow/spark_jobs/chembl_etl_spark.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="chembl_etl",
        packages="org.postgresql:postgresql:42.7.3",
    )