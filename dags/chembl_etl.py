from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    'chembl_etl',
    start_date=datetime(2025, 12, 10),
    schedule_interval=None,
    catchup=False
) as dag:
    
    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_etl",
        application="/opt/airflow/spark_jobs/chembl_etl_spark.py",
        conn_id="spark_default",
        executor_memory="2g",
        total_executor_cores=4,
        name="chembl_etl",
        packages="org.postgresql:postgresql:42.7.3",
    )