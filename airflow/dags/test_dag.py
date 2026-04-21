from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


with DAG(
    'test_dag',
    start_date=datetime(2025, 1, 1),
    owner='nkielbasa',
    schedule_interval=None,
    catchup=False,
    description='Test dag to check connection'
) as dag:

    test_spark = SparkSubmitOperator(
        task_id="test_spark_job",
        conn_id="spark_default",
        application="/opt/workspace/jobs/test_job.py",
        name="test_job",
        verbose=True,
        conf={
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.executor.cores": "1"
        }
    )