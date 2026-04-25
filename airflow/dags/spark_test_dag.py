"""
Airflow DAG to submit Spark jobs to the Spark cluster.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_test_dag',
    default_args=default_args,
    description='Test DAG for Spark job submission',
    catchup=False,
    tags=['spark', 'test'],
)

# Task 1: Check Spark cluster status
check_spark = BashOperator(
    task_id='check_spark_cluster',
    bash_command='echo "Checking Spark cluster..." && curl -s http://spark-master:8080 > /dev/null && echo "Spark Master is healthy!"',
    dag=dag,
)

# Task 2: Submit Spark job
spark_job = SparkSubmitOperator(
    task_id='run_spark_test_job',
    application='/opt/spark/jobs/test_spark_job.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.cores.max': '2',
        'spark.hadoop.fs.permissions.umask-mode': '000',
    },
    dag=dag,
)

# Task 3: Check output
check_output = BashOperator(
    task_id='check_output',
    bash_command='ls -lh /opt/spark/data/output/test_output/ && echo "Output files created successfully!"',
    dag=dag,
)

# Set dependencies
check_spark >> spark_job >> check_output