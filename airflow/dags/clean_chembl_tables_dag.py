"""
Airflow DAG to clean ChEMBL database tables using Spark.
Reads from PostgreSQL, cleans data, and saves to Parquet format.
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
    'clean_chembl_tables',
    default_args=default_args,
    description='Clean ChEMBL database tables and save as Parquet',
    catchup=False,
    tags=['chembl', 'cleaning', 'spark'],
)

# Task 1: Check Spark cluster status
check_spark = BashOperator(
    task_id='check_spark_cluster',
    bash_command='echo "Checking Spark cluster..." && curl -s http://spark-master:8080 > /dev/null && echo "Spark Master is healthy!"',
    dag=dag,
)

# Common Spark config
spark_conf = {
    'spark.master': 'spark://spark-master:7077',
    'spark.executor.memory': '2g',
    'spark.executor.cores': '2',
    'spark.cores.max': '4',
    'spark.hadoop.fs.permissions.umask-mode': '000',
    'spark.jars.packages': 'org.postgresql:postgresql:42.7.4',
}

env_vars = {
    'CHEMBL_DB_HOST': 'host.docker.internal',
    'CHEMBL_DB_PORT': '5432',
    'CHEMBL_DB_NAME': 'chembl',
    'CHEMBL_DB_USER': 'postgres',
    'CHEMBL_DB_PASSWORD': 'postgres',
    'DATA_CLEANED_PATH': '/opt/spark/data/cleaned',
    'PYTHONPATH': '/opt/spark/jobs',
}

# Load jobs
load_compound_properties = SparkSubmitOperator(
    task_id='load_compound_properties',
    application='/opt/spark/jobs/load_compound_properties.py',
    conn_id='spark_default',
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag,
)

load_compound_structures = SparkSubmitOperator(
    task_id='load_compound_structures',
    application='/opt/spark/jobs/load_compound_structures.py',
    conn_id='spark_default',
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag,
)

load_target_dictionary = SparkSubmitOperator(
    task_id='load_target_dictionary',
    application='/opt/spark/jobs/load_target_dictionary.py',
    conn_id='spark_default',
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag,
)

load_assays = SparkSubmitOperator(
    task_id='load_assays',
    application='/opt/spark/jobs/load_assays.py',
    conn_id='spark_default',
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag,
)

load_activities = SparkSubmitOperator(
    task_id='load_activities',
    application='/opt/spark/jobs/load_activities.py',
    conn_id='spark_default',
    conf={**spark_conf, 'spark.cores.max': '8'},  # More cores for large table
    env_vars=env_vars,
    dag=dag,
)

# Join all tables
join_all_tables = SparkSubmitOperator(
    task_id='join_all_tables',
    application='/opt/spark/jobs/join_chembl_tables.py',
    conn_id='spark_default',
    conf=spark_conf,
    env_vars=env_vars,
    dag=dag,
)

# Verify outputs
verify_outputs = BashOperator(
    task_id='verify_all_outputs',
    bash_command='ls -lh /opt/spark/data/cleaned/chembl_joined.parquet && echo "ChEMBL tables joined successfully!"',
    dag=dag,
)

# Set dependencies - check spark, parallel loading, then join, copy final output to Windows mount, then verify
check_spark >> [load_compound_properties, load_compound_structures, load_target_dictionary, load_assays, load_activities] >> join_all_tables >> verify_outputs
