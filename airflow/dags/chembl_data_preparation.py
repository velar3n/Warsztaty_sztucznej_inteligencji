from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Load configuration from environment variables
start_date_str = os.getenv("DAG_START_DATE", "2025-12-10")
schedule_interval = os.getenv("DAG_SCHEDULE_INTERVAL", "None")
schedule_interval = None if schedule_interval == "None" else schedule_interval

start_date = datetime.strptime(start_date_str, "%Y-%m-%d")


def generate_eda_report():
    pass


with DAG(
    'chembl_data_preparation',
    start_date=start_date,
    schedule_interval=schedule_interval,
    catchup=False,
    description='Complete ChEMBL data preparation pipeline: clean tables, EDA, create training datasets'
) as dag:

    # 1. Clean tables
    clean_activities = SparkSubmitOperator(
        task_id="clean_activities",
        application="/opt/airflow/spark_jobs/clean_activities.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="clean_activities",
        packages="org.postgresql:postgresql:42.7.3",
    )

    clean_assays = SparkSubmitOperator(
        task_id="clean_assays",
        application="/opt/airflow/spark_jobs/clean_assays.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="clean_assays",
        packages="org.postgresql:postgresql:42.7.3",
    )

    clean_compound_structures = SparkSubmitOperator(
        task_id="clean_compound_structures",
        application="/opt/airflow/spark_jobs/clean_compound_structures.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="clean_compound_structures",
        packages="org.postgresql:postgresql:42.7.3",
    )

    clean_compound_properties = SparkSubmitOperator(
        task_id="clean_compound_properties",
        application="/opt/airflow/spark_jobs/clean_compound_properties.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="clean_compound_properties",
        packages="org.postgresql:postgresql:42.7.3",
    )

    clean_target_dictionary = SparkSubmitOperator(
        task_id="clean_target_dictionary",
        application="/opt/airflow/spark_jobs/clean_target_dictionary.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="clean_target_dictionary",
        packages="org.postgresql:postgresql:42.7.3",
    )

    clean_molecule_dictionary = SparkSubmitOperator(
        task_id="clean_molecule_dictionary",
        application="/opt/airflow/spark_jobs/clean_molecule_dictionary.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="clean_molecule_dictionary",
        packages="org.postgresql:postgresql:42.7.3",
    )

    # 2. EDA and validation
    run_eda = PythonOperator(
        task_id="generate_eda_report",
        python_callable=generate_eda_report,
    )

    # 3. Create training datasets
    prepare_mlp = SparkSubmitOperator(
        task_id="prepare_mlp_data",
        application="/opt/airflow/spark_jobs/prepare_mlp_data.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="prepare_mlp_data",
    )

    prepare_gnn = SparkSubmitOperator(
        task_id="prepare_gnn_data",
        application="/opt/airflow/spark_jobs/prepare_gnn_data.py",
        conn_id="spark_default",
        executor_memory=os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=int(os.getenv("SPARK_TOTAL_EXECUTOR_CORES", "4")),
        name="prepare_gnn_data",
    )

    # All cleaning tasks in parallel
    cleaning_tasks = [
        clean_activities,
        clean_assays,
        clean_compound_structures,
        clean_compound_properties,
        clean_target_dictionary,
        clean_molecule_dictionary
    ]

    # After all cleaning is done, run EDA
    cleaning_tasks >> run_eda

    # After EDA, prepare both training datasets in parallel
    run_eda >> [prepare_mlp, prepare_gnn]
