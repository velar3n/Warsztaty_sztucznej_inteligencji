"""
Simple test DAG to verify Airflow is working correctly.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    catchup=False,
    tags=['test', 'example'],
)


def print_hello():
    """Simple Python function."""
    print("Hello from Airflow!")
    print("Current time:", datetime.now())
    return "Hello task completed"


def print_context(**context):
    """Print execution context."""
    print(f"Execution date: {context['execution_date']}")
    print(f"DAG run ID: {context['dag_run'].run_id}")
    return "Context task completed"


# Task 1: Simple bash command
task_bash = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Task 2: Python function
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)


# Set task dependencies
task_bash >> task_hello