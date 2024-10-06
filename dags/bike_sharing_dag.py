from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts folder to Python path to access etl.py
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + '/../scripts'))

# Import the ETL function
from etl import run_etl

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'bergen_bike_sharing',
    default_args=default_args,
    description='ETL pipeline for Bergen Bike Sharing Dataset',
    schedule_interval=timedelta(days=1),
)

# Define the Python task
run_etl_task = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl,
    op_kwargs={'file_path': '/opt/airflow/data/bergen_merged.csv'},  # File path inside the Docker container
    dag=dag,
)

run_etl_task
