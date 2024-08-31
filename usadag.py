from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'usadag',
    default_args=default_args,
    description='Upload USA CSV data to MongoDB',
    schedule_interval=None,  # Set to your preferred schedule
)

def run_script(script_path):
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)

# Define the PythonOperator for the upload script
upload_usa_to_mongodb_task = PythonOperator(
    task_id='upload_usa_to_mongodb',
    python_callable=lambda: run_script('/home/naethree/Users/naethree/airflow/dags/usatomongodb.py'),
    dag=dag,
)

upload_usa_to_mongodb_task
