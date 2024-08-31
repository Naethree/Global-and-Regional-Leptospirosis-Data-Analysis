from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import subprocess

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 31),  # Start date set to January 31, 2025
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'leptospirosis_update',
    default_args=default_args,
    description='DAG to update leptospirosis data and upload to MongoDB',
    schedule_interval='0 0 31 1 *',  # Run at midnight on January 31st every year
    catchup=False,
)

# Define Python functions
def run_script(script_path):
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)

def download_and_process():
    run_script('/home/naethree/Users/naethree/airflow/dags/sl.py')

def upload_to_mongodb():
    run_script('/home/naethree/Users/naethree/airflow/dags/sltomongodb.py')

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

download_and_process_task = PythonOperator(
    task_id='download_and_process',
    python_callable=download_and_process,
    dag=dag,
)

upload_to_mongodb_task = PythonOperator(
    task_id='upload_to_mongodb',
    python_callable=upload_to_mongodb,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> download_and_process_task >> upload_to_mongodb_task >> end_task
