from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import random

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
    time.sleep(5)
    if random.randint(1, 20) == 1:
        raise Exception("Falha para teste")
    print(f"The {kwargs['task_type']} task has completed.")

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_test_sensor_parent_1',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval='0 * * * *',
    default_args=default_args,
    catchup=False,
    tags=['test-airflow']
) as dag:

    start_task = PythonOperator(
        task_id='starting_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'starting'}
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_task_type,
        op_kwargs={'task_type': 'ending'}
    )

    start_task >> end_task
