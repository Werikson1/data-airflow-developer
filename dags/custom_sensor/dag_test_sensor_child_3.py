from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from libs.gb_external_task_sensor import GbExternalTaskSensor

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
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

dag = DAG(
    'dag_test_sensor_child_3',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval="20 */3 * * *",
    default_args=default_args,
    catchup=False,
    tags=['test-airflow'])


child_task1 = GbExternalTaskSensor(
    task_id="child_task1",
    external_dag_id="dag_test_sensor_parent_1",
    external_task_id="end_task",
    dag=dag
)

child_task2 = GbExternalTaskSensor(
    task_id="child_task2",
    external_dag_id="dag_test_sensor_parent_2",
    external_task_id="end_task",
    dag=dag
)


child_task3 = GbExternalTaskSensor(
    task_id="child_task3",
    external_dag_id="dag_test_sensor_parent_3",
    external_task_id="end_task",
    dag=dag
)

start_task = PythonOperator(
    task_id='starting_task',
    python_callable=print_task_type,
    op_kwargs={'task_type': 'starting'},
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=print_task_type,
    op_kwargs={'task_type': 'ending'},
    dag=dag
)

[
    child_task1,
    child_task2
] >> start_task >> end_task
