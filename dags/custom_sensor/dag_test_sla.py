from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import time, random

def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    with open("dags/temporary.txt", "a") as f:
        f.write("dag: "+dag)
    print(
        "The callback arguments are: ",
        {
            "dag": dag,
            "task_list": task_list,
            "blocking_task_list": blocking_task_list,
            "slas": slas,
            "blocking_tis": blocking_tis,
        },
    )

def print_task_type(**kwargs):
    """
    Example function to call before and after dependent DAG.
    """
    time.sleep(30)
    if random.randint(1,10) < 5:
        raise Exception("Falha de execução")
    else:
        print(f"The {kwargs['task_type']} task has completed.")

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'sla': timedelta(seconds=10)
}

dag = DAG(
    'dag_test_sla',
    start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule_interval="*/3 * * * *",
    default_args=default_args,
    sla_miss_callback=sla_callback,
    catchup=False,
    tags=['test-sla'])

start_task = BashOperator(
    task_id='sleep_for_20s',  # Cause the task to miss the SLA
    bash_command='sleep 20',
    dag=dag
)

end_task = BashOperator(
     task_id='sleep_for_10s',  # Cause the task to miss the SLA
    bash_command='sleep 10',
    dag=dag
)

start_task >> end_task