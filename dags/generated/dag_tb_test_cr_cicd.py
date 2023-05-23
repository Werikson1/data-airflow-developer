from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from libs.bigquery_operator import BigQueryExecuteQueryOperator
from libs.last_execution_external_task_sensor import LastExecutionExternalTaskSensor

default_args = {
    'owner': 'Gerencia: Front, Coord: Consumer2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dag_tb_test_cr_cicd',
    start_date=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
    schedule_interval='0 9 * * *',
    default_args=default_args,
    catchup=False,
    tags=['factory,cr']
)

with dag:

    
    sensor_tb_test_cr_cicd = LastExecutionExternalTaskSensor(
        task_id='sensor_tb_test_cr_cicd',
        external_dag_id='dag_tb_test_cr_cicd',
        external_task_id='dq_tb_test_cr_cicd',
        poke_interval=2*60,
        timeout=10*60,
        failed_states=['failed', 'skipped', 'upstream_failed']
    )

    dq_tb_test_cr_cicd = BigQueryExecuteQueryOperator(
        task_id='dq_tb_test_cr_cicd',
        sql= 'CALL prc_data_quality',
        use_legacy_sql=False,
        depends_on_past=False,
        priority="BATCH"
    )
    
    job_tb_test_cr_cicd = BigQueryExecuteQueryOperator(
        task_id='job_tb_test_cr_cicd',
        sql= 'CALL prc_load_tb_test_cr_cicd',
        use_legacy_sql=False,
        depends_on_past=False,
        priority="BATCH"
    )

    dq_tb_test_cr_cicd = DummyOperator(
        task_id='dq_tb_test_cr_cicd'
    )
    
    sensor_tb_test_cr_cicd >> dq_tb_test_cr_cicd >> job_tb_test_cr_cicd

    job_tb_test_cr_cicd >> dq_tb_test_cr_cicd