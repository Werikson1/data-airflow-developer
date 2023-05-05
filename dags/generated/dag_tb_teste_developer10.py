from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from libs.bigquery_operator import BigQueryExecuteQueryOperator
from libs.last_execution_external_task_sensor import LastExecutionExternalTaskSensor

default_args = {
    'owner': 'Gerencia: Front, Coord: RGM',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dag_tb_teste_developer10',
    start_date=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
    schedule_interval='0 9 * * *',
    default_args=default_args,
    catchup=False,
    tags=['factory']
)

with dag:

    
    sensor_tb_loja_venda_so = LastExecutionExternalTaskSensor(
        task_id='sensor_tb_loja_venda_so',
        external_dag_id='dag_tb_loja_venda_so',
        external_task_id='dq_tb_loja_venda_so',
        poke_interval=2*60,
        timeout=10*60,
        failed_states=['failed', 'skipped', 'upstream_failed']
    )

    dq_tb_loja_venda_so = BigQueryExecuteQueryOperator(
        task_id='dq_tb_loja_venda_so',
        sql= 'CALL prc_data_quality',
        use_legacy_sql=False,
        depends_on_past=False,
        priority="BATCH"
    )
    
    job_tb_teste_developer10 = BigQueryExecuteQueryOperator(
        task_id='job_tb_teste_developer10',
        sql= 'CALL prc_load_tb_real_dia_cupom',
        use_legacy_sql=False,
        depends_on_past=False,
        priority="BATCH"
    )

    dq_tb_teste_developer10 = DummyOperator(
        task_id='dq_tb_teste_developer10'
    )
    
    sensor_tb_loja_venda_so >> dq_tb_loja_venda_so >> job_tb_teste_developer10

    job_tb_teste_developer10 >> dq_tb_teste_developer10