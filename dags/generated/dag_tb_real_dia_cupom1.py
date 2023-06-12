from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from libs.providers.grupoboticario.operators.fake_bigquery import BigQueryInsertJobOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from libs.gb_external_task_sensor import GbExternalTaskSensor

default_args = {
    'owner': 'Gerencia: Front, Coord: RGM',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dag_tb_real_dia_cupom1',
    start_date=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
    schedule_interval='0 9 * * *',
    default_args=default_args,
    catchup=False,
    tags=['factory']
)

with dag:

    
    sensor_tb_loja_venda_so = GbExternalTaskSensor(
        task_id='sensor_tb_loja_venda_so',
        external_dag_id='dag_tb_loja_venda_so',
        external_task_id='dq_tb_loja_venda_so',
        poke_interval=2*60,
        timeout=10*60,
        failed_states=['failed', 'skipped', 'upstream_failed'],
        dependency_mode='last_today'
    )

    dqv_tb_loja_venda_so = BigQueryInsertJobOperator(
        task_id='dqv_tb_loja_venda_so',
        configuration={
            'query': {
                'query': 'CALL prc_data_quality',
                'useLegacySql': False,
                'priority': 'BATCH'
            },
        },
        depends_on_past=False
    )
    
    job_tb_real_dia_cupom1 = BigQueryInsertJobOperator(
        task_id='job_tb_real_dia_cupom1',
        configuration={
            'query': {
                'query': 'CALL prc_load_tb_real_dia_cupom',
                'useLegacySql': False,
                'priority': 'BATCH'
            }
        },
        depends_on_past=False
    )

    dqe_tb_real_dia_cupom1 = DummyOperator(
        task_id='dqe_tb_real_dia_cupom1'
    )
    
    sensor_tb_loja_venda_so >> dqv_tb_loja_venda_so >> job_tb_real_dia_cupom1

    job_tb_real_dia_cupom1 >> dqe_tb_real_dia_cupom1