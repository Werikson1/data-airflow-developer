from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from libs.providers.grupoboticario.operators.sap_data_services_operator import SapDataServicesOperator

default_args = {
    'owner': 'Gerencia: Front, Coord: RGM',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'dag_tb_loja_venda_so',
    start_date=datetime.strptime('2023-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'),
    schedule_interval='0 9 * * *',
    default_args=default_args,
    catchup=False,
    tags=['factory']
)

with dag:
    job_tb_loja_venda_so = SapDataServicesOperator(
        task_id='job_tb_loja_venda_so',
        nameJob= 'JOB_GCP_BLZ_RAW_TB_EUD_BLZWEB_ADDRESS',
        G_Delta_Inicio=None,
        G_Delta_Fim=None,
        timeout=480,
        pool='sap-ds-pool',
        trigger_rule='all_done'
    )

    dqe_tb_loja_venda_so = DummyOperator(
        task_id='dqe_tb_loja_venda_so'
    )

    job_tb_loja_venda_so >> dqe_tb_loja_venda_so