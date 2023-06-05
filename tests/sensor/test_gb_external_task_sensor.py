import pytest
import os

from airflow import settings
from airflow.models import DagBag, TaskInstance
from airflow.models.dag import DAG
from airflow.utils.state import DagRunState
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from airflow.exceptions import AirflowException

from sqlalchemy import create_engine


from tests.test_utils.db import clear_db_runs
from dags.libs.gb_external_task_sensor import GbExternalTaskSensor


DEFAULT_DATE = datetime(2023, 1, 1)
TASK_SENSOR_ID = "test_gb_external_task_sensor_check"
DAG_A = 'DAG_A'
DAG_B = 'DAG_B'
DEV_NULL = "/dev/null"

class TestGbExternalTaskSensor:
    def setup_method(self):
        if os.path.exists("test.db"):
            os.remove("test.db")
        self.dagbag = DagBag(dag_folder=DEV_NULL, include_examples=False)
        self.args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        db_path = os.path.join(os.getcwd(), "airflow.db")
        settings.engine = create_engine("sqlite:////home/runner/airflow/airflow.db")
        
           


    def create_dag_runs(self, dag, config):
        for conf in config:
            dag.create_dagrun(state=conf['state'],
                            execution_date=conf['execution_date'],
                            run_type=DagRunType.MANUAL)

    def get_ti_states(self, task_sensor_id=TASK_SENSOR_ID, dag_id=DAG_B):
        return settings.Session().query(TaskInstance).filter(
            TaskInstance.task_id == task_sensor_id,
            TaskInstance.dag_id == dag_id,
        ).all()

    def test_gb_external_task_sensor_last_valid(self):
        clear_db_runs()
        dag_a = DAG(DAG_A, default_args=self.args, schedule_interval="0 1 * * *")
        # Necessário colocar 1 hora da manhã para gerar o calculo correto do intervalo
        config = [
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 1, 1)},
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 2, 1)},
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 3, 1)},
        ]
        self.create_dag_runs(dag_a, config)

        dag_b = DAG(DAG_B,
                    default_args=self.args,
                    schedule_interval="@daily")
        op = GbExternalTaskSensor(
            task_id=TASK_SENSOR_ID,
            external_dag_id=DAG_A,
            dag=dag_b,
            timeout=2,
        )

        # DAB_B com data anterior a DAG_A, deve pegar o dagrun mais atual da DAG_A
        # O status deve ser sucesso
        ti_a_execution_date = datetime(2023, 1, 1)
        expect_states = {ti_a_execution_date: 'success'}
        op.run(start_date=ti_a_execution_date, end_date=ti_a_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data identica a ultima execução da DAG_A, deve pegar a ultima execução da DAG_A
        # O status deve ser sucesso
        ti_b_execution_date = datetime(2023, 1, 3)
        expect_states[ti_b_execution_date] = 'success'
        op.run(start_date=ti_b_execution_date, end_date=ti_b_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data a frente da validade da ultima execução da DAG_A, Não deve obter nenhum dagrun
        # O status deve ser up_for_reschedule pois não localizou um dagrun valido
        ti_c_execution_date = datetime(2023, 1, 4)
        expect_states[ti_c_execution_date] = 'up_for_reschedule'
        op.run(start_date=ti_c_execution_date, end_date=ti_c_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag is None

        # Obtem o status do sensor
        tis = self.get_ti_states()

        # Valida todos os status
        for ti in tis:
            assert expect_states[ti.execution_date] == ti.state

    def test_gb_external_task_sensor_last_valid_failed_dep(self):
        clear_db_runs()
        dag_a = DAG(DAG_A, default_args=self.args, schedule_interval="0 1 * * *")
        # Necessário colocar 1 hora da manhã para gerar o calculo correto do intervalo
        config = [
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 1, 1)},
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 2, 1)},
            {'state': DagRunState.FAILED, 'execution_date': datetime(2023, 1, 3, 1)},
        ]
        self.create_dag_runs(dag_a, config)

        dag_b = DAG(DAG_B,
                    default_args=self.args,
                    schedule_interval="@daily")
        op = GbExternalTaskSensor(
            task_id=TASK_SENSOR_ID,
            external_dag_id=DAG_A,
            dag=dag_b,
            timeout=2,
        )

        # DAB_B com data anterior a DAG_A, deve pegar o dagrun mais atual da DAG_A
        # O status deve ser 'up_for_reschedule' pois a DAG_B falhou
        ti_a_execution_date = datetime(2023, 1, 1)
        expect_states = {ti_a_execution_date: 'up_for_reschedule'}
        op.run(start_date=ti_a_execution_date, end_date=ti_a_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data identica a ultima execução da DAG_A, deve pegar a ultima execução da DAG_A
        # O status deve ser 'up_for_reschedule' pois a DAG_B falhou
        ti_b_execution_date = datetime(2023, 1, 3)
        expect_states[ti_b_execution_date] = 'up_for_reschedule'
        op.run(start_date=ti_b_execution_date, end_date=ti_b_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data a frente da validade da ultima execução da DAG_A, Não deve obter nenhum dagrun
        # O status deve ser up_for_reschedule pois não localizou um dagrun valido
        ti_c_execution_date = datetime(2023, 1, 4)
        expect_states[ti_c_execution_date] = 'up_for_reschedule'
        op.run(start_date=ti_c_execution_date, end_date=ti_c_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag is None

        # Obtem o status do sensor
        tis = self.get_ti_states()

        # Valida todos os status
        for ti in tis:
            assert expect_states[ti.execution_date] == ti.state

    def test_gb_external_task_sensor_no_tolerance(self):
        #Campo tolerance é obrigatorio nos modo LAST_IN_RANGE e LAST_SUCCESS_IN_RANGE

        dag_b = DAG(DAG_B,
                    default_args=self.args,
                    schedule_interval="@daily")

        with pytest.raises(AirflowException) as e_info:
            GbExternalTaskSensor(
                task_id=TASK_SENSOR_ID,
                external_dag_id=DAG_A,
                dependency_mode=GbExternalTaskSensor.LAST_IN_RANGE,
                dag=dag_b,
                timeout=2,
            )

        with pytest.raises(AirflowException) as e_info:
            GbExternalTaskSensor(
                task_id=TASK_SENSOR_ID,
                external_dag_id=DAG_A,
                dependency_mode=GbExternalTaskSensor.LAST_SUCCESS_IN_RANGE,
                dag=dag_b,
                timeout=2,
            )

    def test_gb_external_task_sensor_with_task_id(self):
        #external_task_ids e external_task_id não suportado no modo LAST_SUCCESS_IN_RANGE

        dag_b = DAG(DAG_B,
                    default_args=self.args,
                    schedule_interval="@daily")

        with pytest.raises(AirflowException) as e_info:
            GbExternalTaskSensor(
                task_id=TASK_SENSOR_ID,
                external_dag_id=DAG_A,
                tolerance=1,
                external_task_ids=['A'],
                dependency_mode=GbExternalTaskSensor.LAST_SUCCESS_IN_RANGE,
                dag=dag_b,
                timeout=2,
            )

        with pytest.raises(AirflowException) as e_info:
            GbExternalTaskSensor(
                task_id=TASK_SENSOR_ID,
                external_dag_id=DAG_A,
                tolerance=1,
                external_task_id='A',
                dependency_mode=GbExternalTaskSensor.LAST_SUCCESS_IN_RANGE,
                dag=dag_b,
                timeout=2,
            )

    def test_gb_external_task_sensor_last_in_range(self):
        clear_db_runs()
        dag_a = DAG(DAG_A, default_args=self.args, schedule_interval="0 1 * * *")
        # Necessário colocar 1 hora da manhã para gerar o calculo correto do intervalo
        config = [
            {'state': DagRunState.FAILED, 'execution_date': datetime(2023, 1, 1, 1)},
            {'state': DagRunState.FAILED, 'execution_date': datetime(2023, 1, 2, 1)},
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 3, 1)},
        ]
        self.create_dag_runs(dag_a, config)

        dag_b = DAG(DAG_B,
                    default_args=self.args,
                    schedule_interval="@daily")
        op = GbExternalTaskSensor(
            task_id=TASK_SENSOR_ID,
            external_dag_id=DAG_A,
            dependency_mode=GbExternalTaskSensor.LAST_IN_RANGE,
            tolerance=1440,
            dag=dag_b,
            timeout=2,
        )

        # DAB_B com data anterior a DAG_A, deve pegar o dagrun mais atual da DAG_A
        # O status deve ser 'sucesso'
        ti_a_execution_date = datetime(2023, 1, 1)
        expect_states = {ti_a_execution_date: 'success'}
        op.run(start_date=ti_a_execution_date, end_date=ti_a_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data identica a ultima execução da DAG_A, deve pegar a ultima execução da DAG_A
        # O status deve ser 'sucesso'
        ti_b_execution_date = datetime(2023, 1, 3)
        expect_states[ti_b_execution_date] = 'success'
        op.run(start_date=ti_b_execution_date, end_date=ti_b_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data a frente da ultima execução da DAG_A mas dentro da tolerancia, deve pegar a ultima execução da DAG_A
        # O status deve ser success
        ti_c_execution_date = datetime(2023, 1, 4)
        expect_states[ti_c_execution_date] = 'success'
        op.run(start_date=ti_c_execution_date, end_date=ti_c_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 3, 1)

        # DAB_B com data a frente da ultima execução da DAG_A e fora da tolerancia, não deve localizar uma DAGRUN
        # O status deve ser up_for_reschedule pois não localizou um dagrun valido
        ti_c_execution_date = datetime(2023, 1, 5)
        expect_states[ti_c_execution_date] = 'up_for_reschedule'
        op.run(start_date=ti_c_execution_date, end_date=ti_c_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag is None

        # Obtem o status do sensor
        tis = self.get_ti_states()

        # Valida todos os status
        for ti in tis:
            assert expect_states[ti.execution_date] == ti.state

    def test_gb_external_task_sensor_last_success_in_range(self):
        clear_db_runs()
        dag_a = DAG(DAG_A, default_args=self.args, schedule_interval="0 1 * * *")
        # Necessário colocar 1 hora da manhã para gerar o calculo correto do intervalo
        config = [
            {'state': DagRunState.FAILED, 'execution_date': datetime(2023, 1, 1, 1)},
            {'state': DagRunState.SUCCESS, 'execution_date': datetime(2023, 1, 2, 1)},
            {'state': DagRunState.FAILED, 'execution_date': datetime(2023, 1, 3, 1)},
        ]
        self.create_dag_runs(dag_a, config)

        dag_b = DAG(DAG_B,
                    default_args=self.args,
                    schedule_interval="@daily")
        op = GbExternalTaskSensor(
            task_id=TASK_SENSOR_ID,
            external_dag_id=DAG_A,
            dependency_mode=GbExternalTaskSensor.LAST_SUCCESS_IN_RANGE,
            tolerance=1440,
            dag=dag_b,
            timeout=2,
        )

        # DAB_B com data anterior a DAG_A, deve pegar o dagrun mais atual com status de sucesso da DAG_A
        # O status deve ser 'sucesso'
        ti_a_execution_date = datetime(2023, 1, 1)
        expect_states = {ti_a_execution_date: 'success'}
        op.run(start_date=ti_a_execution_date, end_date=ti_a_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 2, 1)

        # DAB_B com data identica a ultima execução da DAG_A porem a execução com sucesso esta dentro da tolerancia,
        # deve pegar a ultima execução com sucesso da DAG_A
        # O status deve ser 'sucesso'
        ti_b_execution_date = datetime(2023, 1, 3)
        expect_states[ti_b_execution_date] = 'success'
        op.run(start_date=ti_b_execution_date, end_date=ti_b_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag == datetime(2023, 1, 2, 1)

        # DAB_B com data a frente da ultima execução da DAG_A e fora da tolerancia da ultima execução com sucesso,
        # Não deve pegar nenhuma execução
        # O status deve ser up_for_reschedule
        ti_c_execution_date = datetime(2023, 1, 4)
        expect_states[ti_c_execution_date] = 'up_for_reschedule'
        op.run(start_date=ti_c_execution_date, end_date=ti_c_execution_date, ignore_ti_state=True)
        assert op.execution_date_external_dag is None

        # Obtem o status do sensor
        tis = self.get_ti_states()

        # Valida todos os status
        for ti in tis:
            assert expect_states[ti.execution_date] == ti.state

    # session = settings.Session()
    # execs = session.query(DagRun).filter(
    #     DagRun.dag_id == 'DAG_A'
    # ).all()
    #
    # for e in execs:
    #     print(f'{e.execution_date} - {e.data_interval_start} - {e.data_interval_end}')
