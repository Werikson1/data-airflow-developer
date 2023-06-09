"""
Objeto........: DagDependencySensor.py
Version.......: 1.0.0
Autor.........: Luciano Marqueto
Data Criacao..: 11/04/2023
Descricao.....: Sensor customizado para controle de dependencias.
Contato.......: luciano.marqueto@grupoboticario.com.br
"""
from datetime import timedelta
from typing import Callable, Iterable, Optional

from airflow import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from airflow.models.dagrun import DagRun
from sqlalchemy import func, cast, Integer
import pendulum


class GbExternalTaskSensor(ExternalTaskSensor):
    """
    Waits for a different DAG or a task in a different DAG to complete for a specific logical date.

    :param dependency_mode: How the sensor will select the logical date
        last_today: (default) Get the last execution that happen on the current date
        last_valid: Get the last valid execution, the execution is valid if the current date
            is less than execution_interval_end + (exectution_interval_end-execution_interval_start).
        last_in_range: Get the last execution after current date - tolerance.
        last_success_in_range: Get the last execution after current date - tolerance,
            considering only successful status.
        LAST: Get the last execution no matter when

    :param tolerance: value in minutes that will define the start of the period using (execution_date - tolerance)

    """

    LAST_VALID = 'last_valid'
    LAST_IN_RANGE = 'last_in_range'
    LAST_SUCCESS_IN_RANGE = 'last_success_in_range'
    LAST_TODAY = 'last_today'
    LAST = 'last'
    CUSTOM = 'custom'

    valid_dependency_modes = [LAST_VALID, LAST_IN_RANGE, LAST_SUCCESS_IN_RANGE, LAST_TODAY, LAST, CUSTOM]

    template_fields = ['external_dag_id', 'external_task_id', 'external_task_ids', 'dependency_mode', 'tolerance']

    def __init__(self,
                 *,
                 failed_states: Optional[Iterable[str]] = [],
                 execution_date_fn: Optional[Callable] = None,
                 dependency_mode: str = None,
                 tolerance: int = None,
                 mode="reschedule",
                 **kwargs):

        self.tolerance = tolerance
        self.dependency_mode = dependency_mode
        self.execution_date_external_dag = None


        if self.dependency_mode not in self.valid_dependency_modes:
            raise AirflowException(f"The dependency_mode must be one of {self.valid_dependency_modes}, "
                                   f"received '{self.dependency_mode}'.")
        if self.tolerance is None and self.dependency_mode in [self.LAST_IN_RANGE, self.LAST_SUCCESS_IN_RANGE]:
            raise AirflowException(f"The tolerance must be informed for dependency_mode {self.dependency_mode}'.")
        if (execution_date_fn is not None and self.dependency_mode != self.CUSTOM) \
                or (execution_date_fn is None and self.dependency_mode == self.CUSTOM):
            raise AirflowException(f"The dependency_mode must be {self.CUSTOM} when execution_date_fn is not None")

        execution_date_fn = getattr(self, f'_{self.dependency_mode}')

        super().__init__(failed_states=failed_states,
                         execution_date_fn=execution_date_fn,
                         mode=mode,
                         **kwargs)

        if (self.external_task_id is not None or self.external_task_ids is not None) and self.dependency_mode == self.LAST_SUCCESS_IN_RANGE:
            raise AirflowException(
                f"The external_task_id and external_task_ids are not supported for dependency_mode {self.LAST_SUCCESS_IN_RANGE}'.")

    def _log_info(self, execution_date, last_execution_date, **kwargs):
        print('*' * 50)
        print(f'Execution_date of actual dag {execution_date}')
        print(f'Last execution_date of {kwargs.get("task").external_dag_id}: {last_execution_date}')
        print('*' * 50)

    @provide_session
    def _last_valid(self, execution_date, session=None, **kwargs):
        """
        Find the last valid execution_date of external_dag
        The execution is considered valid if the current date and time is after the end of the last
        execution_interval and before the start of the next one.
        important the start of the next interval is calculated based on the interval of the last run.

        :param execution_date: execution_date of current dagrun
        :param session:
        :param kwargs:
        :return:
        """

        query = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == kwargs.get('task').external_dag_id
        )

        if session.bind.dialect.name == 'sqlite':
            # Devido limitação do SQLite utilizado nos teste unitários o calculo de dada precisa de algumas
            # transformações adicionais, em caso de manutenção desse método é recomendado executar o teste unitário
            # com os dois mecanismos de banco de dados postgres e sqlite
            query = query.filter(
                cast(func.strftime('%s', DagRun.data_interval_end) +
                     (func.strftime('%s', DagRun.data_interval_end) -
                      func.strftime('%s', DagRun.data_interval_start)), Integer) >
                cast(func.strftime('%s', func.datetime(execution_date)), Integer)
            )
        else:
            query = query.filter(
                DagRun.data_interval_end + (DagRun.data_interval_end - DagRun.data_interval_start) > execution_date
            )

        last_execution_date = query.scalar()

        self._log_info(execution_date, last_execution_date, **kwargs)
        # Essa variavel é utilizada pelos testes unitario
        self.execution_date_external_dag = last_execution_date
        # Garante que um valor é retornado para o sensor
        last_execution_date = last_execution_date if last_execution_date is not None else pendulum.now().add(days=1)
        return last_execution_date

    @provide_session
    def _last_in_range(self, execution_date, session=None, **kwargs):
        """
        Find the last execution_date of external_dag considering a tolerance time.

        :param execution_date: data de execução da dag atual
        :param session:
        :param kwargs:
        :return:
        """

        start_period = execution_date - timedelta(minutes=self.tolerance)

        last_execution_date = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == kwargs.get('task').external_dag_id,
            DagRun.execution_date >= start_period
        ).scalar()

        self._log_info(execution_date, last_execution_date, **kwargs)
        # Essa variavel é utilizada pelos testes unitario
        self.execution_date_external_dag = last_execution_date
        # Garante que um valor é retornado para o sensor
        last_execution_date = last_execution_date if last_execution_date is not None else pendulum.now().add(days=1)
        return last_execution_date

    @provide_session
    def _last_success_in_range(self, execution_date, session=None, **kwargs):
        """
        Find the last execution_date of external_dag considering a tolerance time and dag status success,
        this work better for dag and can be confusing for task dependency.

        :param execution_date: data de execução da dag atual
        :param session:
        :param kwargs:
        :return:
        """

        start_period = execution_date - timedelta(minutes=self.tolerance)

        last_execution_date = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == kwargs.get('task').external_dag_id,
            DagRun.execution_date >= start_period,
            DagRun.state == 'success'
        ).scalar()

        self._log_info(execution_date, last_execution_date, **kwargs)
        # Essa variavel é utilizada pelos testes unitario
        self.execution_date_external_dag = last_execution_date
        # Garante que um valor é retornado para o sensor
        last_execution_date = last_execution_date if last_execution_date is not None else pendulum.now().add(days=1)
        return last_execution_date

    @provide_session
    def _last_today(self, execution_date, session=None, **kwargs):
        """
        Find the last execution_date of external_dag that happen on today.

        :param execution_date: data de execução da dag atual
        :param session:
        :param kwargs:
        :return:
        """

        query = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == kwargs.get('task').external_dag_id,
            DagRun.execution_date >= pendulum.today(),
        )

        last_execution_date = query.scalar()

        self._log_info(execution_date, last_execution_date, **kwargs)
        # Essa variavel é utilizada pelos testes unitario
        self.execution_date_external_dag = last_execution_date
        # Garante que um valor que não tenha um dagrun é retornado para o sensor
        last_execution_date = last_execution_date if last_execution_date is not None else pendulum.now().add(days=1)
        return last_execution_date

    @provide_session
    def _last(self, execution_date, session=None, **kwargs):
        """
        Find the last execution_date of external_dag.

        :param execution_date: data de execução da dag atual
        :param session:
        :param kwargs:
        :return:
        """

        last_execution_date = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == kwargs.get('task').external_dag_id,
        ).scalar()

        self._log_info(execution_date, last_execution_date, **kwargs)
        # Essa variavel é utilizada pelos testes unitario
        self.execution_date_external_dag = last_execution_date
        # Garante que um valor é retornado para o sensor
        last_execution_date = last_execution_date if last_execution_date is not None else pendulum.now().add(days=1)
        return last_execution_date
