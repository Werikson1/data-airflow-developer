from typing import Callable, Iterable, Optional

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session
from airflow.models.dagrun import DagRun
from sqlalchemy import func


class LastExecutionExternalTaskSensor(ExternalTaskSensor):

    def __init__(self,
                 *,
                 failed_states: Optional[Iterable[str]] = ['failed', 'skipped'],
                 execution_date_fn: Optional[Callable] = None,
                 tolerance: str =None,
                 mode="reschedule",
                 **kwargs):

        if execution_date_fn is None:
            execution_date_fn = self.last_execution

        super().__init__(failed_states=failed_states,
                         execution_date_fn=execution_date_fn,
                         mode=mode,
                         **kwargs)

        self.tolerance = tolerance


    @provide_session
    def last_execution(self, execution_date, session=None, **kwargs):
        """
        Procura o execution_date da dag external_dag
        Considera o campo tolerance se for informado, caso contrário calcula o ultimo intervalo valido da dag

        :param execution_date: data de execução da dag atual
        :param session:
        :param kwargs:
        :return:
        """

        # Para logar as execuçoes para analise (apenas para teste remover depois)
        # execucoes = session.query(func.max(DagRun.data_interval_start), func.max(DagRun.data_interval_end),
        #                           func.max(DagRun.execution_date)).filter(
        #     DagRun.dag_id == kwargs.get('task').external_dag_id,
        # ).all()
        # for execucao in execucoes:
        #     print(
        #         f"Execution_date {execucao[2]}, start_interval {execucao[0]}, end_interval {execucao[1]}, intervalo {execucao[1] - execucao[0]}")

        # tolerance = kwargs.get('task').params.get('tolerance')
        if self.tolerance is None:
            # Se a tolerancia não for informada é calculado com base ultimo intevalo de execucao
            last_execution_date = session.query(func.max(DagRun.execution_date)).filter(
                DagRun.dag_id == kwargs.get('task').external_dag_id,
                DagRun.data_interval_end + (DagRun.data_interval_end - DagRun.data_interval_start) > execution_date
            ).scalar()

        else:
            last_execution_date = session.query(func.max(DagRun.execution_date)).filter(
                DagRun.dag_id == kwargs.get('task').external_dag_id,
                DagRun.execution_date >= execution_date - self.tolerance
            ).scalar()

        print('*' * 50)
        print(f'Execution_date of actual dag {execution_date}')
        print(f'Last execution_date of {kwargs.get("task").external_dag_id}: {last_execution_date}')
        print('*' * 50)

        last_execution_date = last_execution_date if last_execution_date is not None else execution_date

        return last_execution_date
