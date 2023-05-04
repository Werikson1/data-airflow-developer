import unittest
from airflow.models import DagBag
from airflow.models import DAG
from airflow.models import TaskInstance
from airflow.utils.state import State
import pendulum

from airflow.utils.session import provide_session
from airflow.models.dagrun import DagRun


class TestCase(unittest.TestCase):
    def setUp(self):
        self._DAG_ID = 'dag_test_sensor_child_fn'
        self.DEFAULT_DATE = pendulum.datetime(2022, 9, 15)
        self.dag_bag = DagBag()
        self.dag = self.dag_bag.get_dag(dag_id=self._DAG_ID)

    def test_dag_integrity(self):
        self.assertEqual(self.dag_bag.import_errors, {}, self.dag_bag.import_errors)
        self.assertIsNotNone(self.dag)
        self.assertEqual(4, len(self.dag.tasks))


    def test_execution_success(self):
        execution_date = pendulum.now()
        self.dag.create_dagrun(
            run_id=f'test_{execution_date}',
            execution_date=execution_date,
            state=State.RUNNING)
        task = self.dag.get_task(task_id='child_task2')
        ti = TaskInstance(task=task, execution_date=execution_date)
        ti.run(ignore_ti_state=True)
        self.test()

    @provide_session
    def test(self, session=None):
        # todas as execuções
        execucoes = session.query(DagRun.data_interval_start, DagRun.data_interval_end, DagRun.execution_date).filter(
            DagRun.dag_id == self._DAG_ID,

        ).all()
        for execucao in execucoes:
            print(f"Execution_date {execucao[2]}, start_interval {execucao[0]}, end_interval {execucao[1]}, intervalo {execucao[1] - execucao[0]}")


if __name__ == '__main__':
    unittest.main()
