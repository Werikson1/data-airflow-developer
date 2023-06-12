from airflow.models.baseoperator import BaseOperator
from random import randint
from time import sleep

class BigQueryExecuteQueryOperator(BaseOperator):
    def __init__(self, sql, use_legacy_sql, priority, **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.use_legacy_sql = use_legacy_sql
        self.priority = priority

    def execute(self, context):
        if randint(1,10) == 1:
            raise Exception("Falha ao executar o pipeline")
            
        randsleep = randint(10,60)
        sleep(randsleep)
        return f'execution time {randsleep}'


class BigQueryInsertJobOperator(BaseOperator):
    def __init__(self, configuration, **kwargs) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration

    def execute(self, context):
        if randint(1,10) == 1:
            raise Exception("Falha ao executar o pipeline")
            
        randsleep = randint(10,60)
        sleep(randsleep)
        return f'execution time {randsleep}'