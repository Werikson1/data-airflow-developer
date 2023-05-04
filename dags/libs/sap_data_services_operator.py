from airflow.models.baseoperator import BaseOperator
from random import randint
from time import sleep

class SapDataServicesOperator(BaseOperator):
    def __init__(self, nameJob, G_Delta_Inicio, G_Delta_Fim, timeout, *args, **kwargs) -> None:
        super().__init__(**kwargs)
        self.nameJob = nameJob
        self.nameRepo = 'DS_ANALYTICS'
        self.G_Delta_Inicio = G_Delta_Inicio
        self.G_Delta_Fim = G_Delta_Fim
        self.timeout = timeout

    def execute(self, context):
        if randint(1,10) == 1:
            raise Exception("Falha ao executar o pipeline")
            
        randsleep = randint(10,60)
        sleep(randsleep)
        return f'execution time {randsleep}'
