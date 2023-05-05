import os
import copy
from jinja2 import Template, Environment, FileSystemLoader

from util import RetryTimes, DelayTime, SensorWaitTime

class Handler:
    available_operator = [
        'SapDataServicesOperator',
        'BigQueryExecuteQueryOperator'
    ]

    def __init__(self, data, output):
        self.data = data
        self.output = output

    def execute(self):
        try:
            self.validate_config()
            self.apply_template()
            self.create_dag()
        except Exception as e:
            print(f"🔥 {str(e)} - {self.doc_id}")

    def load_template(self) -> str:
        if 'raw' in self.data['Table']['ProjectId']:
            template_name = os.environ['DAG_RAW_TEMPLATE']
        else: 
            template_name = os.environ['DAG_TRUSTED_TEMPLATE']

        path = os.path.abspath(os.path.dirname(__file__)) + os.path.join( os.environ['TEMPLATE_PATH'], self.version)
        env =  Environment(loader=FileSystemLoader(path))
        template = env.get_template(template_name)

        return template
    
        # with open(os.path.dirname(os.path.abspath(__file__)) + path, 'r') as template:
        #     return template.read()

    def validate_config(self):
        try:
            if 'DocId' not in self.data['Head']:
                raise Exception("DocId is not defined")
            self.doc_id = self.data['Head']['DocId'].lower()

            if 'Version' not in self.data['Head']:
                raise Exception("Version is not defined")
            self.version = self.data['Head']['Version'].lower()

            if 'StartTime' not in self.data['Head']:
                raise Exception("StartTime is not defined")
            if 'Schedule' not in self.data['Head']:
                raise Exception("Schedule is not defined")
            
            if 'Retries' not in self.data['Head']:
                self.data['Head']['Retries'] = RetryTimes().get_retry_times('Low')
            else:
                self.data['Head']['Retries'] = RetryTimes().get_retry_times(self.data['Head']['Retries'])

            if 'RetryDelay' not in self.data['Head']:
                self.data['Head']['RetryDelay'] = DelayTime().get_delay_time('Short')
            else:
                self.data['Head']['RetryDelay'] = DelayTime().get_delay_time(self.data['Head']['RetryDelay'])

            if 'Gerencia' not in self.data['Head']:
                raise Exception("Gerencia is not defined")
            if 'Coord' not in self.data['Head']:
                raise Exception("Coord is not defined")
            
            if 'ProjectId' not in self.data['Table']:
                raise Exception("ProjectId is not defined")
            if 'DatasetId' not in self.data['Table']:
                raise Exception("DatasetId is not defined")
            if 'TableId' not in self.data['Table']:
                raise Exception("TableId is not defined")
            
            if 'Operator' not in self.data['Task']:
                raise Exception("Operator is not defined")
            if self.data['Task']['Operator'] not in Handler.available_operator:
                raise Exception("Operator is not available")
            
            if 'Dependencies' in self.data:
                for dependency in self.data['Dependencies']:
                    dependency['PokeInterval'] = SensorWaitTime().get_wait_time(dependency['PokeInterval'])

            self.filename = f"dag_{self.doc_id}.py"
        except Exception as e:
            print(str(e))
            raise e

    def apply_template(self):
        try:
            template = self.load_template()
            self.dag = template.render(
                head=self.data['Head'],
                table=self.data['Table'],
                task=self.data['Task'],
                dataquality=self.data['DataQuality'],
                dependencies=self.data['Dependencies'] if 'Dependencies' in self.data else []
                # TODO: sensor_timeout: rule to calculate timeout time for monthly, weekly, daily, hourly, minutely schedule
            )

            return self.dag

        except Exception as e:
            print(str(e))
            raise e

    def create_dag(self):
        try:
            with open(f"{self.output}/{self.filename}", 'w+') as file:
                file.write(self.dag)

        except Exception as e:
            print(str(e))
            raise e
