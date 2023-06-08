```diff
! Esse repositório pode possuir github actions apontando para ubuntu-latest
- Você precisa alterar para o runner da sua VS. Em caso de dúvida procure o canal no slack de devops
```

# Melhorias no Airflow

Repositório temporário para o projeto de melhorias no Airflow

Dúvidas entrar em contato no canal [#tmp-airflow-melhorias](https://grupoboticario-corp.slack.com/archives/C04BJSNEXCM)

## Qual o objetivo deste repositório?

Repositório temporário para o projeto de melhorias no Airflow

## CI/CD
Atualizar

## Setup
Atualizar

## PGADMIN
No composer foi adicionado um container do PGADMIN para facilitar o acesso ao banco de dados do airflow
Para acessar o PGADMIN4:
- Endereço: http://localhost:16543/
- Email: airflow@grupoboticario.com.br
- Password: airflow

O servidor já vai estar configurado e só será solicitado a senha que é `airflow`

## Testes

### Docker

Para executar todos os testes acesse o container do scheduller
```sh
docker exec -it data-temp-airflow_airflow-scheduler_1 bash
```

Execute a chamada do pytest
```sh
pytest
```

Outros comandos uteis
Adicione o argumento `-s` para que o pytest mostre no console os prints
Executar todos os testes de apenas um diretório especifico
```sh
pytest tests/sensor -s
```
Executar um testes especifico
```sh
pytest tests/sensor/test_gb_external_task_sensor.py::TestGbExternalTaskSensor::test_gb_external_task_sensor_last_valid -s
```

### Local (MAC)

Necessário tem o airflow instalado na virutal env

Configurar as variaveis de ambiente para sobescrever o arquivo de configuração
```sh
export AIRFLOW_HOME='/Users/colaborador/Documents/dev/data-temp-airflow'
```
Ajuste no arquivo de configuração airflow.cfg as configurações abaixo, na teoria deveria funcionar com alteração das
variavel de ambiente também, porém em um teste aqui não funcionou e foi necessário ajustar o arquivo, não comitar a
ateração do arquivo de configuração

```sh
export AIRFLOW__CORE__EXECUTOR='SequentialExecutor'
export AIRFLOW__CORE__DAGS_FOLDER='/Users/colaborador/Documents/dev/data-temp-airflow/dags'
export AIRFLOW__CORE__PLUGINS_FOLDER='/Users/colaborador/Documents/dev/data-temp-airflow/plugins'
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='sqlite:////Users/colaborador/Documents/dev/data-temp-airflow/tmp/airflow.db'
export AIRFLOW__LOGGING__BASE_LOG_FORDER='/Users/colaborador/Documents/dev/data-temp-airflow/logs'
export AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION = '/Users/colaborador/Documents/dev/data-temp-airflow/logs/dag_processor_manager/dag_processor_manager.log'
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY = '/Users/colaborador/Documents/dev/data-temp-airflow/logs/scheduler'
```
