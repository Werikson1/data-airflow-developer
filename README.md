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

## Testes

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

