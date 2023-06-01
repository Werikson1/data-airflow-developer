generate_all_dags:
	python3 dag_factory/main.py

generate_dag:
	python3 dag_factory/main.py --config job_raw_tb_loja_venda_so.yaml

# https://migueldoctor.medium.com/how-to-run-postgresql-pgadmin-in-3-steps-using-docker-d6fe06e47ca1
pgadmin:
	docker pull dpage/pgadmin4:latest
	docker run --name pgadmin -p 5050:80 -e 'PGADMIN_DEFAULT_EMAIL=admin@admin.com' -e 'PGADMIN_DEFAULT_PASSWORD=admin' -d dpage/pgadmin4
	open localhost:5050
# hostname: host.lima.internal
# port: 4232
# database: airflow
# user: airflow
# pass: airflow