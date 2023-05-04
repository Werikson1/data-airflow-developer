generate_all_dags:
	python3 dag_factory/main.py

generate_dag:
	python3 dag_factory/main.py --config job_raw_tb_loja_venda_so.yaml