init:
	docker compose up airflow-init

up:
	docker compose up -d
	
down:
	docker compose down

build:
	docker compose build --no-cache

bash-as: #apiserver
	docker exec -it spotify-etl-pipeline-airflow-apiserver-1 bash