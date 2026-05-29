.PHONY: help up down logs test fmt psql topics clean

help:
	@echo "up      - build & start the whole pipeline (kafka, postgres, airflow, producer, consumer)"
	@echo "down    - stop all services"
	@echo "logs    - tail consumer logs"
	@echo "test    - run the unit test suite locally"
	@echo "psql    - open a psql shell on the warehouse"
	@echo "topics  - list kafka topics"
	@echo "clean   - stop services and delete volumes (wipes data)"

up:
	docker compose up --build -d
	@echo "Airflow UI:  http://localhost:8080  (user: admin)"
	@echo "Password:    docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt"
	@echo "REST API:    http://localhost:8000"

down:
	docker compose down

logs:
	docker compose logs -f consumer

test:
	pip install -r requirements-dev.txt
	pytest -q

psql:
	docker compose exec postgres psql -U pipeline -d warehouse

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

clean:
	docker compose down -v
