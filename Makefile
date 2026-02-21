.PHONY: db-up db-status migrate-up lint test test-unit test-integration test-all quality-p1 p2-verify dbt-docs

db-up:
	docker compose run --rm dbmate --migrations-dir /db/migrations up

db-status:
	docker compose run --rm dbmate --migrations-dir /db/migrations status

migrate-up: db-up

lint:
	ruff check . --select E9,F63,F7,F82

test:
	python -m pytest -q -m "not integration"

test-unit:
	python -m pytest -q -m "not integration"

test-integration:
	python -m pytest -q -m "integration"

test-all:
	python -m pytest -q

quality-p1:
	python tools/quality_p1.py

p2-verify:
	python tools/p2_verify.py

dbt-docs:
	docker compose exec -T airflow-webserver dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
	docker compose exec -T airflow-webserver dbt docs generate --empty-catalog --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
