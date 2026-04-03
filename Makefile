.PHONY: db-up db-status migrate-up lint test test-unit test-integration test-all quality-p1 p2-verify dbt-docs frontend-release frontend-release-full backend-data-gate backend-data-gate-full

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

frontend-release:
	python tools/frontend_release_gate.py

frontend-release-full:
	python tools/frontend_release_gate.py --mode full

backend-data-gate:
	python tools/backend_data_readiness_gate.py

backend-data-gate-full:
	python tools/backend_data_readiness_gate.py --mode full
