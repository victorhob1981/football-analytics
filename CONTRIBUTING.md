# Contributing

## Testes

Classificacao adotada:
- `unit` (default): testes rapidos, sem dependencia de Docker/Airflow/Postgres.
- `integration`: testes que dependem de servicos externos (ex.: `docker compose`, Airflow container, banco real).

Markers registrados no `pytest.ini`:
- `integration`

Comandos locais (PowerShell):
```powershell
pytest -q -m "not integration"
pytest -q -m "integration"
pytest -q
```

Atalhos `make`:
```powershell
make test-unit
make test-integration
make test-all
```

## CI

Workflow: `.github/workflows/ci.yml`

- PRs:
  - roda `lint`
  - roda `unit` com `pytest -q -m "not integration"`
  - nao roda `integration`
  - nao roda `dbt-validate`
- Main, nightly (`schedule`) e manual (`workflow_dispatch`):
  - roda `lint` e `unit`
  - roda `integration` com `pytest -q -m "integration"` (com `docker compose up`, wait de container e retry)
  - roda `dbt-validate`
