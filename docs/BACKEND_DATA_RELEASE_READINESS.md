# BACKEND_DATA_RELEASE_READINESS

Data de atualização: 2026-03-22  
Escopo: readiness operacional mínima do backend/BFF e da stack de dados fora do código de produto.

## 1. Gate operacional mínimo

Comando único a partir da raiz do repositório:

```powershell
python tools/backend_data_readiness_gate.py
```

O gate mínimo executa, nesta ordem:
1. `python -m ruff check . --select E9,F63,F7,F82`
2. `python -m pytest -q api/tests tests -m "not integration"`

Atalho `make`:

```powershell
make backend-data-gate
```

Resumo da execução:
- gravado em `artifacts/backend_data_gate_<mode>_<timestamp_utc>/summary.txt`
- status final `PASS` ou `FAIL`
- duração por etapa e total

## 2. Gate completo com stack live

Quando a intenção for validar demo técnica, entrega ou readiness mais forte da stack:

```powershell
python tools/backend_data_readiness_gate.py --mode full
```

O modo `full` executa:
1. gate mínimo
2. `docker compose ps --status running` com exigência explícita de `airflow-webserver` e `postgres`
3. `python -m pytest -q tests -m "integration"`

Atalho `make`:

```powershell
make backend-data-gate-full
```

Pré-requisito do modo `full`:
- Docker Desktop rodando
- stack local disponível com `postgres` e `airflow-webserver`

Subida mínima recomendada:

```powershell
docker compose up -d postgres dbmate airflow-init airflow-webserver
```

## 3. Bloqueante vs nao bloqueante

Bloqueante:
- `backend_data_readiness_gate.py` falhar em qualquer etapa
- `ruff` falhar
- testes unitários de `api/tests` e `tests` falharem
- no modo `full`, Docker/stack indisponíveis ou `tests -m "integration"` falharem

Nao bloqueante neste escopo:
- deploy automatizado inexistente
- pipeline separado de release além do CI atual
- validações pesadas de replay/backfill fora do escopo útil de readiness

## 4. Checklist curto de validação/release técnica

1. Rodar `python tools/backend_data_readiness_gate.py`.
2. Se a entrega exigir validação live da stack, subir `postgres` + `airflow-webserver` e rodar `python tools/backend_data_readiness_gate.py --mode full`.
3. Confirmar `PASS` no resumo e guardar o diretório `artifacts/backend_data_gate_<mode>_<timestamp_utc>/`.
4. Revisar o BFF com pelo menos:
   - `/docs`
   - `/api/v1/health`
   - rotas críticas já cobertas pelos testes de `api/tests`
5. Se houver falha no modo `full`, tratar como bloqueante para demo técnica da stack.

## 5. Automação mínima no repositório

Automação criada/organizada neste escopo:
- `tools/backend_data_readiness_gate.py`: gate local reproduzível
- `Makefile`: atalhos `backend-data-gate` e `backend-data-gate-full`
- CI existente reutilizado em `.github/workflows/ci.yml` como validação hospedada de `lint`, `unit`, `integration` e `dbt-validate`

Ainda fora de escopo:
- deploy automation
- promotion entre ambientes
- observabilidade operacional ampla
- replay/backfill automatizado para release
