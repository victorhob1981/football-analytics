# MART Final Coverage Audit

Data de referencia: `2026-03-20`  
Projeto: `football-analytics`

## 1) Etapa 1 - Diagnostico `standings_evolution.position`

Classificacao: `POSITION_GT_30_VALIDO`.

Evidencias diretas no banco:

- `min(position)=1`
- `max(position)=80`
- `rows(position > 30)=935`
- `rows(position < 1)=0`
- rounds com `max(position)` acima do numero de times distintos na mesma rodada: `0`

Escopos com `position > 30`:

- `champions_league` (`league_id=2`) temporadas `2020..2024`, max ate `66`
- `copa_do_brasil` (`league_id=654`) temporadas `2021..2025`, max `80`
- `libertadores` (`league_id=1122`) temporadas `2021..2025`, max `44`

Leitura tecnica:

- o limite antigo `position <= 30` estava rigido para o portfolio atual;
- nao houve evidencia de bug semantico no modelo para este ponto.

## 2) Etapa 2 - Correcao minima do gate

Arquivo ajustado:

- `quality/great_expectations/expectations/mart_standings_evolution_suite.json`

Diff aplicado:

- expectation `expect_column_values_to_be_between` da coluna `position`:
  - de `max_value: 30`
  - para `max_value: 80`

Justificativa:

- o dado real validado no banco chega ate `80` e permanece consistente com o grain por rodada/time.

## 3) Etapa 3 - Rerun dos quality gates

Comandos executados:

- `python /opt/airflow/quality/great_expectations/run_checkpoints.py --checkpoint gold_marts_checkpoint`
- `airflow dags test data_quality_checks 2026-03-20`

Resultado:

- `gold_marts_checkpoint`: `success=True`
- `data_quality_checks`: `success` (`rows_in=34`, `rows_out=34`)

Veredito para auditoria final:

- `GO`

## 4) Etapa 4 - Auditoria final `raw -> mart`

Metodo aplicado:

1. base de escopos via `raw.competition_seasons` (`competition_key/season_label`);
2. cobertura `raw` por dominio com join em `raw.fixtures`;
3. cobertura `mart` por fato via `match_id -> raw.fixtures.fixture_id`;
4. classificacao por escopo com comparacao contractual (mesmo criterio de chave minima dos modelos dbt):
   - `fact_match_events`: `event_id` e `fixture_id` nao nulos;
   - `fact_fixture_lineups`: `fixture_id`, `team_id`, `lineup_id`, `player_id` nao nulos;
   - `fact_fixture_player_stats`: `fixture_id`, `team_id`, `player_id` nao nulos.

Distribuicao final por escopo (50 escopos):

- `COMPLETO`: `32`
- `PROVIDER_COVERAGE_GAP`: `18`
- `PARCIAL`: `0`
- `PIPELINE_BUG / INCONSISTENCIA`: `0`
- `NAO_INGESTADO`: `0`
- `NAO_APLICAVEL`: `0`

Comparacao com baseline pre-rebuild:

- antes: `49 NAO_INGESTADO`, `1 PARCIAL`, `0 COMPLETO`
- agora: `0 NAO_INGESTADO`, `0 PARCIAL`, `32 COMPLETO`, `18 PROVIDER_COVERAGE_GAP`

Leitura objetiva:

- o vazio estrutural do `mart` foi eliminado;
- os gaps residuais estao concentrados em coverage de provider (nao em falha de materializacao do `mart`).

### Caveats residuais (provider coverage)

Competicoes com gap residual por dominio:

- `brasileirao_a`: eventos `4`, lineups `1`, player_stats `1`
- `brasileirao_b`: eventos `0`, lineups `7`, player_stats `7`
- `bundesliga`: eventos `1`, lineups `1`, player_stats `1`
- `champions_league`: eventos `2`, lineups `23`, player_stats `2`
- `copa_do_brasil`: eventos `0`, lineups `36`, player_stats `36`
- `libertadores`: eventos `1`, lineups `23`, player_stats `1`

Observacao importante de grain:

- nos escopos que antes apareciam como `PARCIAL` por lineups (`brasileirao_a/2024`, `brasileirao_b/2024`, `champions_league/2024_25`), os fixtures faltantes em `mart.fact_fixture_lineups` tinham somente rows com `player_id` nulo em `raw.fixture_lineups` (sem rows validas para o contrato do fato).

## 5) Evidencias geradas

Artefatos desta auditoria:

- `artifacts/mart_final_audit_20260320/mart_raw_scope_matrix.csv`
- `artifacts/mart_final_audit_20260320/scope_status_summary.csv`

## 6) Veredito final do mart

`GO` para considerar a camada `mart` rematerializada e operacionalmente utilizavel para consumo.

Risco residual:

- cobertura parcial herdada de provider em 18 escopos; deve ser tratada como caveat de produto, nao como bloqueio de operacao da camada `mart`.
