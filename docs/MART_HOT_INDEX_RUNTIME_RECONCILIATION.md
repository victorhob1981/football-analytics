# Mart Hot Index Runtime Reconciliation

## 1. Causa exata da ausencia dos indices

Problema confirmado no runtime:

- ausente no banco vivo:
  - `mart.fact_fixture_player_stats (match_id, team_id)`
  - `mart.player_match_summary (player_id, match_date desc, match_id desc)`
- presente no banco vivo:
  - `mart.fact_fixture_lineups (match_id, team_id)`

O repositorio/dbt hoje declara corretamente os dois indices ausentes:

- `dbt/models/marts/core/fact_fixture_player_stats.sql`
  - `materialized='incremental'`
  - `indexes=[{'columns': ['match_id', 'team_id'], 'type': 'btree'}]`
- `dbt/models/marts/analytics/player_match_summary.sql`
  - `materialized='table'`
  - `indexes=[{'columns': ['player_id', 'match_date desc', 'match_id desc'], 'type': 'btree'}]`

O manifest atual confirma esse estado declarativo:

- `fact_fixture_player_stats`
  - `materialized = incremental`
  - `indexes = [{'columns': ['match_id', 'team_id'], 'type': 'btree'}]`
- `player_match_summary`
  - `materialized = table`
  - `indexes = [{'columns': ['player_id', 'match_date desc', 'match_id desc'], 'type': 'btree'}]`

Prova do adapter/Postgres:

- o macro `postgres__get_create_index_sql` suporta o config `indexes`
- o materialization `table` sempre chama `create_indexes(target_relation)`
- o materialization `incremental` so chama `create_indexes(target_relation)` quando:
  - a tabela ainda nao existe
  - a relacao anterior e `view`
  - ha `full_refresh`

Prova temporal do drift:

- os arquivos com `indexes` foram alterados em `2026-04-01 12:05:17`
- o `manifest.json` foi regenerado em `2026-04-01 12:05:34`
- o `run_results.json` vigente era de `2026-04-01 11:44:43`
- esse `run_results` nao continha:
  - `fact_fixture_player_stats`
  - `player_match_summary`
- o `dbt.log` mostra execucao anterior dos modelos em `05:58`, antes da adicao do config `indexes`

Leitura objetiva da causa:

1. os modelos foram rematerializados antes de o config `indexes` existir no dbt;
2. depois disso houve apenas `dbt parse`, sem `dbt run` desses modelos;
3. `player_match_summary`, por ser `table`, ficou sem indice porque foi recriada antes da configuracao e nao foi rerodada depois;
4. `fact_fixture_player_stats`, por ser `incremental`, nao teria o indice recriado por um `dbt run` incremental normal mesmo depois da configuracao;
5. `fact_fixture_lineups` permaneceu com o indice vivo porque nao perdeu o indice anterior.

## 2. Por que isso aconteceu

O gap nao veio de configuracao errada atual do repositorio.

O gap veio da combinacao de dois fatores:

- rematerializacao anterior das tabelas antes de o `indexes` config ser introduzido;
- ausencia de um rerun posterior compativel com a semantica de criacao de indices do dbt.

Em especial:

- `player_match_summary` precisava de um rerun de `table` depois da mudanca declarativa;
- `fact_fixture_player_stats` exigiria `full_refresh` ou criacao manual do indice, porque incremental normal nao backfilla indice ausente em tabela ja existente.

## 3. Correcao escolhida

Correcao minima segura escolhida:

- reconciliar o runtime diretamente, sem tocar no repositorio declarativo, porque o repositorio ja estava correto

Acao executada:

- `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mart_fact_fixture_player_stats_match_team ON mart.fact_fixture_player_stats (match_id, team_id)`
- `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mart_player_match_summary_player_match_date ON mart.player_match_summary (player_id, match_date desc, match_id desc)`

Justificativa:

- menor intervencao segura para restaurar o runtime agora;
- nao cria mentira entre runtime e repositorio, porque o dbt ja declara esses indices;
- evita `full_refresh` desnecessario de `fact_fixture_player_stats`;
- resolve o gap final sem reabrir outras ondas.

## 4. Arquivos alterados

Nenhum arquivo de codigo ou migration precisou ser alterado nesta reconciliacao.

Arquivos documentais alterados:

- `docs/MART_HOT_INDEX_RUNTIME_RECONCILIATION.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`
- `docs/DB_TUNING_FINAL_CLOSURE.md`

Artefatos gerados:

- `artifacts/db_tuning/mart_hot_index_reconciled/baseline.json`
- `artifacts/db_tuning/mart_hot_index_reconciled/baseline.md`

## 5. Validacao before/after

### Catalogo do banco

Before:

- ausentes:
  - `idx_mart_fact_fixture_player_stats_match_team`
  - `idx_mart_player_match_summary_player_match_date`

After:

- presentes:
  - `idx_mart_fact_fixture_player_stats_match_team`
  - `idx_mart_player_match_summary_player_match_date`

### EXPLAIN (ANALYZE, BUFFERS)

| query | Onda 0 | Onda 1 after | estado regressivo | estado reconciliado |
| --- | ---: | ---: | ---: | ---: |
| `match_center_player_stats` | `234.439 ms` | `0.261 ms` | `1322.409 ms` | `0.391 ms` |
| `player_contexts` | `48.009 ms` | `0.646 ms` | `37.068 ms` | `1.431 ms` |

Planos after:

- `match_center_player_stats`
  - `Index Scan using idx_mart_fact_fixture_player_stats_match_team`
- `player_contexts`
  - `Bitmap Index Scan using idx_mart_player_match_summary_player_match_date`

### Endpoints

| endpoint | Onda 0 | Onda 1 after | estado regressivo | estado reconciliado |
| --- | ---: | ---: | ---: | ---: |
| `match_center` | `428.134 ms` | `48.324 ms` | `344.990 ms` | `62.755 ms` |
| `player_profile` | `347.434 ms` | `49.462 ms` | `228.556 ms` | `42.820 ms` |

Leitura:

- `match_center` saiu do estado regressivo e voltou para perto do patamar da Onda 1
- `player_profile` saiu do estado regressivo e ficou melhor que o after da Onda 1

## 6. Impacto nos endpoints

Impacto objetivo:

- `match_center`
  - melhoria de `81.81%` sobre o estado regressivo
- `player_profile`
  - melhoria de `81.27%` sobre o estado regressivo

## 7. Riscos remanescentes

- o runtime agora esta coerente com o repositorio
- permanece apenas uma observacao operacional:
  - se `fact_fixture_player_stats` perder esse indice novamente por algum evento externo, um `dbt run` incremental normal nao o recriara sozinho; sera necessario `full_refresh` ou nova reconciliacao manual

Nao ha gap real aberto nesta frente apos a correcao.

## 8. Conclusao objetiva sobre fechamento final do tuning

Resultado:

- a causa da ausencia dos indices foi isolada com evidencia objetiva
- o runtime foi reconciliado com o que o repositorio ja declarava
- `match_center` e `player_profile` sairam do estado regressivo

Conclusao:

- o gap final do tuning foi fechado
- o tuning do banco pode ser considerado encerrado
