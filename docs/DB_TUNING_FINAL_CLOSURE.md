# DB Tuning Final Closure

## 1. Resumo executivo final

Status consolidado final:

- validacao final executada
- governanca e `raw.match_events` seguem verdes
- bootstrap limpo segue verde
- a regressao final de runtime/API nas superficies quentes da Onda 1 foi corrigida

Conclusao executiva:

- o programa de tuning pode ser encerrado formalmente
- status correto no estado atual e:
  - `tuning fechado`

Motivo objetivo:

- o ultimo gap real era a ausencia, no runtime, de dois indices quentes do `mart`
- a reconciliacao runtime desses indices foi executada e validada

## 2. Baseline original

Baseline oficial de referencia: Onda 0

API:

| endpoint | baseline Onda 0 |
| --- | ---: |
| `matches_list` | `31.166 ms` |
| `match_center` | `428.134 ms` |
| `player_profile` | `347.434 ms` |
| `team_profile` | `675.524 ms` |
| `rankings_player_goals` | `117.266 ms` |

Queries quentes:

| query | baseline Onda 0 |
| --- | ---: |
| `match_center_player_stats` | `234.439 ms` |
| `player_contexts` | `48.009 ms` |

dbt:

| recorte/modelo | baseline |
| --- | ---: |
| recorte controlado Wave 2 | `273.74 s` |
| `dim_player` | `150.038 s` |
| `fact_fixture_player_stats` | `123.053 s` |

Ingestao:

| metrica | baseline Onda 3 |
| --- | ---: |
| DAG wall clock | `17604.745 ms` |
| step `load_fixture_player_statistics_silver_to_raw` | `10937 ms` |
| WAL | `26 MB` |

## 3. Mudancas executadas por onda

### Onda 0

- habilitacao de `pg_stat_statements`
- baseline reproduzivel de endpoints, SQL, dbt e inventario fisico
- reconciliacao inicial do drift de `raw.match_events`

### Onda 1

- indices quentes no `mart`
- pool de conexoes na API
- reducao forte de latencia e buffers nos endpoints centrais

### Onda 2

- materializacao persistida em `stg_fixture_player_statistics`
- materializacao persistida em `stg_player_season_statistics`
- reducao de recomputacao pesada em `dim_player` e `fact_fixture_player_stats`

### Onda 3

- consolidacao de classificacao + `upsert` no path generico `silver -> raw`
- remocao dos dois `COUNT(*)` previos do caminho quente
- reducao de wall clock e WAL no benchmark controlado

### Onda 4

- reconciliacao canonica de `raw.match_events`
- oficializacao declarativa do DDL canonico no repositorio
- saneamento do bootstrap global
- migracao fisica das seasons historicas para fora do `default`
- limpeza de duplicidade local de indices no `default`

### Rodada curta final

- isolamento da causa exata da ausencia dos indices quentes do `mart`
- reconciliacao runtime dos dois indices ausentes
- restauracao do desempenho de `match_center` e `player_profile`

## 4. Ganhos confirmados por area

### API

Validacao final reconciliada executada em `artifacts/db_tuning/mart_hot_index_reconciled/baseline.json`.

| endpoint | Onda 0 | after Onda 1 | estado final | leitura |
| --- | ---: | ---: | ---: | --- |
| `matches_list` | `31.166 ms` | `18.207 ms` | `20.365 ms` | ganho mantido vs baseline |
| `match_center` | `428.134 ms` | `48.324 ms` | `62.755 ms` | ganho restaurado; levemente acima da Onda 1 |
| `player_profile` | `347.434 ms` | `49.462 ms` | `42.820 ms` | ganho restaurado; melhor que Onda 1 |
| `team_profile` | `675.524 ms` | `52.960 ms` | `55.925 ms` | ganho mantido |
| `rankings_player_goals` | `117.266 ms` | `96.871 ms` | `74.931 ms` | ganho mantido e melhorado |

Confirmacoes estruturais das queries quentes:

| query | Onda 0 | after Onda 1 | estado final | leitura |
| --- | ---: | ---: | ---: | --- |
| `match_center_player_stats` | `234.439 ms` | `0.261 ms` | `0.391 ms` | ganho restaurado |
| `player_contexts` | `48.009 ms` | `0.646 ms` | `1.431 ms` | ganho restaurado |

Evidencia de catalogo no runtime atual:

- presentes no banco atual:
  - `idx_mart_fact_fixture_lineups_match_team`
  - `idx_mart_fact_fixture_player_stats_match_team`
  - `idx_mart_player_match_summary_player_match_date`

Leitura correta:

- o repositorio declarava corretamente os indices
- o gap veio de drift operacional de runtime
- a reconciliacao do runtime fechou o problema

### dbt

Ultimo estado verde validado continua sendo o da Onda 2:

| recorte/modelo | baseline | after Onda 2 | leitura |
| --- | ---: | ---: | --- |
| recorte controlado | `273.74 s` | `197.91 s` | ganho de `27.70%` |
| `dim_player` | `150.038 s` | `37.704 s` | ganho material |
| `fact_fixture_player_stats` | `123.053 s` | `46.690 s` | ganho material |

Nesta rodada curta final, nenhum rerun de dbt foi necessario porque o repositorio ja estava correto e o gap era apenas de runtime.

### Ingestao

Ultimo estado verde validado continua sendo o da Onda 3:

| metrica | baseline | after Onda 3 | leitura |
| --- | ---: | ---: | --- |
| wall clock | `17604.745 ms` | `15134.458 ms` | `14.03%` melhor |
| step Airflow | `10937 ms` | `8727 ms` | `20.21%` melhor |
| WAL | `26 MB` | `52 kB` | `99.81%` melhor |

Nesta rodada curta final, nenhum benchmark novo de ingestao foi necessario porque nao houve alteracao nessa frente.

### `raw.match_events`

Validacao final mantida verde:

| season | particao final | rows |
| --- | --- | ---: |
| 2020 | `raw.match_events_2020` | 37742 |
| 2021 | `raw.match_events_2021` | 54189 |
| 2022 | `raw.match_events_2022` | 51986 |
| 2023 | `raw.match_events_2023` | 57591 |
| 2024 | `raw.match_events_2024` | 61326 |
| 2025 | `raw.match_events_2025` | 21376 |

Confirmacoes:

- `raw.match_events_default = 0`
- `default` com seasons drenadas = `0`
- total final = `284210`
- PK distinta final = `284210`
- duplicidades locais removidas do `default = 0`

### Bootstrap / governanca

Validacao final mantida verde:

- `dbmate status`: `Applied: 26`, `Pending: 0`
- versoes de migration duplicadas no repo: `0`
- bootstrap limpo rerodado em banco temporario:
  - passou por todas as `26` migrations atuais

## 5. Before/after consolidado

Resumo executivo:

| area | before | melhor estado atingido | estado final | status |
| --- | --- | --- | --- | --- |
| API hot reads | baseline Onda 0 ruim | Onda 1 muito melhor | ganho restaurado | `OK` |
| dbt | baseline Onda 0/2 pesado | Onda 2 melhor | ultimo estado verde mantido | `OK` |
| ingestao | baseline Onda 3 mais caro | Onda 3 melhor | ultimo estado verde mantido | `OK` |
| `raw.match_events` | drift + `default` concentrando historico | Onda 4 concluida | mantido verde | `OK` |
| bootstrap/governanca | bloqueios historicos | saneado | mantido verde | `OK` |

## 6. Riscos remanescentes reais

- nao ha gap real aberto no tuning executado
- observacao operacional residual:
  - em `fact_fixture_player_stats`, um `dbt run` incremental normal nao backfilla indice ausente em tabela existente; se esse indice voltar a desaparecer por acao externa, sera necessario `full_refresh` ou reconciliacao manual

## 7. O que ficou explicitamente fora de escopo

- novo tuning estrutural
- novas migrations de correcao
- rerun amplo de dbt
- nova rodada de tuning de ingestao
- tuning de memoria, replica, arquitetura macro ou outras tabelas

## 8. Conclusao objetiva

Estado final correto:

- `tuning fechado`

O que esta verde:

- Onda 0
- Onda 1
- Onda 2 no ultimo estado validado
- Onda 3 no ultimo estado validado
- Onda 4 em `raw.match_events`
- bootstrap e governanca do repositorio

Conclusao final:

- o ultimo gap real foi fechado pela reconciliacao dos indices quentes do `mart`
- runtime e repositorio voltaram a ficar coerentes
