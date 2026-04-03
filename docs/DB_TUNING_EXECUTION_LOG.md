# DB Tuning Execution Log

Data de referencia: 2026-03-31  
Documento base: `docs/DB_TUNING_ANALYSIS_AND_PLAN.md`

## Status geral

- Onda 0: `CONCLUIDA`
- Onda 1: `CONCLUIDA`
- Onda 2: `CONCLUIDA`
- Onda 3: `CONCLUIDA`
- Onda 4: `BLOQUEADA`

## Onda 0 - Baseline e governanca

### Estado inicial

- `pg_stat_statements` indisponivel no runtime observado.
- baseline SQL e de endpoints ainda nao materializada em artefato reproduzivel.
- `dbt/target/run_results.json` e `dbt/logs/dbt.log` disponiveis para baseline de transformacao.
- ha suspeita forte de drift em `raw.match_events` entre schema vivo e migrations.

### Mudancas executadas

- `docker-compose.yml`: preload controlado de `pg_stat_statements` no servico `postgres`.
- `tools/db_tuning_wave0_capture.py`: captura reproduzivel de baseline SQL, endpoints, dbt, inventario fisico e reconciliacao de schema.
- `artifacts/db_tuning/wave0/wave0_baseline.json`: baseline estruturada.
- `artifacts/db_tuning/wave0/wave0_baseline.md`: baseline legivel para revisao.

### Evidencia before

- `shared_preload_libraries` vazio antes da instrumentacao.
- `pg_extension` sem `pg_stat_statements`.
- `EXPLAIN (ANALYZE, BUFFERS)` previo em `mart.fact_fixture_player_stats` por `match_id`: `177.952 ms` no baseline atual da Onda 0.
- `EXPLAIN (ANALYZE, BUFFERS)` previo em `mart.player_match_summary` por `player_id`: `48.009 ms` no baseline atual da Onda 0.

### Validacao after

- `shared_preload_libraries = pg_stat_statements`.
- extensao `pg_stat_statements 1.10` instalada e consultavel.
- endpoints criticos baselineados com `200 OK`:
  - `matches_list`: `31.166 ms` medio
  - `match_center`: `428.134 ms` medio
  - `player_profile`: `347.434 ms` medio
  - `team_profile`: `675.524 ms` medio
  - `rankings_player_goals`: `117.266 ms` medio
- top SQL capturado com chamadas, tempo e buffers:
  - maior query observada na baseline: `1118.569 ms` total / `372.856 ms` medio / `811473` buffers
  - query de player stats do match center: `424.602 ms` total / `141.534 ms` medio / `485100` buffers
- baseline dbt por modelo capturada:
  - wall clock total: `283.04 s`
  - `dim_player`: `146.554 s`
  - `fact_fixture_player_stats`: `112.2 s`
- inventario fisico atualizado:
  - banco `football_dw`: `5907 MB`
  - maiores tabelas: `raw.fixture_lineups` `1499 MB`, `raw.fixture_player_statistics` `1398 MB`, `mart.fact_fixture_player_stats` `1266 MB`
- drift reconciliado:
  - `raw.match_events`: `confirmed_drift`
  - colunas vivas nao referenciadas nas migrations de `raw.match_events`: `provider`, `provider_league_id`, `competition_key`, `season_label`, `provider_season_id`, `source_run_id`

### Arquivos alterados nesta onda

- `docker-compose.yml`
- `tools/db_tuning_wave0_capture.py`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- `pg_stat_statements` passou a existir, mas a baseline ainda e local e sem concorrencia externa controlada.
- Worktree do repositorio esta muito sujo fora do escopo; nenhuma dessas alteracoes deve ser misturada com arquivos ja modificados pelo usuario.
- O drift confirmado de `raw.match_events` precisa ser preservado como restricao na Onda 3 e Onda 4; nao e seguro assumir que migrations refletem integralmente o runtime.

### Decisao

- wave concluida, seguir

## Onda 1 - Estabilizacao das leituras quentes

### Estado inicial

- As leituras quentes confirmadas na Onda 0 continuam concentradas em `mart.fact_fixture_player_stats`, `mart.fact_fixture_lineups`, `mart.player_match_summary` e consultas com `fact_matches`.
- Baseline objetiva atual:
  - `match_center`: `428.134 ms` medio
  - `player_profile`: `347.434 ms` medio
  - `team_profile`: `675.524 ms` medio
  - `rankings_player_goals`: `117.266 ms` medio
- `pg_stat_statements` confirmou custo alto nas queries reais da API:
  - player stats do match center: `485100` buffers em 3 chamadas
  - lineups do match center: `345459` buffers em 3 chamadas
  - queries de team profile sobre `fact_matches` e lineups dominam o topo por tempo total

### Mudancas executadas

- `db/migrations/20260331214500_wave1_hot_read_indexes.sql`: formalizacao dos indices cirurgicos da wave.
- indice aplicado em runtime:
  - `mart.fact_fixture_player_stats (match_id, team_id)`
  - `mart.fact_fixture_lineups (match_id, team_id)`
  - `mart.player_match_summary (player_id, match_date desc, match_id desc)`
- `api/src/db/client.py`: substituicao de conexao por chamada por `ConnectionPool`.
- `api/src/core/config.py`: parametros minimos de pool via env.
- `api/requirements.txt`: dependencia `psycopg-pool==3.2.8`.
- `tools/db_tuning_wave0_capture.py`: suporte a label de saida para comparacao before/after sem sobrescrever baseline anterior.

### Validacao before/after

- `EXPLAIN (ANALYZE, BUFFERS)`:
  - match center player stats:
    - before: `Execution Time 234.439 ms`
    - after: `Execution Time 0.592 ms`
    - before: `Parallel Seq Scan`
    - after: `Index Scan using idx_mart_fact_fixture_player_stats_match_team`
    - before buffers: `485100` no top SQL da baseline
    - after buffers: `93` no top SQL da baseline after
  - match center lineups:
    - before: `Execution Time 223.379 ms`
    - after: `Execution Time 0.349 ms`
    - before: `Parallel Seq Scan`
    - after: `Index Scan using idx_mart_fact_fixture_lineups_match_team`
    - before buffers: `345459`
    - after buffers: `89`
  - player contexts:
    - before: `Execution Time 48.009 ms`
    - after: `Execution Time 1.799 ms`
    - before: `Parallel Seq Scan on player_match_summary`
    - after: `Bitmap Index Scan on idx_mart_player_match_summary_player_match_date`
- endpoints criticos:
  - `matches_list`: `31.166 ms -> 18.207 ms` (`41.58%` melhor)
  - `match_center`: `428.134 ms -> 48.324 ms` (`88.71%` melhor)
  - `player_profile`: `347.434 ms -> 49.462 ms` (`85.76%` melhor)
  - `team_profile`: `675.524 ms -> 52.960 ms` (`92.16%` melhor)
  - `rankings_player_goals`: `117.266 ms -> 96.871 ms` (`17.39%` melhor)
- `fact_matches` revisado e mantido sem novo indice nesta wave:
  - before medido: `Seq Scan` em `fact_matches` com `Execution Time 14.624 ms`
  - after observado com cache quente: `Execution Time 5.873 ms`
  - decisao: tabela ainda pequena (`15878` rows); nao ha evidencia suficiente para indexacao adicional nesta wave
- write path preservado:
  - nenhum indice novo em `raw`
  - confirmacao catalogal mantida apenas no `mart`
  - carga `silver -> raw` nao foi alterada nesta wave

### Arquivos alterados nesta onda

- `db/migrations/20260331214500_wave1_hot_read_indexes.sql`
- `api/requirements.txt`
- `api/src/core/config.py`
- `api/src/db/client.py`
- `tools/db_tuning_wave0_capture.py`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- `fact_matches` segue em `Seq Scan`; hoje o custo e aceitavel, mas precisa revalidacao se volume crescer materialmente.
- os indices do `mart` reduzem leitura de produto, mas aumentarao custo de rebuild dessas tabelas no dbt; esse trade-off entra formalmente na Onda 2.
- o pool de conexoes foi validado no runtime local e removeu warning de encerramento com `close()` explicito, mas ainda nao foi exercitado sob concorrencia multiworker.

### Decisao

- wave concluida, seguir

## Onda 2 - Reducao de custo de transformacao dbt

### Estado inicial

- baseline dbt da Onda 0:
  - wall clock: `283.04 s`
  - `dim_player`: `146.554 s`
  - `fact_fixture_player_stats`: `112.2 s`
- `dbt_project.yml` ainda materializa `staging` e `intermediate` como `view`.
- a cadeia critica permanece:
  - `stg_fixture_player_statistics`
  - `int_fixture_player_context`
  - `fact_fixture_player_stats`
  - `dim_player`

### Mudancas executadas

- `dbt/models/staging/stg_fixture_player_statistics.sql`: override de materializacao para `table`.
- `dbt/models/staging/stg_player_season_statistics.sql`: override de materializacao para `table`.
- recorte controlado executado com `dbt run --threads 1 --full-refresh --select stg_fixture_player_statistics stg_player_season_statistics int_fixture_player_context fact_fixture_player_stats dim_player`.
- validacao direcionada executada com `dbt test --select dim_player fact_fixture_player_stats`.

### Validacao before/after

- recorte dbt controlado:
  - before: `273.74 s`
  - after: `197.91 s`
  - ganho de wall clock: `75.83 s` (`27.70%`)
- por modelo:
  - `stg_fixture_player_statistics`: `0.174 s -> 102.403 s`
  - `stg_player_season_statistics`: `0.093 s -> 10.744 s`
  - `dim_player`: `150.038 s -> 37.704 s`
  - `fact_fixture_player_stats`: `123.053 s -> 46.690 s`
  - `int_fixture_player_context`: `0.089 s -> 0.114 s`
- leitura executiva:
  - o parsing pesado saiu do consumo repetido nos incrementais caros e passou a ser pago uma vez nas superficies persistidas.
  - o maior ganho ficou em `dim_player` e `fact_fixture_player_stats`, exatamente onde a Onda 0 tinha concentrado custo.
- consistencia:
  - `dbt test --select dim_player fact_fixture_player_stats`
  - resultado: `PASS=19 WARN=0 ERROR=0 SKIP=0 TOTAL=19`
- custo fisico adicional:
  - `mart.stg_fixture_player_statistics`: `1170 MB`
  - `mart.stg_player_season_statistics`: `178 MB`
  - trade-off aceito nesta wave porque o ganho de runtime e material e a persistencia corta recomputacao pesada confirmada

### Arquivos alterados nesta onda

- `dbt/models/staging/stg_fixture_player_statistics.sql`
- `dbt/models/staging/stg_player_season_statistics.sql`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- houve aumento de storage no `mart`; isso precisa entrar na politica final da Onda 4.
- `stg_player_season_statistics.sql` ja tinha modificacoes do usuario fora desta wave; nesta execucao a alteracao feita foi apenas a linha de materializacao e ela foi aplicada sem reverter o restante.
- `int_fixture_player_context` continua `view`; por enquanto a evidencia mostrou ganho suficiente sem persisti-la, mas isso deve ser reavaliado se o chain voltar a crescer.

### Decisao

- wave concluida, seguir

## Onda 3 - Escrita, ingestao e manutencao

### Estado inicial

- a Onda 0 confirmou drift em `raw.match_events` e a Onda 1 preservou o `raw` sem novos indices.
- o alvo agora e exclusivamente ingestao/manutencao:
  - staging/temp tables
  - pre-count de inserts/updates
  - batch/chunking
  - lock/WAL/autovacuum
  - custo do caminho `silver -> raw`
- benchmark controlado escolhido no path generico:
  - DAG: `silver_to_postgres_fixture_player_statistics`
  - escopo: `league_id=648`, `season=2024`
  - rows do lote: `17229`
  - run reaplicado: `2026-03-28T172733Z`
- baseline objetiva do write path antes da mudanca:
  - wall clock do DAG test: `17604.745 ms`
  - duracao do step `load_fixture_player_statistics_silver_to_raw`: `10937 ms`
  - contadores operacionais: `inseridas=0`, `atualizadas=0`, `ignoradas=17229`
  - `pg_stat_statements` no alvo mostrou quatro custos dominantes:
    - staging insert: `576.426 ms`
    - `COUNT(*)` de inserts: `386.096 ms`
    - `COUNT(*)` de updates: `434.293 ms`
    - `upsert` final: `396.484 ms`
  - tempo SQL observado nessas statements: `1796.053 ms`
  - WAL do lote: `27409120` bytes (`26 MB`)
- leitura executiva do baseline:
  - o staging nao era o principal custo.
  - o desperdicio confirmado estava nas duas passagens extras de `COUNT(*)` e no `upsert` aplicado sobre todo o staging mesmo em rerun idempotente.

### Mudancas executadas

- `infra/airflow/dags/common/services/warehouse_service.py`:
  - helper `_stage_and_upsert_with_classified_counts` para classificar `inserted/updated/ignored` em uma unica passada sobre `staging + target`;
  - substituicao do pre-count duplo do loader generico por `WITH classified AS MATERIALIZED (...)` + `upsert`;
  - validacao defensiva para garantir `inserted + updated == changed_rows`.
- nenhuma alteracao em:
  - `chunksize` de `to_sql`
  - loaders customizados de `fixtures`, `match_statistics` ou `match_events`
  - indices ou config do Postgres
- motivo do recorte:
  - a evidencia confirmou o problema no path generico;
  - o staging insert nao era dominante no baseline;
  - mexer nos loaders customizados nesta onda ampliaria escopo sem medicao especifica adicional.

### Validacao before/after

- benchmark controlado do mesmo lote em `silver_to_postgres_fixture_player_statistics`:
  - wall clock total: `17604.745 ms -> 15134.458 ms` (`14.03%` melhor)
  - duracao do step Airflow: `10937 ms -> 8727 ms` (`20.21%` melhor)
  - contadores operacionais preservados:
    - before: `inseridas=0`, `atualizadas=0`, `ignoradas=17229`
    - after: `inseridas=0`, `atualizadas=0`, `ignoradas=17229`
- `pg_stat_statements` no alvo:
  - before:
    - staging insert: `576.426 ms`
    - `COUNT(*)` inserts: `386.096 ms`
    - `COUNT(*)` updates: `434.293 ms`
    - `upsert`: `396.484 ms`
  - after:
    - staging insert: `541.369 ms`
    - `classified + upsert` em CTE unica: `399.875 ms`
    - `CREATE TEMP TABLE`: `1.048 ms`
  - tempo SQL observado nessas statements: `1796.053 ms -> 942.292 ms` (`47.54%` melhor)
  - evidência estrutural:
    - os dois `COUNT(*)` sumiram do top SQL.
    - o write path passou a fazer classificacao e `upsert` em uma unica statement pesada.
- WAL do lote:
  - before: `27409120` bytes (`26 MB`)
  - after: `53056` bytes (`52 kB`)
  - reducao: `99.81%`
- validacao cruzada do mesmo path generico em outro dataset:
  - DAG: `silver_to_postgres_player_season_statistics`
  - escopo: `league_id=654`, `season=2025`
  - resultado: `SUCCESS`
  - duracao do step: `1255 ms`
  - contadores: `inseridas=0`, `atualizadas=0`, `ignoradas=2393`
- lock/autovacuum:
  - nenhuma espera de lock foi observada na validacao local single-session.
  - `pg_stat_user_tables` segue sem contadores maduros (`n_live_tup`, `n_tup_upd`, `autovacuum_count` em zero), entao esta wave nao produz conclusao executiva confiavel sobre autovacuum/bloat.

### Arquivos alterados nesta onda

- `infra/airflow/dags/common/services/warehouse_service.py`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- os loaders customizados de `fixtures`, `match_statistics` e `match_events` ainda carregam o padrao antigo de pre-count; eles ficaram fora da mudanca para nao ampliar escopo sem benchmark dedicado.
- nao houve mudanca em `chunksize`; o benchmark mostrou que o staging insert nao era o custo dominante deste lote.
- metricas catalogais de autovacuum/manutencao continuam insuficientes no runtime atual para afirmar ganho ou ausencia de bloat.

### Decisao

- wave concluida, seguir

## Onda 4 - Estrutura e escala de longo prazo

### Estado inicial

- o primeiro item obrigatorio desta wave e `raw.match_events`.
- estado vivo observado:
  - `raw.match_events_default`: `220151` rows (`79.01%` do total), `115 MB`
  - `raw.match_events_2024`: `58484` rows (`20.99%` do total), `34 MB`
  - seasons hoje presas no `default`: `2020`, `2021`, `2022`, `2023`, `2025`
- drift estrutural ainda confirmado:
  - schema vivo com `25` colunas, incluindo `provider`, `provider_league_id`, `competition_key`, `season_label`, `provider_season_id`, `provider_event_id`, `source_run_id`
  - PK viva: `(provider, season, fixture_id, event_id)`
  - migrations base ainda registram `PRIMARY KEY (event_id, season)` para `raw.match_events`
- complexidade fisica adicional:
  - o `default` carrega pares duplicados de indices em `fixture_id`, `player_id` e `team_id`
  - o parent e os filhos nao estao reconciliados com o estado declarativo do repositorio

### Escopo exato que seria executado

- comecar pela estrategia de particionamento de `raw.match_events`, porque ela e a dependencia estrutural mais forte desta wave.
- so seguir para superficies legadas/politica final se o reparticionamento fosse executavel sem ampliar escopo.

### Execucao

- conciliacao do estado vivo de `raw.match_events`:
  - distribuicao real por particao e por season
  - constraints vivas
  - indices vivos do parent e dos filhos
  - colunas vivas vs migrations
- nenhuma mudanca de DDL foi executada nesta wave.

### Validacao before/after

- before:
  - `79.01%` das rows de `raw.match_events` seguem no `default`
  - so existe particao dedicada para `2024`
  - o estado declarativo do repo nao representa corretamente a chave/colunas vivas da tabela
- after:
  - sem `after` estrutural, porque a wave encontrou blocker real antes de qualquer alteracao segura

### Arquivos alterados nesta onda

- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- permanecem ativos todos os riscos estruturais de `raw.match_events`:
  - pruning incompleto
  - drift entre schema vivo e migrations
  - indices duplicados em particao default
  - necessidade de mover dados historicos para sair do `default`

### Decisao

- blocker encontrado, parar

### Blocker real

- ponto exato:
  - tentativa de iniciar a estrategia de particionamento de `raw.match_events`.
- por que e blocker real:
  - `raw.match_events_default` concentra `220151` rows (`79.01%` do total) em cinco seasons que deveriam ser candidatas a particoes dedicadas.
  - a tabela viva nao bate com as migrations do repositorio nem em chave primaria nem em colunas.
  - qualquer reparticionamento seguro agora exigiria:
    - reconciliar schema vivo vs migrations;
    - definir DDL canonico do parent;
    - criar estrategia de movimentacao de rows para fora do `default`;
    - revisar duplicidade de indices nos filhos.
  - isso deixou de ser tuning cirurgico e virou programa de migracao estrutural com risco de regressao e rollback proprio.
- impacto no plano:
  - a Onda 4 nao pode prosseguir com seguranca nesta rodada.
  - racionalizacao de superficies legadas e politica final de vacuum/indices ficam dependentes de fechar primeiro o contrato estrutural de `raw.match_events`.
- o que ficou concluido antes dele:
  - Onda 0 completa
  - Onda 1 completa
  - Onda 2 completa
  - Onda 3 completa
- menor proximo passo seguro:
  - abrir um bloco dedicado de reconciliacao de schema para `raw.match_events`, gerar o DDL canonico vivo no repositorio e so depois planejar a migracao de reparticionamento por season com janela de manutencao e rollback explicito.

## Rodada complementar - Reconciliacao estrutural de `raw.match_events`

### Estado inicial

- a Onda 4 havia parado corretamente antes de qualquer reparticionamento porque o contrato estrutural de `raw.match_events` nao estava canonizado no repositorio.
- restricoes desta rodada:
  - nenhum reparticionamento
  - nenhuma movimentacao de rows
  - nenhuma alteracao de PK
  - nenhuma alteracao de indices
  - nenhuma migracao estrutural
- objetivo unico:
  - reconciliar o DDL canônico vivo de `raw.match_events` e preparar o plano tecnico da futura migracao estrutural.

### Mudancas executadas

- criacao do documento tecnico dedicado:
  - `docs/MATCH_EVENTS_SCHEMA_RECONCILIATION_AND_MIGRATION_PLAN.md`
- consolidacao documental do estado vivo de `raw.match_events` com base em catalogo real:
  - colunas, tipos, constraints, PK, FK, particionamento, particoes, distribuicao de rows e indices
- comparativo explicito entre:
  - schema vivo
  - migrations do repositorio
  - contrato assumido pelo `warehouse_service.py`
  - estado canônico recomendado
- definicao do plano seguro da futura migracao estrutural:
  - pre-requisitos
  - ordem de execucao
  - drenagem do `default` por season
  - estrategia de PK/constraints/indices
  - rollback conceitual
  - validacoes before/after

### Evidencia objetiva

- schema vivo confirmado via catalogo e `pg_dump -s`:
  - tabela pai viva com `25` colunas
  - PK viva: `(provider, season, fixture_id, event_id)`
  - FK viva: `fixture_id -> raw.fixtures(fixture_id)`
  - particionamento vivo: `LIST (season)`
  - particoes vivas: `raw.match_events_2024` e `raw.match_events_default`
- distribuicao real confirmada:
  - total: `278635` rows
  - `raw.match_events_default`: `220151` rows (`79.01%`)
  - `raw.match_events_2024`: `58484` rows (`20.99%`)
  - seasons no `default`: `2020`, `2021`, `2022`, `2023`, `2025`
- divergencia confirmada entre runtime e repositorio:
  - migrations ainda registram PK `(event_id, season)`
  - runtime usa e satisfaz PK `(provider, season, fixture_id, event_id)`
  - colunas semanticas vivas usadas pelo loader e ausentes nas migrations:
    - `provider`
    - `provider_league_id`
    - `competition_key`
    - `season_label`
    - `provider_season_id`
    - `source_run_id`
- colunas vivas frias confirmadas:
  - `provider_event_id`: `0` rows nao nulas
  - `ingested_at`: `0` rows nao nulas
- indices vivos confirmados:
  - familia canônica do parent: PK + `assist_id`, `competition_key/season_label`, `fixture_id`, `fixture_id/type`, `player_id`, `team_id`
  - duplicidade local exata no `default`:
    - `fixture_id`
    - `player_id`
    - `team_id`

### Validacao final desta rodada

- o estado canônico recomendado de `raw.match_events` ficou amarrado no repositorio sem executar migracao.
- a divergencia entre estado vivo, migrations e codigo ficou explicitada com evidencia objetiva.
- o blocker original da Onda 4 foi refinado:
  - o blocker operacional de migracao estrutural permanece;
  - o blocker de definicao canônica foi resolvido nesta rodada.
- a futura migracao estrutural agora ja tem:
  - objetivo
  - pre-requisitos
  - ordem segura
  - estrategia de drenagem do `default`
  - estrategia para PK, constraints e indices
  - rollback conceitual
  - checklist de validacao

### Arquivos alterados nesta rodada complementar

- `docs/MATCH_EVENTS_SCHEMA_RECONCILIATION_AND_MIGRATION_PLAN.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- `raw.match_events_default` continua concentrando `79.01%` das rows; nenhum risco operacional foi removido ainda no plano fisico.
- as migrations continuam desatualizadas em relacao ao runtime ate que uma rodada futura oficialize o DDL canônico no repositorio de forma estrutural.
- os tres indices duplicados do `default` continuam existindo e permanecem fora de escopo nesta rodada.
- `provider_event_id` e `ingested_at` seguem como colunas vivas frias; qualquer limpeza delas continua fora do escopo da migracao estrutural futura proposta.

### Decisao

- rodada de reconciliacao concluida
- migracao estrutural futura pronta para abertura dedicada
- Onda 4 continua sem execucao fisica estrutural nesta rodada por decisao deliberada de baixo risco

## Rodada complementar - Oficializacao do DDL canônico de `raw.match_events`

### Estado inicial

- apos a reconciliacao estrutural, o contrato canônico de `raw.match_events` ja estava definido em documento, mas o repositório ainda nao o representava formalmente nas migrations.
- problema central desta rodada:
  - garantir que o repo descreva corretamente `raw.match_events` sem misturar isso com reparticionamento fisico.
- restricoes preservadas:
  - nenhum reparticionamento
  - nenhuma movimentacao de rows
  - nenhuma drenagem do `default`
  - nenhuma alteracao de dados
  - nenhuma revisao/remocao de indices duplicados

### Estrategia escolhida

- abordagem combinada e controlada:
  1. editar as migrations historicas que definem/endurecem `raw.match_events` para corrigir bootstrap limpo;
  2. adicionar uma migration forward-only apenas assertiva para registrar e exigir o contrato canônico sem alterar fisicamente a tabela.
- justificativa:
  - editar so historico melhoraria o bootstrap, mas deixaria o marco de oficializacao implicito;
  - criar so migration forward-only manteria bootstrap limpo nascendo de um DDL historico errado;
  - a combinacao preserva reprodutibilidade, seguranca operacional e integridade do historico de decisao.

### Mudancas executadas

- `db/migrations/20260217120000_baseline_schema.sql`
  - `raw.match_events` oficializada com as `25` colunas canônicas;
  - PK canônica oficializada: `(provider, season, fixture_id, event_id)`;
  - particao `raw.match_events_default` adicionada ao estado declarativo minimo.
- `db/migrations/20260219154500_raw_constraints_indexes_hardening.sql`
  - hardening alinhado ao contrato canônico;
  - PK fallback ajustada para o grao canônico;
  - indice canônico `idx_raw_match_events_competition_season_label` oficializado.
- `db/migrations/20260401101500_match_events_canonical_assertion.sql`
  - migration nova, forward-only e nao destrutiva;
  - valida colunas, PK, FK, particionamento, particoes minimas e familia canônica de indices.
- `docs/MATCH_EVENTS_CANONICAL_DDL_OFFICIALIZATION.md`
  - documento da decisao de estrategia e do estado oficializado.

### Evidencia objetiva

- estado vivo reconciliado anteriormente e agora refletido nas migrations:
  - `25` colunas
  - PK viva/canônica: `(provider, season, fixture_id, event_id)`
  - FK viva/canônica: `fixture_id -> raw.fixtures(fixture_id)`
  - particionamento: `LIST (season)`
  - particoes minimas reconhecidas: `raw.match_events_2024` e `raw.match_events_default`
  - familia canônica de indices: `assist_id`, `competition_key/season_label`, `fixture_id`, `fixture_id/type`, `player_id`, `team_id`
- contrato do codigo preservado:
  - `warehouse_service.py` continua usando `ON CONFLICT (provider, season, fixture_id, event_id)`
  - `EVENTS_TARGET_COLUMNS` continua exigindo as colunas semanticas agora oficializadas no repo

### Validacao desta rodada

- validacao estatica concluida:
  - o estado declarado das migrations agora bate com o canônico definido na reconciliacao;
  - a camada declarativa ficou separada da migracao fisica futura;
  - a migration assertiva explicita o que e exigido do runtime sem mover rows nem alterar dados.
- validacao executavel de bootstrap limpo:
  - bloqueada por ambiente desta thread
  - `docker version` retornou cliente disponivel, mas daemon indisponivel:
    - `failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine`
  - `psql` indisponivel no host:
    - `The term 'psql' is not recognized`
- leitura correta:
  - o repo ficou estruturalmente apto a reproduzir o canônico de `raw.match_events`;
  - a prova executavel de `dbmate up` em banco limpo permanece pendente por blocker de ambiente, nao por incoerencia ja encontrada nas migrations.

### Arquivos alterados nesta rodada complementar

- `db/migrations/20260217120000_baseline_schema.sql`
- `db/migrations/20260219154500_raw_constraints_indexes_hardening.sql`
- `db/migrations/20260401101500_match_events_canonical_assertion.sql`
- `docs/MATCH_EVENTS_CANONICAL_DDL_OFFICIALIZATION.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- a validacao executavel de bootstrap limpo continua pendente por indisponibilidade de Docker daemon/psql nesta sessao.
- a migration assertiva nao corrige ambientes divergentes; ela apenas impede que a divergencia siga silenciosa.
- nenhum risco operacional do `default` foi removido ainda; a rodada nao tocou no plano fisico.

### Decisao

- oficializacao declarativa concluida
- migracao fisica estrutural continua separada e adiada
- bootstrap limpo ficou coerente no plano declarativo; prova executavel ficou pendente por blocker de ambiente

## Retomada da validacao executavel - bootstrap limpo com Docker disponivel

### Estado inicial

- a rodada anterior tinha fechado a oficializacao declarativa de `raw.match_events`, mas a prova executavel de bootstrap limpo tinha ficado pendente por indisponibilidade de Docker/psql.

### Mudancas executadas

- criacao de banco temporario de validacao:
  - `football_dw_match_events_bootstrap_test`
- execucao de bootstrap limpo via `dbmate up` contra esse banco temporario.
- execucao manual da migration assertiva canônica de `raw.match_events` no mesmo banco de teste, apos o ponto em que o bootstrap interrompeu.
- atualizacao dos documentos para refletir o resultado real da validacao executavel.

### Evidencia objetiva

- `dbmate up` em banco limpo:
  - aplicou com sucesso as migrations ate `20260330120000_supercopa_do_brasil_seed.sql`
  - falhou depois em `20260330121000_supercopa_do_brasil_seed_repair.sql`
  - erro objetivo:
    - `pq: relation "control.competitions" does not exist`
- `raw.match_events` no banco de teste ficou com:
  - `25` colunas
  - PK `pk_match_events PRIMARY KEY (provider, season, fixture_id, event_id)`
  - FK `fk_match_events_fixture`
  - particoes:
    - `match_events_2024` -> `FOR VALUES IN (2024)`
    - `match_events_default` -> `DEFAULT`
  - familia canônica de indices presente
- a migration `20260401101500_match_events_canonical_assertion.sql` foi executada manualmente no banco de teste com sucesso:
  - `DO`
  - `SELECT 1`

### Validacao final desta retomada

- validacao executavel de `raw.match_events`: concluida e verde
- validacao executavel do bootstrap completo do repositório: bloqueada por migration externa ao escopo de `raw.match_events`
- refinamento correto da conclusao:
  - o repo agora reproduz o canônico de `raw.match_events` em bootstrap limpo
  - o repo ainda nao fecha bootstrap limpo completo por blocker em migration de seed do schema `control`

### Arquivos alterados nesta retomada

- `docs/MATCH_EVENTS_CANONICAL_DDL_OFFICIALIZATION.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- o blocker de bootstrap completo do repo continua aberto em `20260330121000_supercopa_do_brasil_seed_repair.sql`
- isso nao afeta o contrato canônico de `raw.match_events`, mas impede declarar o replay completo do repositório como 100% verde

### Decisao

- validacao pendente por Docker: concluida
- `raw.match_events` canônica em bootstrap limpo: confirmada
- blocker remanescente: externo ao escopo desta tabela

## Rodada complementar - Destravamento do bootstrap global em `control`

### Estado inicial

- `raw.match_events` ja estava encerrada e fora de escopo.
- o objetivo desta rodada era destravar o replay global das migrations no ponto em que a stack ainda parava ao entrar nos seeds da Supercopa.
- diagnostico refinado em replay limpo:
  - o primeiro blocker real nao era `20260330121000_supercopa_do_brasil_seed_repair.sql`;
  - o primeiro blocker real era `20260330120000_supercopa_do_brasil_seed.sql`;
  - evidencia: no banco de falha, `schema_migrations` parou em `20260323112000` e nao havia nenhuma tabela no schema `control`.

### Causa raiz

- as migrations do repositório nao tinham a foundation declarativa do catalogo `control` antes dos seeds:
  - `control.competitions`
  - `control.competition_provider_map`
  - `control.season_catalog`
- os seeds da Supercopa assumiam essas tabelas como preexistentes.
- resultado:
  - o replay limpo chegava ao primeiro seed de competicao sem o schema/tabelas base necessarios.

### Mudancas executadas

- criacao da migration historica minima:
  - `db/migrations/20260329190000_control_competition_catalog_foundation.sql`
- conteudo da correcao:
  - `CREATE SCHEMA IF NOT EXISTS control`
  - criacao idempotente de:
    - `control.competitions`
    - `control.competition_provider_map`
    - `control.season_catalog`
  - PKs, FKs, uniques e indices minimos exigidos pelos seeds:
    - `competitions_pkey`
    - `pk_control_competition_provider_map`
    - `uq_control_competition_provider_map_provider_league`
    - `pk_control_season_catalog`
    - `idx_control_competitions_priority`
    - `idx_control_competitions_type`
    - `idx_control_season_catalog_provider_season_id`

### Validacao executavel

- bootstrap limpo rerodado em banco temporario:
  - `football_dw_bootstrap_control_fix`
- resultado:
  - `20260329190000_control_competition_catalog_foundation.sql`: `PASS`
  - `20260330120000_supercopa_do_brasil_seed.sql`: `PASS`
  - `20260330121000_supercopa_do_brasil_seed_repair.sql`: `PASS`
- evidencia de dados:
  - `control.competition_provider_map` contem `supercopa_do_brasil / sportmonks / 1798`
  - `control.season_catalog` contem `2025`, `2026`, `2027` para `supercopa_do_brasil`

### Novo blocker real

- o replay global agora parou mais adiante em:
  - `db/migrations/20260331214500_wave1_hot_read_indexes.sql`
- erro objetivo:
  - `pq: relation "mart.fact_fixture_player_stats" does not exist`
- causa objetiva do novo blocker:
  - essa migration tenta criar indices em tabelas `mart` que nao existem em bootstrap limpo de schema;
  - elas nascem via dbt/materializacao posterior, nao via migrations de schema base.

### Arquivos alterados nesta rodada complementar

- `db/migrations/20260329190000_control_competition_catalog_foundation.sql`
- `docs/DB_TUNING_EXECUTION_LOG.md`
- `docs/MATCH_EVENTS_CANONICAL_DDL_OFFICIALIZATION.md`

### Riscos remanescentes

- o blocker de `control` foi removido.
- o bootstrap global do repo ainda nao fecha por causa da migration de indices da Onda 1 sobre tabelas `mart` nao bootstrapadas.
- `raw.match_events` nao foi reaberta nem alterada nesta rodada.

### Decisao

- blocker de `control.competitions`: resolvido
- replay global: avancou com sucesso alem da Supercopa
- novo ponto de parada: `20260331214500_wave1_hot_read_indexes.sql`

## Rodada complementar - Correcao arquitetural dos indices do `mart`

### Estado inicial

- o novo blocker real do bootstrap global era:
  - `db/migrations/20260331214500_wave1_hot_read_indexes.sql`
- erro objetivo:
  - `pq: relation "mart.fact_fixture_player_stats" does not exist`
- leitura correta do problema:
  - a migration tentava criar indices em relacoes `mart` que nao existem no schema base;
  - essas relacoes sao materializadas depois via dbt.

### Causa raiz

- a migration de indices da Onda 1 foi posicionada no lugar errado da arquitetura.
- indices de tabelas derivadas do dbt nao devem viver no bootstrap SQL de foundation, porque:
  - quebram replay limpo antes do `dbt run`;
  - nao acompanham corretamente `full_refresh`;
  - deixam o lifecycle dos indices desacoplado do lifecycle das relacoes materializadas.

### Estrategia escolhida

- neutralizar a migration historica de indices do `mart`;
- mover os indices para configuracao nativa `indexes` dos modelos dbt correspondentes.

Justificativa objetiva:

- no adapter Postgres usado pelo projeto, as materializacoes `table` e `incremental` chamam `create_indexes(target_relation)` no ciclo do dbt;
- isso coloca os indices exatamente no momento correto de criacao/recriacao das tabelas derivadas;
- `IF EXISTS` seria mascara de erro, nao correcao arquitetural.

### Mudancas executadas

- `db/migrations/20260331214500_wave1_hot_read_indexes.sql`
  - convertido em no-op explicito.
- `dbt/models/marts/core/fact_fixture_player_stats.sql`
  - indice dbt em `(match_id, team_id)`.
- `dbt/models/marts/core/fact_fixture_lineups.sql`
  - indice dbt em `(match_id, team_id)`.
- `dbt/models/marts/analytics/player_match_summary.sql`
  - indice dbt em `(player_id, match_date desc, match_id desc)`.
- `docs/MART_INDEX_STRATEGY_BOOTSTRAP_FIX.md`
  - decisao arquitetural e impacto registrados.

### Validacao executavel

- `dbt parse`: `PASS`
- validacao do manifest dentro do container dbt:
  - `fact_fixture_player_stats` exposto com `config.indexes`
  - `fact_fixture_lineups` exposto com `config.indexes`
  - `player_match_summary` exposto com `config.indexes`
- bootstrap limpo rerodado em banco temporario:
  - `football_dw_bootstrap_mart_fix`
- resultado:
  - `20260331214500_wave1_hot_read_indexes.sql`: `PASS`
  - replay global passou com sucesso desse ponto

### Novo blocker real

- o bootstrap global agora parou mais adiante em outro ponto:
  - colisao de versao no `dbmate`
- evidencia objetiva:
  - existem duas migrations com o mesmo prefixo de versao:
    - `20260331214500_coaches_identity.sql`
    - `20260331214500_wave1_hot_read_indexes.sql`
  - erro observado:
    - `pq: duplicate key value violates unique constraint "schema_migrations_pkey"`

### Arquivos alterados nesta rodada complementar

- `db/migrations/20260331214500_wave1_hot_read_indexes.sql`
- `dbt/models/marts/core/fact_fixture_player_stats.sql`
- `dbt/models/marts/core/fact_fixture_lineups.sql`
- `dbt/models/marts/analytics/player_match_summary.sql`
- `docs/MART_INDEX_STRATEGY_BOOTSTRAP_FIX.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- o blocker do `mart` foi removido.
- o bootstrap global ainda nao esta totalmente verde por causa da colisao de versao `20260331214500`.
- `raw.match_events` nao foi tocada nesta rodada.

### Decisao

- blocker arquitetural dos indices do `mart`: resolvido
- bootstrap limpo: avancou alem dele
- novo ponto de parada: duplicidade de versao `20260331214500` no historico do `dbmate`

## Rodada complementar - Correcao da colisao de versao `20260331214500`

### Estado inicial

- o primeiro blocker real do bootstrap global apos a correcao do `mart` era:
  - `duplicate key value violates unique constraint "schema_migrations_pkey"`
- causa imediata:
  - duas migrations com a mesma versao `20260331214500`:
    - `20260331214500_coaches_identity.sql`
    - `20260331214500_wave1_hot_read_indexes.sql`

### Diagnostico objetivo

- ambiente atual:
  - `schema_migrations` contem `20260331214500` apenas uma vez
  - `raw.coaches` existe
  - `raw.idx_coaches_name` existe
  - a nova versao `20260401103000` ainda nao existia
- leitura mais forte:
  - `coaches_identity` foi a migration que historicamente ocupou a versao `20260331214500`
  - `wave1_hot_read_indexes` ficou sem identidade propria confiavel no historico
- motivo:
  - ordem observada no replay limpo: `coaches_identity` entra antes da `wave1`
  - efeitos reais de `coaches_identity` estao presentes no ambiente atual

### Estrategia escolhida

- preservar `20260331214500_coaches_identity.sql` intacta
- mover `wave1_hot_read_indexes` para versao propria e unica:
  - `20260401103000_wave1_hot_read_indexes.sql`

Justificativa:

- evita reatribuir a versao historica de uma migration que deixou efeito real no banco;
- elimina a colisao no repositorio;
- mantem compatibilidade com ambientes existentes, onde a nova migration entra apenas como `no-op`.

### Mudancas executadas

- rename controlado:
  - `db/migrations/20260331214500_wave1_hot_read_indexes.sql`
  - para `db/migrations/20260401103000_wave1_hot_read_indexes.sql`
- conteudo mantido como `no-op`, conforme decisao arquitetural anterior.

### Validacao executavel

- checagem de duplicidade no diretório `db/migrations`:
  - nenhuma versao duplicada restante
- bootstrap limpo rerodado em banco temporario:
  - `football_dw_bootstrap_version_fix`
- resultado:
  - replay passou por todas as migrations restantes
  - conclusao observada:
    - `Writing: ./db/schema.sql`
- leitura correta:
  - a colisao de versao foi resolvida
  - o bootstrap global do repositório ficou verde nesta trilha de validacao

### Arquivos alterados nesta rodada complementar

- `db/migrations/20260401103000_wave1_hot_read_indexes.sql`
- `docs/MIGRATION_VERSION_COLLISION_FIX.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Riscos remanescentes

- ambientes existentes passarao a ver a nova versao `20260401103000` como pendente, mas ela e `no-op`.
- a versao antiga `20260331214500` permanece corretamente associada a `coaches_identity` no historico do repo.

### Decisao

- colisao de versao: resolvida
- bootstrap global: concluido com sucesso nesta rodada

## Rodada complementar - Tentativa de execucao fisica da Onda 4 em `raw.match_events`

### Estado inicial

- escopo mantido exclusivamente em `raw.match_events`
- objetivo operacional:
  - criar particoes explicitas para seasons historicas ainda presas em `raw.match_events_default`
  - drenar o `default` por season
  - preservar PK, FK, contagem total e coerencia do catalogo
- estado vivo observado em `2026-04-01`:
  - particoes atuais:
    - `raw.match_events_2024`
    - `raw.match_events_default`
  - total de rows: `284210`
  - PK distinta: `284210`
  - distribuicao por season:
    - `2020`: `37742` em `raw.match_events_default`
    - `2021`: `54189` em `raw.match_events_default`
    - `2022`: `51986` em `raw.match_events_default`
    - `2023`: `57591` em `raw.match_events_default`
    - `2024`: `61326` em `raw.match_events_2024`
    - `2025`: `21376` em `raw.match_events_default`

### Estrategia tentada

- seguir estritamente a ordem definida para a execucao fisica:
  1. criar particao explicita da season alvo
  2. inserir pela tabela pai para roteamento correto
  3. validar contagem da slice
  4. remover a mesma slice do `default`
  5. validar novamente
- primeira season alvo:
  - `2025`

### Evidencia objetiva

Teste nao destrutivo executado:

```sql
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE TABLE raw.match_events_2025_probe
PARTITION OF raw.match_events
FOR VALUES IN (2025);
ROLLBACK;
```

Resultado:

```text
ERROR: updated partition constraint for default partition "match_events_default" would be violated by some row
```

### Diagnostico objetivo

- blocker real confirmado no primeiro passo da sequencia obrigatoria;
- o PostgreSQL nao permite criar a particao explicita de uma season enquanto o `default` ainda contem rows dessa mesma season;
- como a estrategia exigida dependia exatamente dessa ordem, a execucao fisica nao pode prosseguir com seguranca.

### Impacto

- nenhuma alteracao fisica foi aplicada;
- nenhuma row foi movida;
- nenhuma nova particao foi persistida;
- a Onda 4 permaneceu aberta no aspecto fisico/estrutural.

### Arquivos alterados nesta rodada complementar

- `docs/MATCH_EVENTS_PARTITION_MIGRATION_EXECUTION.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Decisao

- execucao fisica de `raw.match_events`: interrompida no primeiro blocker real
- Onda 4: nao concluida nesta rodada
- menor proximo passo seguro:
  - redesenhar a estrategia operacional da drenagem por season para um fluxo compativel com a semantica de particao `DEFAULT` do PostgreSQL

## Rodada complementar - Execucao fisica da Onda 4 com estrategia compativel com `DEFAULT`

### Estrategia escolhida

- estrategia `A`, por season:
  1. copiar a slice da season do `default` para staging fora da arvore de particao
  2. validar slice e PK local
  3. deletar a slice do `default`
  4. criar a particao explicita da season
  5. reinserir a slice pela tabela pai `raw.match_events`
  6. validar contagem, PK, FK e indices
- motivo:
  - menor complexidade operacional que `ATTACH PARTITION`
  - rollback natural por season, com uma migration por bloco
  - indices e constraints da particao final passam a nascer pelo pai

### Mudancas executadas

Migrations criadas e aplicadas:

- `db/migrations/20260401113000_match_events_partition_2025.sql`
- `db/migrations/20260401113100_match_events_partition_2020.sql`
- `db/migrations/20260401113200_match_events_partition_2022.sql`
- `db/migrations/20260401113300_match_events_partition_2021.sql`
- `db/migrations/20260401113400_match_events_partition_2023.sql`
- `db/migrations/20260401113500_match_events_default_index_cleanup.sql`

### Validacao before/after

Estado `before`:

- total: `284210`
- PK distinta: `284210`
- distribuicao:
  - `2020`: `37742` em `raw.match_events_default`
  - `2021`: `54189` em `raw.match_events_default`
  - `2022`: `51986` em `raw.match_events_default`
  - `2023`: `57591` em `raw.match_events_default`
  - `2024`: `61326` em `raw.match_events_2024`
  - `2025`: `21376` em `raw.match_events_default`
- indices no `default`: `10`

Execucao `dbmate up`:

- `20260401113000_match_events_partition_2025.sql`: `1.572 s`
- `20260401113100_match_events_partition_2020.sql`: `1.831 s`
- `20260401113200_match_events_partition_2022.sql`: `2.219 s`
- `20260401113300_match_events_partition_2021.sql`: `2.360 s`
- `20260401113400_match_events_partition_2023.sql`: `2.403 s`
- `20260401113500_match_events_default_index_cleanup.sql`: `0.026 s`

Estado `after`:

- total: `284210`
- PK distinta: `284210`
- `raw.match_events_default`: `0` rows totais
- `raw.match_events_default` com seasons alvo: `0` rows
- distribuicao final:
  - `2020`: `37742` em `raw.match_events_2020`
  - `2021`: `54189` em `raw.match_events_2021`
  - `2022`: `51986` em `raw.match_events_2022`
  - `2023`: `57591` em `raw.match_events_2023`
  - `2024`: `61326` em `raw.match_events_2024`
  - `2025`: `21376` em `raw.match_events_2025`
- FK orphans nas seasons drenadas:
  - `2020`: `0`
  - `2021`: `0`
  - `2022`: `0`
  - `2023`: `0`
  - `2025`: `0`
- indices por particao:
  - `match_events_2020`: `7`
  - `match_events_2021`: `7`
  - `match_events_2022`: `7`
  - `match_events_2023`: `7`
  - `match_events_2024`: `7`
  - `match_events_2025`: `7`
  - `match_events_default`: `7`
- `dbmate status`:
  - `Applied: 26`
  - `Pending: 0`

### Arquivos alterados nesta rodada complementar

- `db/migrations/20260401113000_match_events_partition_2025.sql`
- `db/migrations/20260401113100_match_events_partition_2020.sql`
- `db/migrations/20260401113200_match_events_partition_2022.sql`
- `db/migrations/20260401113300_match_events_partition_2021.sql`
- `db/migrations/20260401113400_match_events_partition_2023.sql`
- `db/migrations/20260401113500_match_events_default_index_cleanup.sql`
- `docs/MATCH_EVENTS_PARTITION_MIGRATION_EXECUTION.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Decisao

- execucao fisica de `raw.match_events`: concluida
- cleanup de indices duplicados do `default`: concluido
- Onda 4: concluida

## Rodada final - Encerramento e validacao consolidada

### Estado inicial

- objetivo da rodada:
  - validar o estado final consolidado apos Ondas 0-4
  - sem abrir novas frentes
  - sem novo tuning estrutural
  - sem novas migrations, salvo regressao real
- regra de parada:
  - interromper a rodada final no primeiro gap real confirmado

### Validacoes executadas

- captura final de runtime/API:
  - `python tools/db_tuning_wave0_capture.py final_closure`
- checagem de catalogo do `mart` para as superficies quentes:
  - `fact_fixture_player_stats`
  - `fact_fixture_lineups`
  - `player_match_summary`
- revalidacao de `raw.match_events`:
  - distribuicao final por particao
  - `default = 0`
  - PK distinta
  - ausencia das duplicidades locais removidas
- governanca:
  - `dbmate status`
  - checagem de unicidade de versions em `db/migrations`
  - bootstrap limpo completo em banco temporario

### Evidencia objetiva

Runtime/API no estado final:

- `matches_list`: `20.365 ms`
- `match_center`: `344.990 ms`
- `player_profile`: `228.556 ms`
- `team_profile`: `55.925 ms`
- `rankings_player_goals`: `74.931 ms`

Comparacao com o melhor estado validado da Onda 1:

- `matches_list`: `18.207 ms -> 20.365 ms`
- `match_center`: `48.324 ms -> 344.990 ms`
- `player_profile`: `49.462 ms -> 228.556 ms`
- `team_profile`: `52.960 ms -> 55.925 ms`
- `rankings_player_goals`: `96.871 ms -> 74.931 ms`

Explain das queries quentes:

- `match_center_player_stats`:
  - Onda 1 after: `0.261 ms`
  - estado final: `1322.409 ms`
  - plano atual: `Parallel Seq Scan` em `mart.fact_fixture_player_stats`
- `player_contexts`:
  - Onda 1 after: `0.646 ms`
  - estado final: `37.068 ms`
  - plano atual: `Parallel Seq Scan` em `mart.player_match_summary`

Catalogo do `mart` no runtime atual:

- presente:
  - `idx_mart_fact_fixture_lineups_match_team`
- ausentes:
  - indice quente de `fact_fixture_player_stats`
  - indice quente de `player_match_summary`

Repositorio atual:

- os indices ainda estao declarados em:
  - `dbt/models/marts/core/fact_fixture_player_stats.sql`
  - `dbt/models/marts/analytics/player_match_summary.sql`

`raw.match_events` no estado final:

- distribuicao final coerente:
  - `2020 -> raw.match_events_2020 = 37742`
  - `2021 -> raw.match_events_2021 = 54189`
  - `2022 -> raw.match_events_2022 = 51986`
  - `2023 -> raw.match_events_2023 = 57591`
  - `2024 -> raw.match_events_2024 = 61326`
  - `2025 -> raw.match_events_2025 = 21376`
- `raw.match_events_default = 0`
- total = `284210`
- PK distinta = `284210`
- duplicidades locais removidas do `default = 0`

Governanca:

- `dbmate status`:
  - `Applied: 26`
  - `Pending: 0`
- versions duplicadas em `db/migrations`:
  - `0`
- bootstrap limpo:
  - replay completo das `26` migrations atuais em banco temporario concluido com sucesso

### Diagnostico objetivo

- problema confirmado:
  - regressao real no runtime/API das superficies quentes da Onda 1
  - causa imediata confirmada:
    - ausencia no banco vivo dos indices quentes de `fact_fixture_player_stats` e `player_match_summary`
- suspeita forte:
  - drift operacional entre o runtime materializado do `mart` e o que o repositorio hoje declara via dbt
- hipotese ainda nao validada:
  - evento exato que removeu ou deixou de recriar esses indices

### Impacto

- governanca, bootstrap e `raw.match_events` seguem verdes
- Onda 4 permanece concluida
- o tuning nao pode ser declarado encerrado no estado atual porque ha regressao real de produto em:
  - `match_center`
  - `player_profile`

### Arquivos alterados nesta rodada final

- `artifacts/db_tuning/final_closure/baseline.json`
- `artifacts/db_tuning/final_closure/baseline.md`
- `docs/DB_TUNING_FINAL_CLOSURE.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Decisao

- blocker final confirmado
- tuning nao encerrado formalmente
- menor proximo passo seguro:
  - reconciliar o runtime do `mart` com os indices quentes ja declarados no repositorio, sem reabrir outras ondas

## Rodada curta - Reconciliacao runtime dos indices quentes do `mart`

### Diagnostico objetivo

- gap final confirmado:
  - `mart.fact_fixture_player_stats` sem indice quente vivo
  - `mart.player_match_summary` sem indice quente vivo
- o repositorio continuava correto:
  - `fact_fixture_player_stats.sql` declara `indexes` em `(match_id, team_id)`
  - `player_match_summary.sql` declara `indexes` em `(player_id, match_date desc, match_id desc)`
- materializacao atual:
  - `fact_fixture_player_stats`: `incremental`
  - `player_match_summary`: `table`
  - `fact_fixture_lineups`: `incremental`
- causa exata provada com artefatos dbt:
  - `dbt.log` mostra execucao de `fact_fixture_player_stats` e `player_match_summary` em `05:58`
  - os arquivos com `indexes` foram alterados apenas em `12:05`
  - depois disso houve `dbt parse`, mas nao `dbt run` desses modelos
- detalhe decisivo do adapter:
  - em `incremental`, `create_indexes(target_relation)` so roda em criacao inicial, `view` previa ou `full_refresh`
  - logo, um `dbt run` incremental normal nao recriaria o indice ausente de `fact_fixture_player_stats`

### Correcao escolhida

- menor intervencao segura:
  - reconciliar o runtime diretamente
  - sem alterar codigo ou migrations, porque o repositorio ja declarava o estado correto

Comandos executados:

- `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mart_fact_fixture_player_stats_match_team ON mart.fact_fixture_player_stats (match_id, team_id)`
- `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mart_player_match_summary_player_match_date ON mart.player_match_summary (player_id, match_date DESC, match_id DESC)`

### Validacao before/after

Catalogo after:

- `idx_mart_fact_fixture_player_stats_match_team`: presente
- `idx_mart_player_match_summary_player_match_date`: presente

Captura pos-correcao:

- `python tools/db_tuning_wave0_capture.py mart_hot_index_reconciled`

Queries quentes:

- `match_center_player_stats`
  - regressivo: `1322.409 ms`
  - after: `0.391 ms`
  - plano after: `Index Scan using idx_mart_fact_fixture_player_stats_match_team`
- `player_contexts`
  - regressivo: `37.068 ms`
  - after: `1.431 ms`
  - plano after: `Bitmap Index Scan using idx_mart_player_match_summary_player_match_date`

Endpoints:

- `match_center`
  - baseline Onda 0: `428.134 ms`
  - after Onda 1: `48.324 ms`
  - regressivo: `344.990 ms`
  - after reconciliacao: `62.755 ms`
- `player_profile`
  - baseline Onda 0: `347.434 ms`
  - after Onda 1: `49.462 ms`
  - regressivo: `228.556 ms`
  - after reconciliacao: `42.820 ms`

### Arquivos alterados nesta rodada curta

- `artifacts/db_tuning/mart_hot_index_reconciled/baseline.json`
- `artifacts/db_tuning/mart_hot_index_reconciled/baseline.md`
- `docs/MART_HOT_INDEX_RUNTIME_RECONCILIATION.md`
- `docs/DB_TUNING_FINAL_CLOSURE.md`
- `docs/DB_TUNING_EXECUTION_LOG.md`

### Decisao

- gap final do runtime/API: resolvido
- runtime e repositorio do `mart`: reconciliados
- tuning: encerrado
