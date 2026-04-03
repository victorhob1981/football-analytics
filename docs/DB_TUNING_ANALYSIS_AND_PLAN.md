# DB Tuning Analysis and Plan

Data de referencia: 2026-03-31  
Escopo: analise somente. Nenhuma alteracao de codigo, schema, indice, query, configuracao ou runtime foi executada como parte deste documento.

## 1. Resumo executivo

O banco deste projeto ja esta em um ponto em que tuning e necessario e viavel. O problema principal nao esta em um unico SQL isolado; ele esta na combinacao de quatro fatores:

- `mart` fisicamente subotimizado para o workload real de leitura da API/BFF.
- pipeline dbt com cadeias pesadas sobre `views`, repetindo parsing de `JSONB` em modelos player-centric.
- ingestao idempotente correta, mas com custo de escrita maior que o necessario por conta de multiplas passagens sobre staging/target.
- observabilidade de performance insuficiente para operar tuning de forma sustentavel.

Leituras objetivas confirmadas:

- O projeto roda sobre `Postgres 16.12` em um unico servico `postgres` no `docker compose`.
- O banco `football_dw` tem cerca de `5.9 GB`.
- `pg_stat_statements` nao esta habilitado.
- As maiores tabelas estao concentradas em dados de jogadores:
  - `raw.fixture_lineups`: `677917` rows, `1499 MB`
  - `raw.fixture_player_statistics`: `675734` rows, `1398 MB`
  - `mart.fact_fixture_player_stats`: `675734` rows, `1266 MB`
  - `mart.fact_fixture_lineups`: `674758` rows, `901 MB`
- O `mart` moderno praticamente nao tem indices fisicos. No estado observado, somente `mart.team_match_goals_monthly` e `mart.team_performance_monthly` tinham indices catalogados.
- Query real da API para player stats por partida fez `Parallel Seq Scan` em `mart.fact_fixture_player_stats` inteira e executou em `151.8 ms`, lendo `161408` buffers para retornar `53` rows.
- Query real da API para contextos de jogador fez `Parallel Seq Scan` em `mart.player_match_summary` e executou em `40.45 ms`, lendo `24737` buffers para retornar `13` rows agregados.
- O cliente de banco da API abre uma nova conexao `psycopg.connect(...)` a cada chamada `fetch_one/fetch_all/fetch_val`, sem pool e sem reutilizacao intra-request.
- O dbt mostrou baseline de `283s` para `36` modelos, com forte concentracao em `dim_player` (`146.55s`) e `fact_fixture_player_stats` (`112.2s`).

Diagnostico executivo:

- Problema confirmado de curto prazo: leituras centrais da API estao pagando I/O e scans desnecessarios no `mart`.
- Problema confirmado de medio prazo: a camada de transformacao vai crescer pior do que o volume de dados se continuar reprocessando `JSONB` pesado via `views`.
- Problema confirmado de governanca: falta baseline operacional de performance, faltam metricas de workload e ha sinais de drift entre schema vivo e migrations do repositorio.
- Problema provavel de escala: com mais competicoes, temporadas, usuarios simultaneos e dashboards, o banco passara a sofrer mais por conexoes, I/O e custos de refresh do que por CPU pura.

Direcao recomendada:

- Primeiro instrumentar.
- Depois estabilizar as leituras quentes do `mart`.
- Em seguida reduzir custo de transformacao e escrita.
- So depois discutir tuning fino de parametros de instancia e particionamento estrutural.

## 2. Contexto analisado

Fontes de evidencia usadas nesta analise:

- Repositorio local:
  - `docker-compose.yml`
  - `README.md`
  - `db/migrations/*.sql`
  - `dbt/dbt_project.yml`
  - `dbt/models/**`
  - `infra/airflow/dags/common/services/warehouse_service.py`
  - `infra/airflow/dags/common/services/ingestion_service.py`
  - `infra/airflow/dags/dbt_run.py`
  - `api/src/db/client.py`
  - `api/src/routers/*.py`
  - `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- Observacao read-only do banco em runtime:
  - versao do Postgres
  - tamanho do banco
  - parametros principais
  - tamanhos de relacoes
  - catalogo de indices
  - `pg_stat_user_tables` e `pg_stat_user_indexes`
  - `EXPLAIN (ANALYZE, BUFFERS)` em queries reais da API
- Artefatos do dbt:
  - `dbt/target/run_results.json`
  - `dbt/logs/dbt.log`

Estado funcional relevante do projeto:

- O banco serve workloads mistos:
  - ingestao batch via Airflow
  - transformacao via dbt
  - leitura operacional via BFF/API
  - consumo analitico via Metabase
- O projeto nao esta separado entre banco transacional e banco analitico. O mesmo Postgres absorve ingestao, transformacao, API e BI.
- O `raw` e a base canonica de ingestao; o `mart` e a camada de consumo prioritario; `gold` e legado historico.

## 3. Metodologia da analise

Esta analise foi conduzida em quatro frentes:

1. Leitura estrutural do schema e das migrations.
2. Leitura dos caminhos reais de escrita e leitura no codigo.
3. Observacao read-only do estado atual do banco.
4. Classificacao explicita entre achado confirmado, suspeita forte e hipotese.

Legenda usada:

- `Confirmado`: ha evidencia objetiva no codigo, no catalogo ou em `EXPLAIN`.
- `Suspeita forte`: ha evidencia indireta consistente, mas ainda falta medicao especifica ou validacao catalogal mais fina.
- `Hipotese`: plausivel tecnicamente, porem ainda dependente de benchmark, telemetria ou ambiente-alvo.

Limites desta analise:

- Nao houve teste de carga controlado.
- Nao houve captura de workload historico por ausencia de `pg_stat_statements`.
- Nao houve medicao de disco/IOPS do host.
- As views `pg_stat_*` podem ter sido afetadas por reinicio recente do container, entao alguns contadores de uso nao sao baseline confiavel.

## 4. Diagnostico por area

### 4.1 Modelagem e estrutura

#### Leitura geral do desenho

O desenho geral esta conceitualmente correto para um pipeline de dados:

- `raw` guarda granularidade canonica e payloads ricos.
- `mart` expande entidades e fatos de consumo.
- `gold` e tabelas legadas coexistem como historico.

O problema nao esta no conceito em si. O problema esta em como alguns dados largos do `raw` continuam vazando para o `mart`, e como o `mart` nao ganhou a camada fisica minima para suportar leitura de produto.

#### Achados

`Confirmado`  
As tabelas mais pesadas hoje sao player-centric e carregam colunas largas:

- `raw.fixture_lineups` tem coluna `details JSONB` em `db/migrations/20260219170000_sportmonks_extended_entities.sql`.
- `raw.fixture_player_statistics` e `raw.player_season_statistics` tem `statistics JSONB` e `payload JSONB` na mesma migration.
- `mart.fact_fixture_lineups` preserva `details` em `dbt/models/marts/core/fact_fixture_lineups.sql`.
- `mart.fact_fixture_player_stats` preserva `statistics` em `dbt/models/marts/core/fact_fixture_player_stats.sql`.

Efeito:

- maior largura de linha no `mart`;
- mais pagina lida por query mesmo quando o endpoint precisa de poucas colunas;
- mais custo de armazenamento, cache e vacuum/autovacuum.

`Confirmado`  
Existe coexistencia de superficies analiticas com maturidade fisica diferente:

- `raw`, `mart` e `gold` coexistem por desenho.
- tabelas legadas como `mart.team_match_goals_monthly` e `mart.team_performance_monthly` foram criadas por migration com PKs e indices.
- o `mart` moderno gerado por dbt nao recebeu a mesma camada fisica.

Efeito:

- duplicidade conceitual e operacional;
- tuning fragmentado;
- maior risco de otimizar a tabela errada.

`Confirmado`  
Parte do schema usa tipos mais fracos do que o ideal:

- `raw.fixtures.year` e `raw.fixtures.month` sao `TEXT` em `db/migrations/20260217120000_baseline_schema.sql`.
- tabelas legadas mensais tambem usam `year` e `month` como `TEXT`.
- `round` no baseline original e `TEXT`.

Efeito:

- ordenacao e comparacao temporal menos robustas;
- maior ambiguidade semantica;
- mais dependencia de convencao de formato.

`Confirmado`  
O `mart` moderno depende de testes logicos do dbt, nao de constraints fisicas do banco:

- `dbt/models/marts/core/schema.yml` e `dbt/models/marts/analytics/schema.yml` codificam `not_null`, `unique` e `relationships`.
- o catalogo observado nao mostra indices/PKs fisicos nas tabelas `mart` modernas.

Efeito:

- integridade verificada em pipeline, mas nao garantida no motor;
- queries do planner nao ganham beneficios indiretos de constraints/PKs;
- risco de drift fisico maior.

`Suspeita forte`  
Ha drift de schema entre runtime e migrations no dominio `raw.match_events`.

Evidencia:

- baseline do repositorio cria `raw.match_events` com PK `(event_id, season)` em `db/migrations/20260217120000_baseline_schema.sql`.
- a carga atual trabalha com `EVENTS_TARGET_COLUMNS` contendo `provider`, `provider_league_id`, `competition_key`, `season_label`, `provider_season_id`, `source_run_id` em `infra/airflow/dags/common/services/warehouse_service.py`.
- o upsert de eventos usa `ON CONFLICT (provider, season, fixture_id, event_id)`.
- o banco observado possui indice `idx_raw_match_events_competition_season_label`, o que sugere schema vivo mais rico do que o exposto nas migrations lidas.

Efeito:

- risco alto de futuras migrations/otimizacoes partirem de premissa errada;
- tuning fisico pode errar a chave verdadeira do dado;
- rollback e reproducao de ambiente ficam menos confiaveis.

#### Leitura de crescimento

`Confirmado`  
O crescimento ruim ja comecou nas tabelas de granularidade por jogador:

- `raw.fixture_lineups`: `677917` rows
- `raw.fixture_player_statistics`: `675734` rows
- `mart.fact_fixture_player_stats`: `675734` rows
- `mart.fact_fixture_lineups`: `674758` rows

Isso ainda e manejavel, mas e suficiente para mostrar gargalo de I/O quando nao ha estrutura fisica alinhada ao workload.

### 4.2 Indices

#### Cobertura atual

`Confirmado`  
O `raw` tem cobertura minima razoavel de chaves naturais e filtros obvios:

- `raw.fixtures`: PK em `fixture_id` e indices por `date_utc`, `home_team_id`, `away_team_id`, `league_id`, `season`, `league_id + season`.
- `raw.match_statistics`: PK `(fixture_id, team_id)` e indices por `fixture_id` e `team_id`.
- `raw.fixture_lineups`: PK e indices por `fixture`, `team`, `player`.
- `raw.fixture_player_statistics`: PK e indices por `fixture`, `team`, `player`.
- `raw.player_season_statistics`: PK e indices por `player`, `season`, `league`.

`Confirmado`  
O `mart` moderno esta praticamente sem indices fisicos.

Evidencia observada em catalogo:

- `pg_indexes` no schema `mart` retornou indices apenas para:
  - `team_match_goals_monthly`
  - `team_performance_monthly`
- nao apareceram indices para:
  - `fact_matches`
  - `fact_match_events`
  - `fact_fixture_lineups`
  - `fact_fixture_player_stats`
  - `player_match_summary`
  - `player_season_summary`
  - `dim_player`
  - `dim_team`
  - `dim_competition`

Conclusao:

- o maior gap fisico atual esta no `mart`, nao no `raw`.

#### Aderencia ao workload

`Confirmado`  
Ha desaderencia clara entre filtros reais da API e cobertura de indice no `mart`.

Evidencia:

- `api/src/routers/matches.py` filtra `mart.fact_fixture_player_stats` por `match_id`.
- `api/src/routers/players.py` filtra `mart.player_match_summary` por `player_id`.
- `api/src/core/filters.py` mostra que `mart.fact_matches` e consultada por `league_id`, `season`, `round_number`, `stage_id`, `date_day` e ocasionalmente `competition_key/season_label` via `exists` em `dim_stage`.

`Confirmado`  
As duas unicas queries medidas com `EXPLAIN` mostraram `Seq Scan` nas tabelas centrais do produto:

- Query de player stats por partida:
  - tabela: `mart.fact_fixture_player_stats`
  - tempo: `151.8 ms`
  - retorno: `53 rows`
  - buffers: `161408`
  - plano: `Parallel Seq Scan`
- Query de contexto por jogador:
  - tabela dominante: `mart.player_match_summary`
  - tempo: `40.45 ms`
  - retorno agregado: `13 rows`
  - buffers: `24737`
  - plano: `Parallel Seq Scan`

`Confirmado`  
Nem toda ausencia de indice e prioridade alta.

Evidencia:

- `select ... from mart.dim_player where player_id = ?` executou em `1.36 ms` mesmo com `Seq Scan`.

Conclusao:

- o problema nao e "indexar tudo";
- a prioridade deve seguir workload real, largura da tabela e custo de buffer.

#### Redundancia e excesso

`Suspeita forte`  
Ha possiveis indices redundantes nas particoes de `raw.match_events`.

Evidencia observada:

- `pg_stat_user_indexes` mostrou pares de nomes sugerindo duplicidade de mesma chave em `match_events_default` e `match_events_2024`, por exemplo:
  - `idx_raw_match_events_default_fixture_id`
  - `match_events_default_fixture_id_idx`
  - pares analogos para `player_id` e `team_id`

Risco:

- storage e write amplification sem retorno de leitura;
- confusao operacional sobre qual indice o planner deve usar.

`Hipotese`  
O `raw` pode comecar a sofrer com excesso de indice se cada novo dominio receber combinacoes compostas sem observar write path.

Hoje o maior risco de excesso nao e imediato. O maior risco hoje e falta de indice no `mart`.

### 4.3 Queries e padroes de acesso

#### Acesso do BFF/API

`Confirmado`  
O cliente de banco da API nao usa pool e nao reutiliza conexao por request.

Evidencia:

- `api/src/db/client.py` chama `psycopg.connect(...)` dentro de `_connection()`.
- Cada `fetch_all`, `fetch_one` e `fetch_val` abre e fecha sua propria conexao.

Efeito:

- mais latencia por round-trip;
- mais custo de autenticacao/handshake;
- mais pressao em `max_connections` conforme concorrencia cresce.

`Confirmado`  
Alguns endpoints de pagina detalhada fazem varias consultas sequenciais por request.

Exemplos:

- Match center em `api/src/routers/matches.py` faz leituras separadas para:
  - match base
  - timeline
  - lineups
  - team stats
  - player stats
- Perfil de jogador em `api/src/routers/players.py` faz leituras separadas para:
  - `player_ref`
  - `context_rows`
  - `summary_row`
  - `recent_rows`
  - `history_rows`
  - `stats_rows`
- Perfil de time em `api/src/routers/teams.py` segue padrao semelhante.

Conclusao:

- o risco central nao e um N+1 classico por loop de linhas;
- o risco central e "multi-query endpoint pattern" somado a ausencia de pool.

`Confirmado`  
Nao ha evidencia de N+1 classico por linha em loop.

Evidencia:

- a inspecao dos routers mostrou loops predominantemente de serializacao pos-query;
- nao foram encontrados trechos obvios de `db_client.fetch_*` dentro de loops de rows.

#### Formato das queries

`Confirmado`  
O BFF consulta `fact_matches` de forma muito recorrente e a usa como tabela de escopo para varias paginas:

- listas de times
- rankings
- hub de competicao
- paginas de time
- paginas de jogador
- match center
- home e search

`Confirmado`  
As queries de busca sao naturalmente caras para btree simples.

Evidencia em `api/src/routers/search.py`:

- uso de `translate(lower(coalesce(...)))`
- uso de `LIKE '%termo%'`
- uso de prefixos e variantes tokenizadas

Efeito:

- indice btree na coluna bruta nao resolve esse padrao;
- dimensoes pequenas ainda toleram scan;
- com crescimento, search vai pedir indice funcional/especializado ou superficie dedicada.

`Confirmado`  
Ha varios padroes com `union all`, `row_number()`, `count(distinct ...)`, agregacao e filtros globais.

Exemplos:

- `teams.py`: uniao de mandante/visitante, `row_number`, agregacao final.
- `rankings.py`: escopos calculados com `union all` e coverage queries.
- `competition_hub.py`: multiplos agregados por `stage`, inclusive uniao de times por lado da partida.
- `players.py`: agregacao por jogador e contexto mais recente com ordenacao por data.

Conclusao:

- o workload nao e transacional simples;
- ele e de leitura analitica curta sobre tabelas de produto;
- isso favorece `mart` com indices compostos e/ou agregados persistidos.

`Confirmado`  
O BFF mistura `raw` e `mart` na leitura.

Exemplos:

- `rankings.py` usa `raw.match_statistics` junto com `mart.fact_matches`.
- `search.py` usa `raw.fixtures` para `kickoff_at/status` junto com `mart.fact_matches`.

Efeito:

- tuning precisa cobrir duas camadas;
- parte da API ainda depende de dado cru por falta de consolidacao total no `mart`.

### 4.4 Estatisticas, planner e manutencao

`Confirmado`  
O pipeline dbt ja executa `ANALYZE;` ao final em `infra/airflow/dags/dbt_run.py`.

Isto e positivo. O projeto ja reconhece que estatistica do planner importa.

`Confirmado`  
Mesmo assim, a observabilidade atual de planner/workload e insuficiente.

Evidencia:

- `shared_preload_libraries` esta vazio.
- extensao `pg_stat_statements` nao esta instalada.
- sem ela, nao ha ranking confiavel de SQL por tempo, chamadas, buffers ou I/O.

`Confirmado`  
As views `pg_stat_user_tables` observadas no momento da analise nao estavam maduras o bastante para uso executivo.

Evidencia:

- tabelas grandes apareceram com `n_live_tup = 0` apesar de contagens reais > `600k`.
- varias tabelas estavam sem `last_analyze` e sem `last_autovacuum` uteis no snapshot analisado.

Interpretacao correta:

- isso nao prova necessariamente problema estrutural de `ANALYZE`;
- mas prova que o projeto hoje nao possui baseline persistente e confiavel de manutencao/uso para operar tuning com conforto.

`Suspeita forte`  
As tabelas de upsert frequente do `raw` tendem a ser as primeiras candidatas a bloat/fragmentacao conforme o portfolio crescer:

- `raw.fixture_player_statistics`
- `raw.fixture_lineups`
- `raw.match_events`
- `raw.match_statistics`
- `raw.provider_sync_state` e pequeno e nao preocupa.

Motivo:

- padrao `INSERT ... ON CONFLICT DO UPDATE`
- atualizacao de `updated_at = now()`
- repeticao de cargas idempotentes

Hoje nao ha evidencia objetiva de bloat alto. Falta telemetria para confirmar ou refutar.

### 4.5 Particionamento e distribuicao

`Confirmado`  
`raw.match_events` e particionada por `season`, mas a estrategia esta incompleta.

Evidencia:

- baseline cria `PARTITION BY LIST (season)` em `db/migrations/20260217120000_baseline_schema.sql`.
- existe particao explicita `match_events_2024`.
- no banco observado, `match_events_default` ainda responde pela maior parte do storage de eventos visivel no snapshot:
  - `match_events_default`: `115 MB`
  - `match_events_2024`: `34 MB`

Complemento:

- snapshot documental anterior (`docs/INVENTARIO_DADOS_DO_PROJETO.md`) ja indicava a maior parte das rows fora da particao explicita.

Conclusao:

- o particionamento existe, mas ainda nao esta entregando o valor maximo de pruning e organizacao operacional.

`Confirmado`  
Particionar `fact_matches` agora nao e prioridade.

Motivo:

- `mart.fact_matches` observada com `15878` rows e `12 MB`.
- o problema atual de `fact_matches` e desenho de acesso, nao volume absoluto.

`Suspeita forte`  
Se o projeto ampliar portfolio e historico, as primeiras candidatas reais a particionamento adicional sao tabelas raw append-heavy e wide:

- `raw.match_events`
- eventualmente `raw.fixture_player_statistics`
- eventualmente `raw.fixture_lineups`

Mas:

- isso so faz sentido depois de fechar estrategia de chave temporal/cobertura;
- particionamento prematuro aumenta complexidade de manutencao e pode piorar write path.

### 4.6 Escrita, ingestao e pipelines

`Confirmado`  
Os loaders `silver -> raw` estao corretos do ponto de vista de idempotencia, mas pagam custo extra de escrita e leitura.

Padrao observado em `infra/airflow/dags/common/services/warehouse_service.py`:

- le arquivos parquet do MinIO/S3
- concatena `DataFrame` em memoria
- cria tabela temporaria `staging_*`
- grava staging via `to_sql(..., method="multi")`
- calcula `inserted`
- calcula `updated`
- executa `INSERT ... ON CONFLICT DO UPDATE`

Efeito:

- tres passagens logicas sobre staging/target por carga:
  - contagem de novos
  - contagem de alterados
  - carga final
- mais I/O e CPU do que o necessario para um mesmo lote
- mais custo de lock e WAL conforme o volume crescer

`Confirmado`  
O projeto usa `IS DISTINCT FROM` para evitar updates desnecessarios.

Isto e positivo e reduz write amplification quando bem aplicado.

`Suspeita forte`  
O uso de `pandas.to_sql(..., method="multi")` sem `chunksize` explicito pode virar gargalo de statement size, memoria e throughput em lotes maiores.

Nao ha evidencia objetiva de falha atual. Ha risco de escala.

`Confirmado`  
O banco absorve simultaneamente:

- ingestao Airflow
- dbt
- API/BFF
- Metabase

Evidencia:

- um unico servico `postgres` em `docker-compose.yml`
- `README.md` posiciona o mesmo banco como warehouse operacional e fonte do Metabase

Conclusao:

- o projeto opera workload misto no mesmo motor;
- qualquer excesso de indice ou refresh pesado passa a competir diretamente com leitura de produto.

### 4.7 Marts, views e materializacao

`Confirmado`  
O dbt usa:

- `staging`: `view`
- `intermediate`: `view`
- `marts`: `table` por default

Evidencia:

- `dbt/dbt_project.yml`

`Confirmado`  
Os modelos mais caros hoje sao exatamente os que dependem de parsing e consolidacao de dados de jogador.

Baseline observada no `dbt run`:

- `dim_player`: `146.55s`
- `fact_fixture_player_stats`: `112.2s`
- `player_season_summary`: `10.64s`

`Confirmado`  
Os modelos player-centric fazem muito trabalho sobre `JSONB`:

- `stg_fixture_player_statistics.sql` usa `jsonb_array_elements`, `regexp_replace`, `max(case when ...)`.
- `stg_player_season_statistics.sql` faz parsing semelhante.
- `dim_player.sql` consolida ids/nomes/nacionalidade com `union`, `row_number()` e deduplicacao.
- `int_fixture_player_context.sql` junta lineups, player stats, events e matches.

Conclusao:

- hoje o principal custo de transformacao e "CPU + I/O de parsing/recombination", nao apenas join simples.

`Suspeita forte`  
Ha oportunidades claras para materializacao intermediaria/persistente antes do `mart` final em trilhas pesadas de jogador.

Racional:

- a mesma logica pesada e consumida por mais de um modelo final;
- manter tudo como `view` empurra custo repetido para cada `run`;
- o beneficio tende a crescer mais rapido que o custo de storage adicional.

`Confirmado`  
Ha espaco para servir leituras prontas em alguns modulos da API.

Candidatos conceituais:

- busca global
- hubs de competicao por stage
- contexts de jogador/time
- rankings recorrentes
- comparativos home/away

Hoje varios desses modulos recalculam derivacoes sobre `fact_matches` e `player_match_summary` em runtime.

### 4.8 Infra e configuracao

`Confirmado`  
Parametros observados no banco analisado:

- `shared_buffers = 128 MB`
- `work_mem = 4 MB`
- `maintenance_work_mem = 64 MB`
- `effective_cache_size = 4 GB`
- `max_connections = 100`

`Hipotese`  
Esses parametros podem estar subotimizados para producao, mas nao ha base suficiente para afirmar isso sem:

- memoria real do host
- concorrencia alvo
- perfil de escrita/leitura
- tipo de storage

Interpretacao correta:

- `128 MB` de `shared_buffers` e baixo em termos absolutos frente a um banco de `5.9 GB`;
- mas tuning de memoria sem contexto do host e perigoso;
- o primeiro problema confirmado continua sendo desenho fisico/observabilidade.

`Confirmado`  
Nao ha pool de conexoes na camada da aplicacao.

Com `max_connections = 100`, isso e um risco mais claro do que `shared_buffers` no curto prazo, porque a API faz varias chamadas ao banco por request.

`Hipotese`  
Checkpoints, WAL, fsync, `checkpoint_timeout`, `wal_compression`, `random_page_cost`, `effective_io_concurrency` e afins podem ser relevantes em ambiente de producao.  
Nao ha evidencias suficientes no repositorio local para concluir ajuste fino desses parametros.

### 4.9 Observabilidade e governanca de performance

`Confirmado`  
Falta camada minima de observabilidade SQL para tuning profissional:

- sem `pg_stat_statements`
- sem baseline de top SQL
- sem historico por endpoint
- sem budget/perfis de latency documentados
- sem rotina explicita de revisao de bloat e manutencao

`Confirmado`  
Os dados documentais de inventario ja nao batem exatamente com o estado vivo observado.

Exemplo:

- `docs/INVENTARIO_DADOS_DO_PROJETO.md` registra snapshot de `2026-03-24`.
- o banco observado em `2026-03-31` ja mostra contagens maiores em fatos player-centric e `fact_matches`.

Isto nao e erro funcional, mas e sinal de que governanca de capacidade/performance ainda nao esta operacionalizada.

`Suspeita forte`  
Sem baseline por endpoint, o time corre risco de:

- celebrar ganho que nao existe;
- introduzir regressao silenciosa no dbt ou no BFF;
- indexar a tabela errada;
- ajustar parametro de instancia para mascarar problema de modelagem.

## 5. Gargalos confirmados

1. `mart` moderno sem indices fisicos nas tabelas quentes do produto.
2. Query de player stats por partida varrendo `mart.fact_fixture_player_stats` inteira via `Parallel Seq Scan`.
3. Query de contexto de jogador varrendo `mart.player_match_summary` via `Parallel Seq Scan`.
4. API sem pool de conexoes e sem reutilizacao de conexao por request.
5. Endpoints detalhados com varias queries sequenciais por request.
6. dbt pesado nas cadeias player-centric por uso intensivo de `views` e parsing de `JSONB`.
7. Carga `silver -> raw` com overhead estrutural de staging + contagem de inserts + contagem de updates + upsert.
8. `raw.match_events` com particionamento incompleto e concentracao relevante em particao default.
9. Ausencia de `pg_stat_statements` e de baseline SQL operacional.

## 6. Gargalos provaveis

1. Busca global com `translate(lower(...)) like '%...%'` tende a virar gargalo conforme dimensoes crescerem.
2. Consultas em `fact_matches` por `league_id/season/date/stage` tendem a piorar em paginas de listagem e rankings se a superficie continuar sem indices fisicos.
3. `competition_hub` tende a concentrar scans e agregacoes repetidas por `competition_key/season_label/stage`.
4. Tabelas raw com upsert recorrente tendem a desenvolver bloat e write amplification sem rotina de medicao.
5. Possiveis indices redundantes em particoes de `match_events` podem estar adicionando custo de escrita sem beneficio.
6. Drift entre migrations e schema vivo pode comprometer qualquer programa serio de tuning se nao for saneado primeiro.
7. A convivencia de ingestao, dbt, API e Metabase em um unico Postgres tende a virar gargalo de concorrencia/IO antes do limite logico de modelagem.

## 7. Oportunidades de tuning por categoria

### 7.1 Modelagem e estrutura

- Formalizar um mapa unico de "tabelas de verdade para leitura de produto".
- Reduzir propagacao de colunas largas do `raw` para fatos do `mart` quando nao forem essenciais ao consumo.
- Revisar colunas temporais e categorizacoes ainda tipadas como `TEXT` em superficies legadas.
- Fechar drift entre schema vivo e migrations antes de qualquer onda fisica maior.

### 7.2 Indices

- Priorizar indices alinhados a filtros reais do BFF, nao a chaves teoricas.
- Comecar por fatos largos com filtro altamente seletivo:
  - `fact_fixture_player_stats`
  - `player_match_summary`
- Depois avaliar indices compostos em superficies de escopo:
  - `fact_matches`
  - `dim_stage`
  - tabelas de busca
- Revisar duplicidade de indices em particoes de `match_events`.
- Tratar indices pequenos de dimensao como baixa prioridade se a latencia observada ja for baixa.

### 7.3 Queries e contratos de API

- Reduzir round-trips por endpoint onde o custo de conexao pesa mais do que o custo do SQL individual.
- Consolidar melhor a fronteira `raw` vs `mart` para que a API nao precise montar contextos cruzando camadas.
- Revisar endpoints de busca e ranking com foco em superficies serviveis e nao apenas em query tuning pontual.
- Criar baseline por endpoint critico:
  - home
  - competition hub
  - match center
  - team profile
  - player profile
  - rankings
  - global search

### 7.4 Estatisticas, planner e manutencao

- Instrumentar top SQL e buffers.
- Medir bloat antes de qualquer reindexacao.
- Definir rotina de inspecao de autovacuum/analyze.
- Criar baseline de row counts, index hit ratio e growth por tabela.

### 7.5 Particionamento

- Terminar de avaliar `raw.match_events` antes de expandir particionamento para outras tabelas.
- Nao particionar `fact_matches` agora.
- So considerar novos particionamentos em tabelas raw realmente append-heavy e com filtros fortemente alinhados ao criterio de particao.

### 7.6 Escrita e ingestao

- Revisar custo do staging e do pre-count de inserts/updates.
- Avaliar estrategia de batch/chunk e mecanismo de carga mais barato que `to_sql(method="multi")` para volumes maiores.
- Observar lock/WAL/contention em janelas de carga.
- Separar claramente o que precisa de idempotencia row-by-row do que pode usar carga mais orientada a lote.

### 7.7 Marts e materializacao

- Materializar intermediarios pesados de jogador para cortar recomputacao.
- Identificar agregados de leitura frequente que merecem persistencia.
- Diferenciar:
  - superficie de produto de baixa latencia
  - superficie de exploracao BI
  - superficie canonica de depuracao

### 7.8 Infra e configuracao

- Introduzir pool de conexoes na API antes de mexer agressivamente em `max_connections`.
- Ajustar memoria e parametros do Postgres somente apos baseline do workload.
- Se o workload crescer, considerar separacao de leitura analitica/API da carga/transformacao.

### 7.9 Observabilidade e governanca

- Instrumentar queries.
- Versionar baseline por endpoint e por modelo dbt.
- Monitorar crescimento por tabela/indice.
- Definir processo de revisao de schema drift e de indices nao usados.

## 8. Matriz impacto x esforco x risco

| Iniciativa | Categoria | Impacto esperado | Esforco | Risco | Status de evidencia |
|---|---:|---:|---:|---:|---|
| Habilitar observabilidade SQL (`pg_stat_statements` e baseline) | Observabilidade | Alto | Baixo | Baixo | Confirmado |
| Indexar fatos quentes do `mart` alinhados ao BFF | Indices | Alto | Medio | Medio | Confirmado |
| Introduzir pool/reuso de conexoes na API | Infra/aplicacao | Alto | Medio | Medio | Confirmado |
| Materializar trilha pesada de jogador no dbt | Materializacao | Alto | Medio | Medio | Confirmado |
| Revisar custo do staging + upsert + contagens de carga | Escrita/ingestao | Medio/alto | Medio | Medio | Confirmado |
| Revisar particionamento e indexacao de `raw.match_events` | Particionamento | Medio | Medio | Medio | Confirmado |
| Criar superficie dedicada para busca | Queries/serving | Medio/alto | Alto | Medio | Confirmado |
| Reduzir colunas largas no `mart` | Modelagem | Medio | Alto | Medio | Suspeita forte |
| Racionalizar superficies legadas (`gold` e marts antigas) | Governanca/modelagem | Medio | Alto | Medio | Confirmado |
| Ajustar parametros de memoria/IO do Postgres | Infra/config | Medio | Medio | Alto sem baseline | Hipotese |
| Considerar separacao de workloads ou replica de leitura | Arquitetura | Alto | Alto | Alto | Hipotese |
| Sanear drift entre schema vivo e migrations | Governanca | Alto | Medio | Medio | Suspeita forte |
| Revisar indices redundantes em particoes | Indices/manutencao | Medio | Baixo/medio | Baixo | Suspeita forte |

## 9. Plano de execucao futuro em ondas/blocos

### Onda 0 - Baseline e governanca

Objetivo:

- sair de "achismo tecnico" para "tuning guiado por evidencia recorrente".

Escopo:

- habilitar instrumentacao de top SQL;
- capturar baseline por endpoint critico;
- capturar baseline de runtime do dbt por modelo;
- mapear crescimento de tabelas e indices;
- reconciliar schema vivo vs migrations nas tabelas sensiveis.

Saida esperada:

- top 20 queries por tempo total, media, buffers e chamadas;
- ranking dos modelos dbt por tempo e rows processadas;
- inventario fisico atualizado;
- decisao informada de primeira onda de indices e materializacoes.

### Onda 1 - Estabilizacao das leituras quentes

Objetivo:

- reduzir latencia e I/O das paginas de produto com menor risco de regressao.

Escopo:

- focar `fact_fixture_player_stats`;
- focar `player_match_summary`;
- revisar `fact_matches` conforme filtros dominantes;
- reduzir custo de conexao na API;
- validar efeito nos endpoints:
  - match center
  - player profile
  - team profile
  - rankings

Criterio de sucesso:

- queda material em buffers e latencia nas queries medidas;
- sem degradacao perceptivel no write path.

### Onda 2 - Reducao de custo de transformacao dbt

Objetivo:

- cortar recomputacao pesada e tornar o `dbt run` mais previsivel.

Escopo:

- revisar cadeia `stg_fixture_player_statistics -> int_fixture_player_context -> fact_fixture_player_stats`;
- revisar `dim_player`;
- avaliar onde `view` deve virar superficie persistida;
- separar o que e core incremental do que e agregado final.

Criterio de sucesso:

- reducao clara de wall clock nos modelos mais caros;
- reducao de consumo repetido de `JSONB` parsing.

### Onda 3 - Escrita, ingestao e manutencao

Objetivo:

- reduzir custo operacional da carga sem sacrificar idempotencia.

Escopo:

- medir custo real de staging/temp tables;
- revisar batching/chunking;
- revisar contagem de inserts/updates antes do upsert;
- medir locks, WAL e autovacuum nas tabelas raw mais ativas.

Criterio de sucesso:

- menor custo por lote;
- menor janela de competicao entre carga, dbt e leitura.

### Onda 4 - Estrutura e escala de longo prazo

Objetivo:

- preparar o banco para portfolio maior, uso mais concorrente e BI mais pesado.

Escopo:

- concluir estrategia de particionamento de `match_events`;
- avaliar se lineups/player-stats precisam de estrategia estrutural adicional;
- racionalizar superficies legadas;
- avaliar separacao de workloads ou replica de leitura;
- formalizar politica de indices, vacuum e observabilidade.

Criterio de sucesso:

- crescimento previsivel;
- manutencao mais simples;
- menor risco de regressao por tuning reativo.

## 10. Dependencias e pre-requisitos

1. Ambiente de referencia para benchmark que represente o uso alvo.
2. Janela de manutencao ou ambiente de staging para experimentar tuning sem contaminar o baseline.
3. Definicao de SLO/SLA por endpoint ou pelo menos metas de latencia internas.
4. Telemetria de queries e de dbt.
5. Inventario fisico atualizado do schema real.
6. Visibilidade do host:
   - memoria
   - CPU
   - storage
   - IOPS
7. Separacao clara entre objetivos:
   - reduzir latencia de API
   - reduzir tempo de transformacao
   - reduzir custo de ingestao
   - aumentar escalabilidade

## 11. Riscos de intervencao

1. Indexar demais o `raw` e degradar ingestao/WAL.
2. Afinar parametros de memoria sem contexto do host e piorar estabilidade.
3. Particionar tabelas cedo demais e aumentar complexidade sem ganho real.
4. Materializar demais o dbt e aumentar custo de refresh/storage sem cortar latencia percebida.
5. Resolver busca com indice inadequado e gerar falsa sensacao de ganho.
6. Otimizar em cima de schema drift e consolidar erro estrutural.
7. Afinar ambiente local e extrapolar conclusoes para producao.

## 12. Checklist de validacao futura

### Baseline

- Top SQL por tempo total, media, chamadas e buffers.
- P50/P95/P99 dos endpoints criticos.
- Tempo total e por modelo do `dbt run`.
- Growth mensal por tabela e por indice.

### Leituras

- `EXPLAIN (ANALYZE, BUFFERS)` before/after nas queries criticas.
- Buffers lidos vs buffers hit.
- Rows retornadas vs rows varridas.
- Variacao de latencia sob concorrencia moderada.

### Escritas

- rows inseridas, atualizadas e ignoradas por lote.
- tempo por carga `silver -> raw`.
- WAL gerado por lote.
- locks e waits durante carga/dbt.

### Manutencao

- `autovacuum_count`, `autoanalyze_count`, `last_autovacuum`, `last_autoanalyze`.
- bloat por tabela e por indice.
- tamanho de indice nao usado.

### Governanca

- consistencia entre migrations e schema vivo.
- inventario atualizado do estado fisico.
- registro de decisao para cada indice/materializacao criada.

## 13. Conclusao objetiva

O banco deste projeto nao esta "mal desenhado" no sentido arquitetural. Ele esta subacabado do ponto de vista fisico e operacional para o workload que o proprio projeto ja criou.

O principal gargalo confirmado hoje esta na leitura do `mart`, especialmente na trilha de dados de jogador. O segundo gargalo confirmado esta na ausencia de observabilidade SQL e no custo estrutural do dbt sobre `views` pesadas. O terceiro gargalo confirmado esta na camada da API, que faz varias consultas por request sem pool de conexoes.

O caminho correto nao e partir direto para tuning de parametro ou particionamento amplo. O caminho correto e:

1. instrumentar;
2. estabilizar leituras quentes do `mart`;
3. reduzir recomputacao no dbt;
4. revisar write path e manutencao;
5. so entao discutir tuning de instancia e arquitetura de escala.

Se esse plano for seguido nessa ordem, o projeto tende a ganhar:

- menor latencia de API;
- menor custo de I/O;
- menor runtime de transformacao;
- menor risco de regressao;
- melhor sustentabilidade de longo prazo.
