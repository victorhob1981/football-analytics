
# Plano detalhado de evolucao de arquitetura e ingestao SportMonks (Advanced)

Data de referencia: 2026-02-19  
Projeto: `football-analytics`  
Escopo: suportar ingestao confiavel e analise para os conjuntos de dados listados (competicao, fixtures completos, team stats, eventos, lineups/minutos, player stats, transferencias, lesoes/suspensoes, tecnicos, ratings, head-to-head), com foco em idempotencia, rastreabilidade e operacao segura.

---

## 1. Objetivo do documento

Este documento foi escrito para guiar a mudanca em duas frentes:

1. **Arquitetura**: definir o que deve ser modificado, removido e adicionado no repositorio para suportar os novos dados sem quebrar o que ja funciona.
2. **Operacao**: detalhar como executar a nova ingestao em ambiente local/operacional apos as mudancas.

O foco e deixar o projeto pronto para crescer sem criar divida tecnica que inviabilize manutencao futura.

---

## 2. Estado atual (AS-IS) com base no codigo do repositorio

### 2.1 Fluxo atual oficial

Fluxo orquestrado pela DAG `pipeline_brasileirao`:

1. Ingestao Bronze:
   - `ingest_brasileirao_2024_backfill` (fixtures)
   - `ingest_statistics_bronze` (team match stats)
   - `ingest_match_events_bronze` (eventos)
2. Bronze -> Silver:
   - `bronze_to_silver_fixtures_backfill`
   - `bronze_to_silver_statistics`
   - `bronze_to_silver_match_events`
3. Silver -> Raw (Postgres):
   - `silver_to_postgres_fixtures`
   - `silver_to_postgres_statistics`
   - `silver_to_postgres_match_events`
4. Transformacao e qualidade:
   - `dbt_run`
   - `great_expectations_checks`
   - `data_quality_checks`

### 2.2 O que ja esta bem resolvido

1. **Idempotencia** em cargas raw principais via `ON CONFLICT ... DO UPDATE` com `IS DISTINCT FROM`.
2. **Grain explicito** para:
   - `raw.fixtures`: `fixture_id`
   - `raw.match_statistics`: `(fixture_id, team_id)`
   - `raw.match_events`: `(event_id, season)` com particao por `season`
3. **Controle de progresso** para backfill em statistics/events com `raw.provider_sync_state`.
4. **Base provider-aware inicial** ja criada:
   - `raw.provider_entity_map`
   - `raw.provider_sync_state`

### 2.3 Lacunas para o objetivo desejado

Hoje o projeto cobre bem fixtures, stats por time e eventos. As lacunas para o escopo SportMonks Advanced sao:

1. Nao ha ingestao dedicada para:
   - estrutura de competicao (leagues/seasons/stages/rounds em tabelas proprias)
   - standings snapshot por rodada
   - lineups e minutos
   - stats por jogador por partida
   - stats de jogador por temporada
   - transferencias
   - lesoes/suspensoes
   - tecnicos/historico
   - ratings dedicados
   - head-to-head modelado
2. Nao existe fato analitico no mart para responder perguntas de tipo:
   - "quantos chutes o jogador X deu na partida Y"
3. O contrato de `raw.match_statistics` e por time (nao por jogador), entao ele nao substitui player match stats.
4. A interface `ProviderAdapter` ainda expoe apenas 4 metodos (`fixtures`, `fixture_statistics`, `fixture_events`, `standings`).

### 2.4 Implicacao analitica direta

Sem `fixture_player_stats` e sem `lineups/minutes` no modelo atual, nao e possivel construir indicadores robustos por 90 minutos, ranking individual completo, comparativos com xG individual/rating por jogador por partida, ou perguntas textuais de scout com confianca.

---

## 3. Arquitetura alvo (TO-BE)

## 3.1 Principios de desenho

1. **Idempotencia obrigatoria**: toda carga deve poder rodar 2 vezes sem inflar dados.
2. **Contrato de grain por tabela**: cada entidade com chave natural explicita.
3. **Provider-aware**: manter origem (`source_provider`) e `source_id`, com mapeamento canonico quando necessario.
4. **Evolucao incremental**: introduzir novas entidades sem quebrar tabelas existentes.
5. **Observabilidade desde o inicio**: cada etapa com contagem, erro, retries e progresso.
6. **Qualidade automatizada**: dbt + GE + checks SQL para os novos datasets.

### 3.2 Mapa de dominios funcionais

Arquitetura recomendada por dominio:

1. Dominio `competition_structure`
   - leagues, seasons, stages, rounds, standings snapshots
2. Dominio `fixtures_core`
   - fixtures completas (status, HT/FT, arbitragem, venue, attendance, weather)
3. Dominio `match_team_stats`
   - stats por time por partida (incluindo xG e metricas avancadas)
4. Dominio `match_events`
   - eventos granulares por minuto e jogador
5. Dominio `lineups_minutes`
   - lineup inicial, banco, substituicoes, posicao, formacao, minutos
6. Dominio `player_stats`
   - player stats por partida e por temporada
7. Dominio `squad_context`
   - transferencias, lesoes/suspensoes, tecnicos
8. Dominio `comparative_analytics`
   - ratings dedicados e head-to-head

### 3.3 Fluxo macro alvo

1. Ingestao Bronze por dominio (json canonico)
2. Mapeamento Silver por dominio (parquet typed)
3. Load Raw por dominio (upsert idempotente)
4. Transformacao dbt:
   - staging por fonte raw
   - intermediarios de consolidacao
   - fatos/dimensoes analiticas finais
5. Quality gates
6. Exposicao BI (Metabase)

---

## 4. Mudancas de arquitetura: o que modificar, remover e adicionar

## 4.1 Providers (`infra/airflow/dags/common/providers/`)

### Modificar

1. `base.py`
   - ampliar `ProviderAdapter` com novos contratos:
     - `get_competition_structure(...)`
     - `get_fixture_lineups(...)`
     - `get_fixture_player_statistics(...)`
     - `get_player_season_statistics(...)`
     - `get_transfers(...)`
     - `get_injuries(...)`
     - `get_suspensions(...)`
     - `get_coaches(...)`
     - `get_head_to_head(...)`
2. `sportmonks.py`
   - implementar os metodos acima convertendo payload para envelope canonico.
3. `registry.py`
   - manter o padrao atual de provider default e rate limit, incluindo novos env vars por entidade.

### Adicionar

1. Mapeadores internos no provider para converter codigos SportMonks em nomes canonicos de metrica.
2. Metadata padrao em `provider_meta` com pagina, limite diario, endpoint, e filtros usados.

### Remover (ou descontinuar)

1. Logica duplicada de normalizacao espalhada fora de provider.
2. Qualquer caminho legado que acesse provider sem passar pelo adapter canonico.

Motivo:
Centralizar conversao de payload no adapter reduz divergencia e impede que cada DAG "interprete a API de um jeito".

---

## 4.2 Servico de ingestao Bronze (`infra/airflow/dags/common/services/ingestion_service.py`)

### Modificar

1. Extrair um fluxo generico reutilizavel para entidades por fixture:
   - resolver fixtures alvo
   - aplicar skip/cursor
   - chamar provider endpoint da entidade
   - escrever `data.json`
   - persistir sync_state
2. Reaproveitar o que hoje ja existe em statistics/events:
   - `_resolve_pending_fixture_ids`
   - `_calculate_next_cursor`
   - `_upsert_sync_state`

### Adicionar

1. Novas funcoes de ingestao:
   - `ingest_competition_structure_raw()`
   - `ingest_standings_raw()`
   - `ingest_lineups_raw()`
   - `ingest_fixture_player_stats_raw()`
   - `ingest_player_season_stats_raw()`
   - `ingest_transfers_raw()`
   - `ingest_injuries_raw()`
   - `ingest_suspensions_raw()`
   - `ingest_coaches_raw()`
   - `ingest_head_to_head_raw()`
2. Parametros padronizados em `dag_run.conf`:
   - `mode`: `incremental` ou `backfill`
   - `provider`
   - `league_id`, `season`/`season_id`
   - `fixture_ids` quando aplicavel
   - `entity_filters` opcionais (ex.: `team_id`, `player_id`)

### Remover (gradual)

1. Hardcode de estrategias por entidade no corpo principal.
2. Dependencia em comportamento implicito de `params` sem validacao de tipo.

Motivo:
Escalar para 10+ entidades sem padrao comum explode manutencao e aumenta risco de regressao.

---

## 4.3 DAGs de ingestao e pipeline (`infra/airflow/dags/`)

### Modificar

1. `pipeline_brasileirao.py`
   - criar novos TaskGroups por dominio:
     - `group_competition_structure`
     - `group_fixture_enrichment`
     - `group_player_layer`
     - `group_context_extras`
   - manter `dbt_run -> GE -> SQL checks` no final.

### Adicionar

Padrao por entidade (3 DAGs):

1. Bronze:
   - `ingest_<entity>_bronze.py`
2. Silver:
   - `bronze_to_silver_<entity>.py`
3. Raw:
   - `silver_to_postgres_<entity>.py`

Entidades novas recomendadas:

1. `competition_structure`
2. `standings`
3. `lineups`
4. `fixture_player_statistics`
5. `player_season_statistics`
6. `transfers`
7. `injuries`
8. `suspensions`
9. `coaches`
10. `head_to_head`

### Remover (gradual)

1. DAGs ad-hoc para cargas manuais sem contrato.
2. Qualquer DAG que escreva direto em mart sem passar por dbt.

Motivo:
Padrao homogeneo por entidade simplifica operacao, monitoramento e troubleshooting.

---

## 4.4 Mapping Silver (`infra/airflow/dags/common/mappers/` + `mapping_service.py`)

### Modificar

1. `mapping_service.py`
   - adicionar funcoes `map_<entity>_raw_to_silver()`.
   - manter semantica "latest key by fixture" para entidades fixture-scoped.
2. Padrao de dedupe:
   - por chave natural da entidade (nao por `drop_duplicates` generico sem grain definido).

### Adicionar

Novos mappers:

1. `competition_structure_mapper.py`
2. `standings_mapper.py`
3. `lineups_mapper.py`
4. `fixture_player_statistics_mapper.py`
5. `player_season_statistics_mapper.py`
6. `transfers_mapper.py`
7. `injuries_mapper.py`
8. `suspensions_mapper.py`
9. `coaches_mapper.py`
10. `head_to_head_mapper.py`

### Remover (gradual)

1. Transformacao ad-hoc de metrica no loader raw.
2. Parse de json complexo fora dos mappers.

Motivo:
Silver precisa ser a camada de tipagem e normalizacao, deixando raw loader focado em contrato/upsert.

---

## 4.5 Load Raw (`infra/airflow/dags/common/services/warehouse_service.py` + migrations)

### Modificar

1. `warehouse_service.py`
   - adicionar `TARGET_COLUMNS`, `REQUIRED_COLUMNS` e loaders para cada nova tabela.
   - manter padrao:
     - staging temp table
     - contagem de inserted/updated/ignored
     - upsert com `IS DISTINCT FROM`

### Adicionar

Novas tabelas raw (proposta minima para cobrir tudo listado):

1. `raw.competition_leagues`
   - grain: `(provider, league_source_id)`
2. `raw.competition_seasons`
   - grain: `(provider, season_source_id)`
3. `raw.competition_stages`
   - grain: `(provider, stage_source_id)`
4. `raw.competition_rounds`
   - grain: `(provider, round_source_id)`
5. `raw.standings_snapshots`
   - grain: `(provider, league_source_id, season_source_id, stage_source_id, round_source_id, team_source_id)`
6. `raw.fixture_lineups`
   - grain: `(fixture_id, team_id, player_id)`
7. `raw.fixture_player_statistics`
   - grain: `(fixture_id, team_id, player_id)`
8. `raw.player_season_statistics`
   - grain: `(season_id, team_id, player_id, stat_scope)` ou equivalente definido na API real
9. `raw.transfers`
   - grain: `(provider, transfer_source_id)` ou NK por jogador+data+clubes
10. `raw.player_availability`
   - grain: `(provider, player_source_id, status_type, start_date)`
11. `raw.coaches`
   - grain: `(provider, coach_source_id)`
12. `raw.coach_tenures`
   - grain: `(provider, coach_source_id, team_source_id, start_date)`
13. `raw.player_ratings`
   - grain: `(fixture_id, player_id)`
14. `raw.head_to_head_fixtures`
   - grain: `(provider, fixture_source_id)` ou reutilizar `fixture_id` canonico

Tambem e recomendado evoluir `raw.fixtures` com colunas novas:

1. `attendance`
2. `weather_*` (ex.: `weather_temp_c`, `weather_desc`, `weather_wind_kph`)
3. `referee_id`
4. `stage_id`, `round_id` (ids de estrutura)
5. `home_goals_ht`, `away_goals_ht`, `home_goals_ft`, `away_goals_ft`
6. `source_provider` (se optar por provider-aware na propria tabela)

### Remover (gradual)

1. Dependencia de tabela unica para conceitos diferentes.
2. Colunas sobrecarregadas sem semantica estavel.

Motivo:
Separar entidades por grain reduz duplicidade, simplifica upsert e evita null explosion.

---

## 4.6 dbt (`dbt/models/`)

### Modificar

1. `models/staging/sources.yml`
   - adicionar todas as novas fontes raw.
2. Staging atual:
   - preservar `stg_matches`, `stg_match_statistics`, `stg_match_events`.
   - criar novos `stg_*` para lineups/player stats/competition structure/context extras.
3. Marts:
   - adicionar fatos/dimensoes para analytics individual e contexto.

### Adicionar (camada staging)

1. `stg_competition_leagues.sql`
2. `stg_competition_seasons.sql`
3. `stg_competition_stages.sql`
4. `stg_competition_rounds.sql`
5. `stg_standings_snapshots.sql`
6. `stg_fixture_lineups.sql`
7. `stg_fixture_player_statistics.sql`
8. `stg_player_season_statistics.sql`
9. `stg_transfers.sql`
10. `stg_player_availability.sql`
11. `stg_coaches.sql`
12. `stg_coach_tenures.sql`
13. `stg_player_ratings.sql`
14. `stg_head_to_head_fixtures.sql`

### Adicionar (camada intermediate)

1. `int_fixture_player_context.sql`
   - join entre fixture_player_stats + lineups + events
2. `int_competition_round_calendar.sql`
   - round_key canonico por temporada
3. `int_player_form_window.sql`
   - agregacoes rolling para BI

### Adicionar (camada marts core)

1. `dim_round.sql`
2. `dim_stage.sql`
3. `dim_coach.sql`
4. `fact_fixture_player_stats.sql`
5. `fact_fixture_lineups.sql`
6. `fact_standings_snapshots.sql`

### Adicionar (camada marts analytics)

1. `player_match_summary.sql`
2. `player_season_summary.sql`
3. `player_90_metrics.sql`
4. `coach_performance_summary.sql`
5. `head_to_head_summary.sql`

### Remover (gradual)

1. Metrica individual inferida apenas por eventos quando existir fonte nativa de player stats.
2. Regras de negocio em dashboard SQL custom fora do dbt.

Motivo:
dbt precisa concentrar semantica de negocio e garantir reprodutibilidade.

---

## 4.7 Qualidade de dados (dbt + GE + SQL checks)

### Modificar

1. `dbt/models/**/schema.yml`
   - adicionar testes de grain para cada nova entidade.
2. `quality/great_expectations/checkpoints/raw_checkpoint.yml`
   - incluir validacoes para novas tabelas raw.
3. `quality/great_expectations/checkpoints/gold_marts_checkpoint.yml`
   - incluir fatos/dimensoes novos.
4. `infra/airflow/dags/data_quality_checks.py`
   - adicionar assertions de cobertura e duplicidade por entidade.

### Adicionar testes obrigatorios

1. **Unicidade de grain** em todas as tabelas novas.
2. **Not null** para chaves obrigatorias.
3. **Relationships**:
   - fixture scoped -> `raw.fixtures`
   - player scoped -> `dim_player` (na camada mart)
4. **Accepted ranges**:
   - minutos >= 0 (aceitando null quando regra exigir)
   - percentuais entre 0 e 100
5. **Cobertura minima por fixture**:
   - ex.: lineup com ao menos 11 titulares por time quando status final.

### Motivo

Sem quality gate por entidade nova, regressao silenciosa vira regra em backfills grandes.

---

## 4.8 Observabilidade e operacao

### Modificar

1. Reusar `StepMetrics` em todos os novos loaders e mappers.
2. Uniformizar logs com campos:
   - `provider`, `entity_type`, `scope_key`, `rows_in`, `rows_out`, `failures`, `cursor_before`, `cursor_after`.

### Adicionar

1. Tabela opcional `raw.ingestion_audit` para auditoria historica por run/entidade.
2. Artefatos de diagnostico por entidade em `artifacts/<run_id>/`.

### Motivo

Backfill volumoso sem telemetria detalhada fica cego para falhas parciais.

---

## 4.9 CI/CD e teste

### Modificar

1. Jobs de unit continuam separados de integration (ja iniciado no projeto).
2. Adicionar smoke tests por entidade nova:
   - mapper unit tests
   - grain tests dbt
3. Atualizar comandos agregadores:
   - manter `quality-p1`
   - evoluir `p2-verify` para incluir novas queries de coverage.

### Adicionar

1. `tools/quality_pX.py` dedicado para pacote expanded.
2. Testes de parse de DAG para novos arquivos.

---

## 4.10 Metabase e camada BI

### Modificar

1. Sincronizacao de schema no Metabase apos criacao dos novos marts.
2. Revisar modelos existentes para apontar para novos fatos individuais.

### Adicionar

1. Collections:
   - Player Analytics
   - Team Context
   - Competition Structure
2. Perguntas prontas:
   - "Shots by player by match"
   - "xG por jogador por rodada"
   - "Minutos jogados e contribuicoes por 90"
   - "Coach impact timeline"

---

## 5. Ordem recomendada de implementacao (com motivo)

## Fase 0 - Congelamento e baseline de seguranca

1. Snapshot de estado atual (row counts + duplicidade + cobertura).
2. Backups logicos do Postgres.
3. Congelar execucoes automaticas durante rollout estrutural.

Motivo:
Evitar mistura de codigo novo com carga antiga sem rastreio.

## Fase 1 - Fundacao de schema e contratos

1. Criar migrations para novas tabelas raw e colunas adicionais de fixtures.
2. Definir PK/UK/indices conforme grain.
3. Atualizar `docs/contracts/data_contracts.md`.

Motivo:
Sem contrato fisico pronto, qualquer ingestao vira schema drift.

## Fase 2 - Expansao de provider + ingestion bronze

1. Expandir `ProviderAdapter`.
2. Implementar metodos SportMonks no adapter.
3. Criar DAGs de ingestao Bronze para entidades novas com conf padronizado.
4. Integrar `provider_sync_state` nos novos fluxos de backfill.

Motivo:
Garantir captura auditavel e retomavel antes de transformar dados.

## Fase 3 - Mappers Silver e Load Raw

1. Implementar mappers por entidade.
2. Implementar loaders raw com staging + upsert idempotente.
3. Rodar backfill de lote pequeno para validacao.

Motivo:
Consolidar qualidade da camada operacional antes de mexer em marts.

## Fase 4 - dbt staging/intermediate/marts

1. Adicionar `stg_*` das novas fontes.
2. Construir intermediarios de consolidacao.
3. Entregar novos fatos/dimensoes e analytics.

Motivo:
Disponibilizar valor analitico de forma controlada e testavel.

## Fase 5 - Quality gates e monitoramento

1. Expandir dbt tests + GE + SQL assertions.
2. Adicionar queries de cobertura e duplicidade por dominio.
3. Automatizar artefatos para auditoria.

Motivo:
Sem qualidade automatizada, qualquer reprocessamento vira risco alto.

## Fase 6 - Go-live progressivo

1. Backfill historico por temporada.
2. Rodar validacoes de idempotencia (segunda execucao sem mudancas).
3. Habilitar dashboards no Metabase.

Motivo:
Entrar em producao sem big bang, reduzindo falha sistica.

---

## 6. Matriz de dados suportados e implementacao necessaria

### 6.1 Estrutura de competicao

Requisitos:
1. leagues
2. seasons
3. stages/rounds
4. standings completas

Implementacao:
1. Raw:
   - `competition_leagues`, `competition_seasons`, `competition_stages`, `competition_rounds`, `standings_snapshots`
2. dbt:
   - `dim_competition` evoluida + `dim_stage` + `dim_round` + `fact_standings_snapshots`
3. Qualidade:
   - unique por ids de estrutura e por snapshot grain

### 6.2 Fixtures completas

Requisitos:
1. cronograma historico/futuro
2. status detalhado
3. placar HT/FT
4. arbitragem
5. venue
6. attendance/weather quando disponivel

Implementacao:
1. Evoluir `raw.fixtures` com colunas extras.
2. Ajustar `fixtures_mapper.py` para preencher campos novos.
3. Ajustar `stg_matches.sql` e `int_matches_enriched.sql`.

### 6.3 Team match stats avancadas

Requisitos:
1. posse, finalizacoes, passes, cruzamentos, escanteios, faltas, cartoes, impedimentos, defesas, ataques perigosos, xG

Implementacao:
1. Evoluir `statistics_mapper.py` para mapear novas metricas.
2. Evoluir `raw.match_statistics` (ou criar `raw.match_team_statistics_v2`).
3. Atualizar `stg_match_statistics.sql` e fatos derivados.

### 6.4 Eventos detalhados

Requisitos:
1. gols com tipo
2. assistencias
3. cartoes
4. substituicoes
5. VAR
6. penalties perdidos

Implementacao:
1. Manter `raw.match_events`.
2. Expandir parser de `detail`/`type`.
3. Acrescentar colunas opcionais quando API fornecer `provider_event_id` e metadata de evento.

### 6.5 Lineups e minutos

Requisitos:
1. escalacao inicial
2. banco
3. substituicoes
4. posicao
5. formacao
6. minutos jogados

Implementacao:
1. Nova tabela `raw.fixture_lineups`.
2. Novo mapper `lineups_mapper.py`.
3. Novo fato `mart.fact_fixture_lineups`.
4. `player_90_metrics` usando minutos como denominador.

### 6.6 Player stats por jogo e temporada

Requisitos por jogo:
1. gols, assistencias, passes, passes-chave, finalizacoes, desarmes, interceptacoes, duelos, faltas, cartoes, defesas, clean sheets, xG individual, rating

Requisitos por temporada:
1. agregados, medias, minutos, titularidade, ranking

Implementacao:
1. Nova tabela `raw.fixture_player_statistics` (grain fixture/team/player).
2. Nova tabela `raw.player_season_statistics`.
3. Novos marts:
   - `fact_fixture_player_stats`
   - `player_match_summary`
   - `player_season_summary`

### 6.7 Extras (transferencias, lesoes/suspensoes, tecnicos, ratings, H2H)

Implementacao:
1. `raw.transfers`
2. `raw.player_availability` (lesao/suspensao)
3. `raw.coaches` + `raw.coach_tenures`
4. `raw.player_ratings` (separado quando necessario)
5. `raw.head_to_head_fixtures` e mart `head_to_head_summary`

---

## 7. Mapa de alteracoes por pasta/arquivo

## 7.1 Arquivos existentes que precisam mudar

1. `infra/airflow/dags/common/providers/base.py`
2. `infra/airflow/dags/common/providers/sportmonks.py`
3. `infra/airflow/dags/common/providers/registry.py`
4. `infra/airflow/dags/common/services/ingestion_service.py`
5. `infra/airflow/dags/common/services/mapping_service.py`
6. `infra/airflow/dags/common/services/warehouse_service.py`
7. `infra/airflow/dags/pipeline_brasileirao.py`
8. `dbt/models/staging/sources.yml`
9. `dbt/models/staging/stg_matches.sql`
10. `dbt/models/staging/stg_match_statistics.sql`
11. `dbt/models/staging/stg_match_events.sql`
12. `dbt/models/marts/core/schema.yml`
13. `dbt/models/marts/analytics/schema.yml`
14. `quality/great_expectations/checkpoints/raw_checkpoint.yml`
15. `quality/great_expectations/checkpoints/gold_marts_checkpoint.yml`
16. `infra/airflow/dags/data_quality_checks.py`
17. `docs/contracts/data_contracts.md`
18. `docs/ARCHITECTURE.md`
19. `README.md`
20. `docker-compose.yml` (se novos env vars forem introduzidos)

## 7.2 Novos arquivos esperados

1. DAGs:
   - `infra/airflow/dags/ingest_lineups_bronze.py`
   - `infra/airflow/dags/ingest_fixture_player_statistics_bronze.py`
   - `infra/airflow/dags/ingest_player_season_statistics_bronze.py`
   - `infra/airflow/dags/ingest_competition_structure_bronze.py`
   - etc.
2. Mappers:
   - `infra/airflow/dags/common/mappers/lineups_mapper.py`
   - `infra/airflow/dags/common/mappers/fixture_player_statistics_mapper.py`
   - etc.
3. dbt:
   - novos `stg_*`, `int_*`, `fact_*`, `dim_*`, `analytics_*`
4. Migrations:
   - novos `.sql` em `db/migrations/` para cada bloco de schema
5. Queries de diagnostico:
   - `warehouse/queries/<entity>_missing.sql`
   - `warehouse/queries/<entity>_duplicates.sql`
   - `warehouse/queries/<entity>_coverage.sql`

## 7.3 O que remover/descontinuar

1. Endpoints/chamadas paralelas fora do provider canonico.
2. Scripts ad-hoc de ingestao sem `sync_state`.
3. Dependencia de mart legado para dados novos (tudo novo deve entrar via dbt).

---

## 8. Tutorial detalhado: como executar a nova ingestao apos a arquitetura nova

Importante: os comandos abaixo sao **PowerShell-friendly**.

## 8.1 Pre-requisitos

1. .env atualizado com chaves e defaults corretos.
2. Containers ativos (postgres, minio, irflow-webserver, irflow-scheduler).
3. Migrations novas aplicadas.
4. DAGs novas presentes e sem erro de parse.

### Exemplo de variaveis no .env (bloco novo sugerido)

`env
ACTIVE_PROVIDER=sportmonks
SPORTMONKS_DEFAULT_LEAGUE_ID=648
SPORTMONKS_DEFAULT_SEASON=2024
INGEST_FIXTURES_REQUESTS_PER_MINUTE=0
INGEST_STATISTICS_REQUESTS_PER_MINUTE=0
INGEST_EVENTS_REQUESTS_PER_MINUTE=0
INGEST_LINEUPS_REQUESTS_PER_MINUTE=0
INGEST_PLAYER_STATS_REQUESTS_PER_MINUTE=0
INGEST_TRANSFERS_REQUESTS_PER_MINUTE=0
INGEST_INJURIES_REQUESTS_PER_MINUTE=0
INGEST_SUSPENSIONS_REQUESTS_PER_MINUTE=0
INGEST_COACHES_REQUESTS_PER_MINUTE=0
INGEST_H2H_REQUESTS_PER_MINUTE=0
API_KEY_SPORTMONKS=<sua_chave>
SPORTMONKS_BASE_URL=https://api.sportmonks.com/v3/football
`

## 8.2 Aplicar migracoes

`powershell
make db-up
`

Validar status:

`powershell
make db-status
`

## 8.3 Reiniciar servicos Airflow para carregar env + DAGs

`powershell
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
docker compose ps
`

## 8.4 Executar backfill da estrutura de competicao

Exemplo usando wrapper Python para evitar erro de quoting JSON no PowerShell:

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_competition_structure_bronze", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
`

Depois rode Silver e Raw correspondentes:

`powershell
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_competition_structure 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_competition_structure 2026-02-19
`

## 8.5 Executar backfill de fixtures completas

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_brasileirao_2024_backfill", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
`

`powershell
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_fixtures_backfill 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_fixtures 2026-02-19
`

## 8.6 Executar team stats e eventos

Team stats:

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_statistics_bronze", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_statistics 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_statistics 2026-02-19
`

Eventos:

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_match_events_bronze", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_match_events 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_match_events 2026-02-19
`

## 8.7 Executar lineups e player stats (novo)

Lineups:

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_lineups_bronze", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_lineups 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_lineups 2026-02-19
`

Player stats por partida:

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_fixture_player_statistics_bronze", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_fixture_player_statistics 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_fixture_player_statistics 2026-02-19
`

Player stats por temporada:

`powershell
@'
import json
import subprocess
conf = json.dumps({
    "mode": "backfill",
    "provider": "sportmonks",
    "league_id": 648,
    "season": 2024
})
subprocess.run(
    ["airflow", "dags", "test", "ingest_player_season_statistics_bronze", "2026-02-19", "-c", conf],
    check=True,
)
'@ | docker compose exec -T airflow-scheduler python -
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_player_season_statistics 2026-02-19
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_player_season_statistics 2026-02-19
`

## 8.8 Executar extras (transferencias, disponibilidade, tecnicos, h2h)

Padrao identico:

1. ingest_<entity>_bronze
2. ronze_to_silver_<entity>
3. silver_to_postgres_<entity>

Sempre com mode=backfill no primeiro ciclo historico.

## 8.9 Rodar dbt para materializar marts novos

`powershell
docker compose exec -T airflow-webserver dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
docker compose exec -T airflow-webserver dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
docker compose exec -T airflow-webserver dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
`

Para foco em player analytics:

`powershell
docker compose exec -T airflow-webserver dbt run -s stg_fixture_player_statistics fact_fixture_player_stats player_match_summary --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
docker compose exec -T airflow-webserver dbt test -s stg_fixture_player_statistics fact_fixture_player_stats player_match_summary --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
`

## 8.10 Rodar quality gates

`powershell
docker compose exec -T airflow-webserver airflow dags test great_expectations_checks 2026-02-19
docker compose exec -T airflow-webserver airflow dags test data_quality_checks 2026-02-19
`

## 8.11 Validacao de idempotencia (obrigatoria)

1. Rode novamente o mesmo backfill.
2. Verifique logs:
   - esperado: grande volume de ignoradas e/ou tualizadas=0 quando sem mudanca.
3. Rode queries de duplicidade:
   - zero duplicatas no grain.

Exemplo (player stats, tabela futura):

`sql
select fixture_id, team_id, player_id, count(*)
from raw.fixture_player_statistics
group by 1,2,3
having count(*) > 1;
`

Resultado esperado:   linhas.

## 8.12 Verificacao analitica final

Pergunta alvo:
"Arrascaeta deu 2 chutes contra o Fluminense?"

Com act_fixture_player_stats implementada, a consulta fica direta:

`sql
select
  m.match_id,
  m.date_day,
  p.player_name,
  t.team_name,
  fps.shots_total
from mart.fact_fixture_player_stats fps
join mart.dim_player p on p.player_sk = fps.player_sk
join mart.dim_team t on t.team_sk = fps.team_sk
join mart.fact_matches m on m.match_id = fps.match_id
where p.player_name ilike '%Arrascaeta%'
  and (
    m.home_team_name ilike '%Fluminense%'
    or m.away_team_name ilike '%Fluminense%'
  )
order by m.date_day desc;
`

---

## 9. Como evitar duplicacao entre providers (api-football x sportmonks)

## 9.1 Regra de ouro

Todo registro deve carregar origem explicita:

1. source_provider
2. source_id
3. canonical_id (via aw.provider_entity_map quando aplicavel)

## 9.2 Estrategia recomendada

1. No bootstrap SportMonks:
   - priorizar SportMonks como origem ativa.
2. Durante migracao:
   - limpar recorte legado API-Football quando necessario (como ja feito para Brasileirao 2024).
3. Longo prazo:
   - mapear entidades equivalentes em provider_entity_map.
   - usar canonical_id como chave de referencia em marts.

## 9.3 Beneficio

Elimina duplicidade sem sacrificar rastreabilidade da origem.

---

## 10. Checklist de aceite por etapa

## Etapa arquitetura

1. Migrations aplicadas sem erro.
2. Tabelas novas com PK/UK e indices criados.
3. Contratos atualizados em docs.

## Etapa ingestao

1. Backfill por dominio executa com logs de progresso.
2. provider_sync_state atualizado por entidade/scope.
3. Reexecucao nao duplica dados.

## Etapa transformacao

1. dbt run completo.
2. dbt test sem falhas.
3. GE checkpoints sem falhas.

## Etapa analitica

1. Query por jogador por partida retorna resultados consistentes.
2. Dashboards no Metabase conseguem consumir marts novos.

---

## 11. Plano de rollback

1. Manter migrations additive-first (sem drop imediato).
2. Se falhar ingestao nova:
   - desabilitar DAGs novas no pipeline.
   - voltar para fluxo atual (fixtures/stats/events) sem perda.
3. Se falhar dbt novo:
   - manter marts antigas ativas.
4. Se falhar no BI:
   - restaurar export anterior de colecoes do Metabase.

---

## 12. Conclusao executiva

Para suportar tudo que voce listou do SportMonks Advanced com seguranca, o projeto precisa evoluir de "3 entidades principais" para "arquitetura por dominio", mantendo os pilares que ja funcionam hoje:

1. upsert idempotente,
2. controle de progresso por sync_state,
3. transformacao dbt-first,
4. quality gate formal.

A ordem proposta minimiza risco:

1. primeiro contrato e schema,
2. depois coleta Bronze/Raw,
3. depois modelagem dbt,
4. por fim backfill completo + BI.

Com isso implementado, o sistema passa a responder analises individuais de jogador por partida/temporada, incluindo minutos por 90, xG individual, rating e contexto de disponibilidade (lesao/suspensao), sem perder governanca operacional.
