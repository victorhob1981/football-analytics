# Liga Portugal / Primeira Liga Ingestion Plan

## 0. Status Operacional Atual

Snapshot validado em `2026-03-28`:
- `2023_24` (`provider_season_id=21825`) e `2024_25` (`provider_season_id=23793`) estao com baseline operacional completo no escopo canonico do projeto;
- classificacao vigente para esse recorte:
  - `COMPLETA_NO_ESCOPO_CANONICO_DAS_2_SEASONS`
- a frente de reconciliacao de `raw.player_season_statistics -> mart.player_season_summary` foi encerrada sem regressao relevante no mart compartilhado;
- esse baseline de 2 seasons deve ser tratado como congelado:
  - nao reabrir ingestao;
  - nao reabrir dbt/marts;
  - nao reabrir BFF/frontend/assets;
  - so voltar a mexer nesse dominio se surgir regressao objetiva.

Pendencias remanescentes da Liga Portugal:
- `2020_21`
- `2021_22`
- `2022_23`

Essas 3 seasons continuam fora da expansao por ausencia de `provider_season_id` validado. A proxima frente valida para a competicao e apenas `season_id recovery`, sem tocar no baseline ja verde.

## 1. Objetivo

Este documento consolida o plano oficial de ingestao da Liga Portugal / Primeira Liga no escopo ja suportado pelo projeto, usando `provider=sportmonks` e `provider_league_id=462`.

Objetivo do plano:
- fechar a versao final de planejamento, pronta para virar execucao futura em blocos;
- limitar o escopo ao que o projeto ja suporta ou ja documenta;
- evitar improvisacao em onboarding, seasons, gates, validacoes e criterio de aceite;
- preservar a regra de parar no primeiro blocker real durante a execucao futura.

Este documento nao executa ingestao, nao inicia DAGs, nao cadastra nada em `control.*` e nao altera pipeline, BFF ou frontend.

## 2. Contexto do Projeto

### 2.1 Base documental e tecnica usada

Base principal:
- `docs/INVENTARIO_DADOS_DO_PROJETO.md`
- `docs/DATA_COVERAGE_AUDIT_20260320.md`
- `docs/MART_FRONTEND_BFF_CONTRACTS.md`
- `docs/BFF_API_CONTRACT.md`
- `docs/VISUAL_ASSETS_INGESTION.md`
- `infra/airflow/dags/pipeline_brasileirao.py`
- `infra/airflow/dags/common/services/ingestion_service.py`
- `infra/airflow/dags/common/services/warehouse_service.py`
- `infra/airflow/dags/data_quality_checks.py`
- `api/src/core/context_registry.py`
- `frontend/src/config/competitions.registry.ts`
- `frontend/src/config/seasons.registry.ts`
- `dbt/models/control/competition_season_config.sql`

### 2.2 Principios que orientam este plano

- nao inventar feature nova;
- preservar o que ja esta verde;
- validar o baseline antes dos derivados;
- tratar cobertura parcial do provider como fato operacional, nao como motivo para ampliar escopo;
- usar contratos canonicos explicitos entre warehouse, API e frontend;
- separar gate real de fechamento de cobertura desejavel.

### 2.3 Estado atual consolidado do projeto

Escopo real ja suportado para competicoes:
- estrutura competitiva: competition, seasons, stages, rounds;
- standings: snapshots e derivados;
- fixtures: status, venue, placares, attendance, referee, weather quando houver;
- profundidade de partida: match statistics, match events, lineups, fixture player statistics;
- profundidade sazonal: player season statistics;
- contexto adicional: head to head, team coaches, team sidelined, player transfers;
- marts e superficies de consumo: matches, standings, teams, players, rankings, search.

Escopo estrutural de copa/hibrido ja existe no projeto, mas nao entra no baseline da Primeira Liga:
- groups;
- ties;
- progression;
- rotas e marts de competicoes cup/hybrid.

## 3. Premissas e Convencoes

### 3.1 Convencoes fixas deste plano

- provider: `sportmonks`
- provider league id: `462`
- tipo da competicao: liga domestica regular
- calendario: `split_year`
- janela alvo: ultimos 5 campeonatos fechados validos no projeto
- politica de execucao futura: parar no primeiro blocker real

### 3.2 Convencoes de labels

Convencao recomendada e alinhada ao projeto:

| Item | Convencao | Exemplo 2024/25 | Uso principal |
|---|---|---|---|
| `seasonLabel` | `YYYY/YYYY+1` | `2024/2025` | exibicao e contrato humano |
| `catalogLabel` | `YYYY_YY` | `2024_25` | catalogo e referencia interna quando aplicavel |
| `queryId` | `YYYY` | `2024` | API, filtros e contexto split-year |

Observacoes:
- o projeto ja usa `queryId` como ano inicial da season split-year;
- `warehouse_service.py` consome explicitamente `season_label` e `provider_season_id`;
- `catalogLabel` deve ser usado como convencao de catalogo, mas so deve ser persistido em tabela se o schema existente tiver coluna para isso.

### 3.3 Regra das 5 seasons

Seasons candidatas iniciais deste plano:
- `2020/2021`
- `2021/2022`
- `2022/2023`
- `2023/2024`
- `2024/2025`

Estas 5 seasons so entram na execucao futura se o Bloco 0 provar:
- resolucao valida para `provider_season_id`;
- season fechada;
- coerencia entre label, datas e catalogo.

## 4. Escopo da Liga Portugal

### 4.1 Matriz de dominio e prioridade

| Dominio | Entidade / dado | Projeto suporta hoje? | Aplica a Liga Portugal? | Prioridade | Observacao |
|---|---|---:|---:|---|---|
| Catalogo | competicao canonica | parcial | sim | obrigatorio | `462` ainda nao esta no catalogo atual |
| Catalogo | seasons split-year | parcial | sim | obrigatorio | depende do Bloco 0 |
| Estrutura | leagues, seasons, stages, rounds | sim | sim | obrigatorio | baseline de liga |
| Standings | snapshots e derivados | sim | sim | obrigatorio | baseline de standings |
| Fixtures | fixture core, status, venue, placares | sim | sim | obrigatorio | baseline de liga |
| Fixtures | attendance, weather, referee | parcial | sim | desejavel | nao bloqueia baseline |
| Identidade | teams e venues | sim | sim | obrigatorio | fecha com fixtures/standings |
| Partida | match statistics | sim | sim | obrigatorio | match center baseline |
| Partida | match events | sim | sim | obrigatorio | timeline baseline |
| Partida | lineups | sim | sim | obrigatorio | necessario para camada de jogador |
| Partida | fixture player statistics | sim | sim | obrigatorio | necessario para camada de jogador |
| Sazonal | player season statistics | sim | sim | obrigatorio | depende de lineups + FPS |
| Contexto | head to head | sim | sim | desejavel | eleva para padrao completo |
| Contexto | team coaches | sim | sim | desejavel | nao bloqueia MVP |
| Contexto | team sidelined | sim | sim | desejavel | nao bloqueia MVP |
| Contexto | player transfers | sim | sim | desejavel | nao bloqueia MVP |
| Analytics | league, team, player marts | parcial | sim | obrigatorio | baseline de consumo |
| Consumo | matches, standings, teams, players, rankings, search | sim | sim | obrigatorio | baseline BFF/frontend |
| Assets | competition logo, club logos, player photos | sim | sim | opcional | nao bloqueia MVP |
| Cup/hybrid | groups, ties, progression | parcial | nao | nao aplicavel | fora do baseline da liga |
| Incerto | pitch | nao evidenciado | incerto | nao aplicavel | nao esta suportado no inventario atual |
| Incerto | insights e pass-accuracy dedicados | parcial | incerto | opcional | nao usar como gate |

### 4.2 Baseline, completo e oportunistico

Baseline MVP da Liga Portugal:
- catalogo e seasons resolvidos;
- estrutura competitiva;
- standings;
- fixtures;
- teams e venues;
- match statistics;
- match events;
- lineups;
- fixture player statistics;
- player season statistics;
- marts core;
- BFF baseline para liga domestica.

Escopo completo:
- baseline MVP;
- head to head;
- team coaches;
- team sidelined;
- player transfers;
- assets visuais.

Escopo oportunistico:
- elevar cobertura de attendance, weather e referee;
- elevar cobertura de fotos de jogadores;
- qualquer melhoria nao necessaria para MVP ou padrao completo.

## 5. Decisoes de Modelagem

### 5.1 Competicao canonica recomendada

Recomendacao final:

| Item | Valor recomendado | Justificativa |
|---|---|---|
| `competition_key` | `primeira_liga` | consistente com chaves atuais em snake_case |
| `provider` | `sportmonks` | provider do projeto para esta ingestao |
| `provider_league_id` | `462` | id alvo declarado |
| `competition_type` | `league` | liga domestica regular |
| `seasonCalendar` | `split_year` | alinhado as ligas europeias no projeto |

Observacao:
- `competition_key=primeira_liga` e a recomendacao final deste plano;
- se existir colisao real com chave ja usada no ambiente, isso vira blocker do Bloco 1.

### 5.2 Registros esperados em control

Os registros abaixo sao o contrato minimo esperado. Se o schema real das tabelas tiver colunas adicionais obrigatorias, o preenchimento final deve seguir o schema existente no ambiente, sem alterar a semantica abaixo.

#### `control.competitions`

Registro minimo esperado:
- `competition_key='primeira_liga'`
- `competition_type='league'`

Campos adicionais, se o schema exigir:
- nome de exibicao;
- slug;
- ordenacao;
- metadados de logo ou regiao.

#### `control.competition_provider_map`

Registro minimo esperado:
- `provider='sportmonks'`
- `provider_league_id=462`
- `competition_key='primeira_liga'`

#### `control.season_catalog`

Cinco registros minimos esperados, um por season:
- `provider='sportmonks'`
- `competition_key='primeira_liga'`
- `season_label`
- `provider_season_id`
- `season_start_date`
- `season_end_date`

Campos adicionais, se existirem no schema:
- `season_name`
- `catalog_label`
- flags operacionais de ativacao ou ordenacao

### 5.3 Contrato final entre warehouse, API e frontend

Contrato recomendado:

| Camada | Identidade canonica esperada |
|---|---|
| Warehouse | `provider + competition_key + season_label + provider_season_id` |
| API | `competitionId='462'` no catalogo canonico e `seasonId=queryId` |
| Frontend | exibir `seasonLabel`, trafegar `competitionId='462'` e `queryId` |

Implicacoes:
- warehouse resolve facts e marts por `competition_key` e `provider_season_id`;
- API traduz `competitionId` e `seasonId` para o mesmo universo de seasons do warehouse;
- frontend nao inventa rotulos de season fora do contrato split-year.

Arquivos do projeto que devem refletir este contrato na execucao futura:
- `api/src/core/context_registry.py`
- `frontend/src/config/competitions.registry.ts`
- `frontend/src/config/seasons.registry.ts` somente se o comportamento generico nao cobrir o caso

Precheck importante:
- nesta rodada nao foi confirmada DDL versionada clara para `control.competitions`, `control.competition_provider_map` e `control.season_catalog`;
- antes de qualquer escrita futura, o ambiente precisa comprovar existencia e schema real dessas tabelas.

### 5.4 Absorcao formal do antigo Bloco 3

O antigo Bloco 3 nao existe como bloco independente por tres motivos:
- nao ha DAG de master data autonomo para teams, venues ou players;
- `dim_team` e `dim_venue` surgem a partir de fixtures/standings/fact_matches;
- `dim_player` so fecha depois de lineups, fixture player statistics e player season statistics.

Redistribuicao formal:
- entra no Bloco 2: identidade estrutural de `team` e `venue`, participantes da competicao e consistencia entre standings e fixtures;
- entra no Bloco 5: identidade e enriquecimento de `player`, fechamento de `dim_player`, acoplado a player season statistics.

Consequencia pratica para execucao futura:
- nao abrir uma task separada de "cadastro de elenco";
- validar `team/venue` ao fechar o Bloco 2;
- validar `player` ao fechar o Bloco 5;
- reduzir um bloco artificial que nao teria gate tecnico proprio.

## 6. Blocos Operacionais

### Bloco 0 - Diagnostico e resolucao de seasons

**Objetivo operacional**
- fechar a matriz das 5 seasons validas da Primeira Liga para o projeto.

**Granularidade operacional**
- executar como bloco unico para as 5 seasons;
- a saida e uma matriz fechada, nao cinco decisoes soltas.

**Entradas**
- `provider=sportmonks`
- `provider_league_id=462`
- convencao split-year do projeto
- leitura atual de `control.season_catalog`, `raw.fixtures` e catalogos de contexto

**Artefatos / tabelas / jobs**
- consultas read-only
- sem DAG de carga
- artefato principal: matriz de resolucao de season

**Saidas esperadas**
- 5 seasons candidatas aprovadas ou blocker formalizado
- `seasonLabel`, `catalogLabel`, `queryId`, `provider_season_id`, datas e status de season fechada

**Validacoes e evidencia minima**
- 1:1 entre season e `provider_season_id`
- zero ambiguidade de split-year
- datas coerentes com o label
- evidencia minima: tabela de resolucao consolidada

**Gate real de fechamento**
- todas as 5 seasons resolvidas de forma canonica ou impossibilidade comprovada

**Cobertura desejavel nao bloqueante**
- ja estimar cobertura historica provavel por season

**Blocker real**
- qualquer season sem `provider_season_id` confiavel
- qualquer ambiguidades entre `queryId`, `seasonLabel` e datas

**Risco monitoravel**
- necessidade de descartar 1 ou mais seasons e reduzir a janela historica efetiva

### Bloco 1 - Onboarding canonico da competicao

**Objetivo operacional**
- fechar o contrato canonico da competicao e das seasons antes de qualquer carga.

**Granularidade operacional**
- modelo hibrido:
- competicao e provider map sao cadastro unico;
- season catalog e pacote de 5 registros;
- tudo deve ser aprovado como lote semantico unico.

**Entradas**
- matriz aprovada do Bloco 0
- recomendacao canonica: `competition_key=primeira_liga`

**Artefatos / tabelas / jobs**
- `control.competitions`
- `control.competition_provider_map`
- `control.season_catalog`
- `api/src/core/context_registry.py`
- `frontend/src/config/competitions.registry.ts`
- `frontend/src/config/seasons.registry.ts` se necessario
- sem DAG de carga; este bloco fecha contrato e payload

**Saidas esperadas**
- payload unico de onboarding
- definicao final de `competition_key`, labels e ids
- diff esperado de registries e cadastros

**Validacoes e evidencia minima**
- o mesmo contrato resolve a competicao em warehouse, API e frontend
- `competition_key`, `provider_league_id` e labels nao entram em conflito com o catalogo atual
- evidencia minima: tabela de cadastro e matriz de reflexo por camada

**Gate real de fechamento**
- onboarding canonico sem ambiguidade de chave, id ou labels

**Cobertura desejavel nao bloqueante**
- documentar campos adicionais nao minimos das tabelas `control.*`

**Blocker real**
- colisao de `competition_key`
- schema real de `control.*` divergente do minimo assumido
- impossibilidade de representar a season split-year no contrato atual

**Risco monitoravel**
- necessidade de pequenos ajustes em `seasons.registry.ts` caso a logica generica nao cubra o caso

### Bloco 2 - Estrutura competitiva, fixtures, standings e identidade estrutural

**Objetivo operacional**
- carregar o baseline estrutural da liga e fechar a identidade de `team` e `venue`.

**Granularidade operacional**
- modelo hibrido:
- `competition structure` pode ser operada em lote;
- `standings` e `fixtures` devem ser executados season a season, preferencialmente da mais recente para a mais antiga;
- validar uma season antes de liberar a proxima.

**Entradas**
- Bloco 0 fechado
- Bloco 1 fechado

**Artefatos / tabelas / jobs**
- bronze/silver/raw:
  - `competition_leagues`
  - `competition_seasons`
  - `competition_stages`
  - `competition_rounds`
  - `standings_snapshots`
  - `fixtures`
- mart:
  - `fact_matches`
  - `fact_standings_snapshots`
  - `dim_team`
  - `dim_venue`
  - `league_summary`
- DAGs:
  - `ingest_competition_structure_bronze`
  - `bronze_to_silver_competition_structure`
  - `silver_to_postgres_competition_structure`
  - `ingest_standings_bronze`
  - `bronze_to_silver_standings`
  - `silver_to_postgres_standings`
  - `ingest_brasileirao_2024_backfill`
  - `bronze_to_silver_fixtures_backfill`
  - `silver_to_postgres_fixtures`

**Saidas esperadas**
- 5 seasons com estrutura, standings e fixtures carregados
- `dim_team` e `dim_venue` utilizaveis
- facts base de partida criadas

**Validacoes e evidencia minima**
- row count por season em `raw.fixtures` e `raw.standings_snapshots`
- fixtures sem `outside_catalog`
- standings reconciliadas com participantes dos fixtures
- `stage_id` e `round_id` coerentes
- `dbt run/test` dos modelos base afetados
- evidencia minima: planilha de counts por season + amostras de standings e fixtures

**Gate real de fechamento**
- baseline estrutural completo para as 5 seasons
- identidade de teams e venues suficiente para consumo

**Cobertura desejavel nao bloqueante**
- completude alta de venue metadata
- coverage de `attendance`, `weather` e `referee`

**Blocker real**
- fixtures fora do catalogo canonico
- standings ou fixtures sem season semantics confiavel
- ausencia de estrutura minima por season

**Risco monitoravel**
- gaps pontuais de venue ou round nao impedirem o baseline, mas precisarem de classificacao

### Bloco 3 - Absorvido

**Status operacional**
- nao existe como bloco independente.

**O que entra no Bloco 2**
- identidade estrutural de `team`
- identidade estrutural de `venue`
- reconciliacao de participantes entre standings e fixtures

**O que entra no Bloco 5**
- identidade de `player`
- enriquecimento de `dim_player`
- consistencia entre participacao em partida e consolidacao sazonal

**Consequencia pratica**
- nao abrir task de execucao dedicada para "elenco base";
- fechar identidade de team/venue junto do baseline estrutural;
- fechar identidade de player junto da camada sazonal.

### Bloco 4 - Profundidade de partida

**Objetivo operacional**
- fechar o match center baseline da Primeira Liga.

**Granularidade operacional**
- season a season;
- dentro de cada season, processar todos os fixtures elegiveis;
- so avancar para a season seguinte quando a season corrente estiver validada.

**Entradas**
- fixtures finalizados e semanticamente resolvidos do Bloco 2

**Artefatos / tabelas / jobs**
- raw:
  - `match_statistics`
  - `match_events`
  - `fixture_lineups`
  - `fixture_player_statistics`
- mart:
  - `fact_match_events`
  - `fact_fixture_lineups`
  - `fact_fixture_player_stats`
- caminho principal do pipeline:
  - `ingest_fixture_enrichments_bronze`
  - `bronze_to_silver_statistics`
  - `silver_to_postgres_statistics`
  - `bronze_to_silver_match_events`
  - `silver_to_postgres_match_events`
  - `bronze_to_silver_lineups`
  - `silver_to_postgres_lineups`
  - `bronze_to_silver_fixture_player_statistics`
  - `silver_to_postgres_fixture_player_statistics`

**Saidas esperadas**
- timeline, lineups, team stats e player stats carregados para os fixtures elegiveis
- fatos de profundidade de partida materializados

**Validacoes e evidencia minima**
- zero orfaos criticos por `fixture_id`
- zero duplicidade nas chaves naturais esperadas
- coerencia entre home/away e estatisticas
- materializacao verde dos fatos em mart
- evidencia minima: matriz de cobertura por secao do match center e contagem por season

**Gate real de fechamento**
- match center baseline utilizavel para a competicao

**Cobertura desejavel nao bloqueante**
- cobertura perfeita de todas as partidas
- completude total de cada secao em 100% dos fixtures

**Blocker real**
- ausencia sistemica de `match_events`, `fixture_lineups` ou `fixture_player_statistics` que inviabilize o baseline de match detail

**Risco monitoravel**
- cobertura parcial do provider em alguns fixtures historicos

### Bloco 5 - Profundidade sazonal de jogador e extras de elenco

**Objetivo operacional**
- fechar a camada de jogador e incorporar extras nao-MVP.

**Granularidade operacional**
- modelo hibrido:
- `player_season_statistics` deve ser executado season a season;
- `team_coaches`, `team_sidelined` e `player_transfers` podem rodar como complementos por season ou por conjunto de ids apos estabilizacao das entidades.

**Entradas**
- Bloco 4 fechado
- lineups e fixture player statistics validos

**Artefatos / tabelas / jobs**
- raw:
  - `player_season_statistics`
  - `team_coaches`
  - `team_sidelined`
  - `player_transfers`
- mart:
  - `player_match_summary`
  - `player_season_summary`
  - `player_90_metrics`
  - `dim_player`
- DAGs:
  - `ingest_player_season_statistics_bronze`
  - `bronze_to_silver_player_season_statistics`
  - `silver_to_postgres_player_season_statistics`
  - `ingest_team_coaches_bronze`
  - `bronze_to_silver_team_coaches`
  - `silver_to_postgres_team_coaches`
  - `ingest_team_sidelined_bronze`
  - `bronze_to_silver_team_sidelined`
  - `silver_to_postgres_team_sidelined`
  - `ingest_player_transfers_bronze`
  - `bronze_to_silver_player_transfers`
  - `silver_to_postgres_player_transfers`

**Saidas esperadas**
- camada sazonal de jogador fechada
- `dim_player` utilizavel
- extras de elenco carregados quando houver cobertura

**Validacoes e evidencia minima**
- `player_season_statistics` reconciliado com lineups e FPS
- materializacao verde dos marts de jogador
- players resolvidos por season sem colapso de identidade
- evidencia minima: contagens por season, taxa de resolucao de player e amostras de rankings

**Gate real de fechamento**
- camada de jogador utilizavel para `/players`, `/teams` e rankings baseline

**Cobertura desejavel nao bloqueante**
- completude de `team_coaches`, `team_sidelined` e `player_transfers`

**Blocker real**
- impossibilidade de fechar `player_season_statistics`
- impossibilidade de estabilizar `dim_player`

**Risco monitoravel**
- cobertura desigual dos extras de elenco

### Bloco 6 - Derivados, rankings e superficies de consumo

**Objetivo operacional**
- ligar a competicao aos marts derivados e as superficies baseline ja suportadas.

**Granularidade operacional**
- modelo hibrido:
- marts core devem ser validados season a season;
- smoke de BFF e frontend deve ser feito no conjunto completo das 5 seasons;
- `head_to_head` pode ser incorporado como aprofundamento dentro do proprio bloco.

**Entradas**
- Bloco 2 fechado
- Bloco 4 fechado
- Bloco 5 fechado

**Artefatos / tabelas / jobs**
- raw:
  - `head_to_head_fixtures`
- mart:
  - `league_summary`
  - `team_monthly_stats`
  - `head_to_head_summary`
  - `coach_performance_summary`
  - marts e consultas que suportam rankings e busca
- DAGs:
  - `ingest_head_to_head_bronze`
  - `bronze_to_silver_head_to_head`
  - `silver_to_postgres_head_to_head`
  - `dbt_run`

**Saidas esperadas**
- superfices baseline de liga operando com a Primeira Liga:
  - `/matches`
  - `/matches/{id}`
  - `/standings`
  - `/teams`
  - `/players`
  - `/rankings`
  - `/search`

**Validacoes e evidencia minima**
- `dbt run/test` verde para marts afetados
- smoke de rotas baseline do BFF
- coerencia entre `fact_matches`, `league_summary` e os derivados de jogador
- evidencia minima: matriz de smoke por rota e season

**Gate real de fechamento**
- Primeira Liga aparece e responde no mesmo baseline funcional das outras ligas domesticas

**Cobertura desejavel nao bloqueante**
- `head_to_head_summary`
- `coach_performance_summary`

**Blocker real**
- quebra das superficies baseline
- marts core inconsistentes com os fatos de base

**Risco monitoravel**
- tentar expandir este bloco para escopo cup/hybrid ou insights nao estabilizados

### Bloco 7 - Ativos visuais

**Objetivo operacional**
- completar logos e fotos de forma incremental e sem contaminar o baseline de dados.

**Granularidade operacional**
- modelo hibrido:
- competition logo e club logos podem ser tratados em lote apos o Bloco 2;
- player photos entram de forma incremental apos o Bloco 5.

**Entradas**
- competicao resolvida
- teams resolvidos
- players resolvidos, se houver fotos de players no escopo

**Artefatos / tabelas / jobs**
- manifests e rotinas descritas em `docs/VISUAL_ASSETS_INGESTION.md`
- sem dependencia de DAG core do pipeline

**Saidas esperadas**
- competition logo
- club logos
- player photos quando houver cobertura

**Validacoes e evidencia minima**
- manifesto por categoria
- taxa de cobertura por ativo
- paths ou referencias validas

**Gate real de fechamento**
- este bloco nao e gate de MVP

**Cobertura desejavel nao bloqueante**
- maior cobertura possivel de fotos de jogadores

**Blocker real**
- so existe se o objetivo declarado da onda for fechar escopo completo com assets

**Risco monitoravel**
- cobertura parcial de player photos

### Bloco 8 - Validacao final, cobertura e aceite

**Objetivo operacional**
- provar que a Liga Portugal foi integrada; nao apenas que jobs rodaram.

**Granularidade operacional**
- executar como fechamento global das 5 seasons, mantendo checklist por season.

**Entradas**
- todos os blocos do caminho escolhido concluidos

**Artefatos / tabelas / jobs**
- `dbt_run`
- `great_expectations_checks`
- `data_quality_checks`
- consultas SQL de auditoria e reconciliacao
- smoke final de BFF/frontend

**Saidas esperadas**
- pacote final de aceite com status por season e por dominio

**Validacoes e evidencia minima**
- contagens por season nas tabelas raw e marts relevantes
- coerencia raw -> mart
- ausencia de orfaos criticos
- ausencia de duplicidade critica
- contracts baseline de API/BFF preservados
- gaps classificados como `PROVIDER_COVERAGE_GAP` quando aplicavel
- evidencia minima: checklist final preenchido e aprovado

**Gate real de fechamento**
- cada season classificada como:
  - `COMPLETA`, ou
  - `PARCIAL JUSTIFICADA`
- sem gap funcional nao classificado

**Cobertura desejavel nao bloqueante**
- maximizar extras de elenco, head to head e assets

**Blocker real**
- inconsistencias criticas entre raw e mart
- orfaos criticos
- quebra de contratos baseline de consumo

**Risco monitoravel**
- declarar pronto com gaps ainda nao classificados

## 7. Caminho Recomendado

### 7.1 Ordem recomendada de execucao futura

Ordem recomendada:
1. Bloco 0
2. Bloco 1
3. Bloco 2
4. Bloco 4
5. Bloco 5
6. Bloco 6
7. Bloco 7
8. Bloco 8

Observacao:
- o antigo Bloco 3 foi absorvido e nao deve ser aberto como task independente.

### 7.2 Menor caminho ate MVP

Menor caminho ate MVP:
- Bloco 0
- Bloco 1
- Bloco 2
- Bloco 4
- Bloco 5
- Bloco 6
- Bloco 8

### 7.3 Menor caminho ate escopo completo

Menor caminho ate escopo completo:
- Bloco 0
- Bloco 1
- Bloco 2
- Bloco 4
- Bloco 5
- Bloco 6
- Bloco 7
- Bloco 8

### 7.4 Checkpoints duros

Primeiro checkpoint duro semantico:
- fim do Bloco 0

Primeiro checkpoint duro de carga:
- fim do Bloco 2

Primeiro bloco com carga real:
- Bloco 2

## 8. Riscos e Blockers Provaveis

Riscos e blockers especificos deste caso:
- Primeira Liga ainda nao esta no catalogo canonico atual do projeto;
- ambiguidade de split-year entre `seasonLabel`, `queryId` e `provider_season_id`;
- schema real de `control.*` nao confirmado nesta rodada;
- nome legado de DAG `ingest_brasileirao_2024_backfill` gerar interpretacao errada na execucao futura;
- `player_season_statistics` depender de lineups e fixture player statistics estaveis;
- cobertura parcial do provider em `attendance`, `weather`, `referee`, `head_to_head`, `team_coaches`, `team_sidelined`, `player_transfers`;
- risco de tratar cup/hybrid como baseline de liga;
- risco de declarar pronto apenas com DAG verde, sem coerencia raw -> mart e sem smoke de consumo.

## 9. Criterio de Aceite

### 9.1 Pronto para MVP

Liga Portugal esta pronta em padrao MVP quando existir, no minimo:
- 5 seasons resolvidas e canonicas;
- cadastro canonico aprovado para competicao e seasons;
- `competition structure`, `standings` e `fixtures` carregados para as 5 seasons;
- `dim_team` e `dim_venue` utilizaveis;
- `match_statistics`, `match_events`, `fixture_lineups` e `fixture_player_statistics` carregados em nivel suficiente para match center baseline;
- `player_season_statistics` carregado e `dim_player` estabilizado;
- marts baseline materializados e coerentes com raw;
- superficies baseline respondendo:
  - `/matches`
  - `/matches/{id}`
  - `/standings`
  - `/teams`
  - `/players`
  - `/rankings`
  - `/search`
- contagens por season registradas;
- ausencia de orfaos criticos;
- gaps remanescentes classificados como `PROVIDER_COVERAGE_GAP` quando aplicavel.

### 9.2 Pronto para padrao completo

Liga Portugal esta pronta em padrao completo quando existir:
- tudo do MVP;
- `head_to_head` carregado e derivado onde aplicavel;
- `team_coaches`, `team_sidelined` e `player_transfers` carregados em cobertura suficiente e classificados;
- assets visuais carregados no escopo acordado;
- checklist final do Bloco 8 aprovado sem gap funcional aberto.

### 9.3 Checklist de aceite do Bloco 8

Checklist final obrigatorio:
- counts por season nas tabelas raw relevantes
- counts por season nos marts relevantes
- reconciliacao raw -> mart
- zero orfaos criticos
- zero duplicidade critica
- `dbt test` verde ou justificativa formal
- `great_expectations_checks` verde ou justificativa formal
- `data_quality_checks` verde ou justificativa formal
- smoke baseline do BFF/API preservado
- gaps classificados como `PROVIDER_COVERAGE_GAP` quando forem do provider

## 10. Conclusao

O planejamento da ingestao da Liga Portugal / Primeira Liga esta consolidado e pronto para ser usado como base oficial da execucao futura.

Estado final deste documento:
- contrato canonico recomendado fechado;
- absorcao do antigo Bloco 3 formalizada;
- gates por bloco endurecidos;
- granularidade operacional definida;
- caminho ate MVP e ate escopo completo definido;
- criterio de aceite final rigidificado.

Este documento encerra a fase de planejamento. A proxima etapa, se autorizada, deve converter este plano em prompts de execucao bloco a bloco, sem reabrir o escopo.
