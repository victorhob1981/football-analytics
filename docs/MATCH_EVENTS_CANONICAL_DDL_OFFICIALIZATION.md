# Match Events Canonical DDL Officialization

Data de referencia: 2026-04-01  
Escopo desta rodada: oficializacao declarativa/canônica de `raw.match_events` no repositório.  
Fora de escopo nesta rodada: reparticionamento fisico, movimentacao de rows, drenagem do `default`, cleanup de colunas frias, remocao de indices duplicados, outras frentes da Onda 4.

## 1. Problema que existia

Antes desta rodada, o repositório nao descrevia corretamente o estado estrutural vivo de `raw.match_events`.

Drifts confirmados:

- migrations historicas registravam PK `(event_id, season)`, enquanto o runtime opera com `(provider, season, fixture_id, event_id)`;
- as colunas semanticas vivas usadas pelo loader nao existiam no DDL historico:
  - `provider`
  - `provider_league_id`
  - `competition_key`
  - `season_label`
  - `provider_season_id`
  - `source_run_id`
- o repositório nao reconhecia explicitamente a particao `raw.match_events_default`;
- o repositório nao declarava o indice canônico `(competition_key, season_label)`;
- um bootstrap limpo por `dbmate up` tenderia a nascer com um contrato historico incorreto para `raw.match_events`.

## 2. Estratégia escolhida

Estratégia adotada: abordagem combinada e controlada.

Componentes:

1. editar as migrations historicas que ja definiam/endureciam `raw.match_events`, para que um bootstrap limpo passe a reproduzir o canônico;
2. adicionar uma migration forward-only de assertiva canônica, sem mudar dados nem estrutura fisica, para registrar explicitamente o ponto em que o repositório passou a exigir esse contrato.

Arquivos de oficializacao:

- `db/migrations/20260217120000_baseline_schema.sql`
- `db/migrations/20260219154500_raw_constraints_indexes_hardening.sql`
- `db/migrations/20260401101500_match_events_canonical_assertion.sql`

## 3. Por que essa estratégia é a correta

Editar apenas migrations historicas seria insuficiente por integridade operacional:

- melhora bootstrap limpo;
- mas nao cria um marco explícito na linha do tempo de quando o canônico passou a ser exigido;
- e nao protege ambientes existentes de continuar divergindo silenciosamente.

Criar apenas uma migration forward-only seria insuficiente por reprodutibilidade:

- manteria o bootstrap limpo replayando um DDL historico errado para `raw.match_events`;
- e deixaria a coerencia depender de correcoes posteriores, o que enfraquece o contrato-base do schema.

Por isso a combinacao e a estrategia correta:

- historico ajustado para reprodutibilidade de ambiente novo;
- migration forward-only apenas assertiva para seguranca operacional e rastreabilidade;
- separacao preservada entre:
  - oficializacao declarativa;
  - migracao estrutural fisica futura.

Leitura executiva:

- esta rodada fecha o contrato do repo;
- a rodada futura continua responsavel por qualquer alteracao fisica de particionamento e redistribuicao de rows.

## 4. O que foi oficializado no repo

### 4.1 Tabela pai canônica

`raw.match_events` passa a ser declarada no repositório com:

- `25` colunas canônicas;
- PK canônica: `(provider, season, fixture_id, event_id)`;
- FK canônica: `fixture_id -> raw.fixtures(fixture_id)`;
- particionamento canônico: `PARTITION BY LIST (season)`.

Colunas canônicas oficializadas:

- `event_id`
- `season`
- `fixture_id`
- `time_elapsed`
- `time_extra`
- `team_id`
- `team_name`
- `player_id`
- `player_name`
- `assist_id`
- `assist_name`
- `type`
- `detail`
- `comments`
- `ingested_run`
- `updated_at`
- `is_time_elapsed_anomalous`
- `provider`
- `provider_league_id`
- `competition_key`
- `season_label`
- `provider_season_id`
- `provider_event_id`
- `ingested_at`
- `source_run_id`

### 4.2 Partições mínimas reconhecidas nesta etapa

O repositório passa a reconhecer explicitamente:

- `raw.match_events_2024`
- `raw.match_events_default`

Isso nao resolve o excesso de rows no `default`.  
Isso apenas oficializa o minimo que o runtime ja possui hoje.

### 4.3 Família canônica de índices

O repositório passa a descrever como canônica a seguinte familia de índices de `raw.match_events`:

- `pk_match_events`
- `idx_raw_match_events_assist_id`
- `idx_raw_match_events_competition_season_label`
- `idx_raw_match_events_fixture_id`
- `idx_raw_match_events_fixture_type`
- `idx_raw_match_events_player_id`
- `idx_raw_match_events_team_id`

## 5. O que continua fora de escopo

Continua fora desta rodada:

- criar novas particoes por season alem de `2024` e `default`;
- mover rows para fora do `default`;
- drenar seasons;
- alterar fisicamente a PK viva do runtime;
- remover ou revisar os tres indices duplicados locais do `default`;
- remover ou reaproveitar `provider_event_id` e `ingested_at`;
- qualquer outra intervencao estrutural em outras tabelas.

## 6. Como fica a reprodutibilidade de ambiente novo

### 6.1 Estado esperado do bootstrap limpo

Em termos declarativos, um ambiente novo que reexecute as migrations deve agora chegar ao contrato canônico de `raw.match_events`, porque:

- a baseline passa a criar a tabela com as colunas e a PK corretas;
- o hardening passa a estar coerente com a PK canônica e cria o índice `(competition_key, season_label)`;
- a migration forward-only de assertiva valida que o runtime bate com o contrato oficializado.

### 6.2 Resultado da prova executável em bootstrap limpo

A validacao executavel de `raw.match_events` foi concluida e depois o blocker global de `control` foi tratado em rodada separada.

Estado consolidado apos essas duas rodadas:

- `raw.match_events` nasceu corretamente em bootstrap limpo com o contrato canônico oficializado;
- a migration assertiva canônica de `raw.match_events` passou em banco temporario;
- o blocker externo de `control` foi removido por uma foundation dedicada do catalogo `control`;
- o replay global do repositório passou pela Supercopa e avancou alem desse ponto.

Leitura correta:

- a oficializacao declarativa de `raw.match_events` ficou validada em bootstrap limpo;
- o blocker remanescente do bootstrap global deixou de ser `control` e passou a estar em migration posterior sobre tabelas `mart`.

## 7. Riscos remanescentes

1. Ambientes existentes que nao estejam no estado vivo canônico nao serao corrigidos por esta rodada; eles apenas passarao a poder ser detectados pela migration assertiva.
2. O `default` continua com `79.01%` das rows observadas no runtime analisado; nenhum ganho fisico ocorreu aqui.
3. Os tres índices duplicados locais do `default` continuam existindo no runtime e continuam deliberadamente fora do contrato canônico.
4. O replay completo das migrations do repositório ainda depende de tratamento posterior para migrations que indexam tabelas `mart` nao bootstrapadas por schema base.

## 8. Próximo passo seguro depois desta rodada

Proximo passo seguro:

1. tratar o proximo blocker global do bootstrap em migrations que tentam indexar tabelas `mart` antes de elas existirem em replay limpo;
2. depois disso, reexecutar `dbmate up` em banco limpo para fechar o bootstrap completo do repo;
3. com esse ponto resolvido, seguir apenas entao para a rodada separada de migracao estrutural fisica de `raw.match_events`, se ainda fizer sentido no plano.
