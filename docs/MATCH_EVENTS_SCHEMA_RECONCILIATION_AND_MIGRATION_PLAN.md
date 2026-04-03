# Match Events Schema Reconciliation and Migration Plan

Data de referencia: 2026-03-31  
Escopo desta rodada: reconciliacao estrutural e planejamento. Nenhuma migracao estrutural, nenhum reparticionamento, nenhuma movimentacao de rows, nenhuma alteracao de PK e nenhuma alteracao de indices foi executada nesta rodada.

## 1. Objetivo

Fechar a base canônica de `raw.match_events` no repositório antes de abrir uma migracao dedicada de estrutura.  
O objetivo aqui nao e "resolver a tabela". O objetivo e:

- congelar o estado vivo correto;
- explicitar onde ele diverge do repositório e do codigo;
- definir o contrato canônico recomendado;
- preparar um plano seguro para a migracao estrutural futura.

## 2. Fontes de evidencia

### Catalogo vivo do Postgres

- `information_schema.columns`
- `pg_constraint`
- `pg_indexes`
- `pg_partitioned_table`
- `pg_inherits`
- `pg_class`
- `pg_dump -s -t raw.match_events -t raw.match_events_2024 -t raw.match_events_default`

### Repositório

- `db/migrations/20260217120000_baseline_schema.sql`
- `db/migrations/20260217122000_optimization_indexes.sql`
- `db/migrations/20260219133000_match_events_time_elapsed_anomaly.sql`
- `db/migrations/20260219154500_raw_constraints_indexes_hardening.sql`
- `infra/airflow/dags/common/services/warehouse_service.py`
- `infra/airflow/dags/common/mappers/events_mapper.py`
- `dbt/models/staging/stg_match_events.sql`
- `tests/test_match_events_semantics.py`

## 3. Estado Vivo Observado

### 3.1 Tabela pai e particionamento

- Tabela pai viva: `raw.match_events`
- Estrategia de particionamento viva: `LIST (season)`
- Tipo da relacao:
  - pai: `relkind = p`
  - folhas: `relkind = r`, `relispartition = true`
- Particoes atualmente existentes:
  - `raw.match_events_2024` para `FOR VALUES IN (2024)`
  - `raw.match_events_default` para `DEFAULT`

### 3.2 DDL vivo consolidado

```sql
CREATE TABLE raw.match_events (
    event_id text NOT NULL,
    season integer NOT NULL,
    fixture_id bigint NOT NULL,
    time_elapsed integer,
    time_extra integer,
    team_id bigint,
    team_name text,
    player_id bigint,
    player_name text,
    assist_id bigint,
    assist_name text,
    type text,
    detail text,
    comments text,
    ingested_run text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    is_time_elapsed_anomalous boolean DEFAULT false NOT NULL,
    provider text NOT NULL,
    provider_league_id bigint,
    competition_key text,
    season_label text,
    provider_season_id bigint,
    provider_event_id text,
    ingested_at timestamp with time zone,
    source_run_id text
)
PARTITION BY LIST (season);
```

Constraints vivas do pai:

- `pk_match_events PRIMARY KEY (provider, season, fixture_id, event_id)`
- `fk_match_events_fixture FOREIGN KEY (fixture_id) REFERENCES raw.fixtures(fixture_id)`

Particoes vivas:

```sql
CREATE TABLE raw.match_events_2024 PARTITION OF raw.match_events FOR VALUES IN (2024);
CREATE TABLE raw.match_events_default PARTITION OF raw.match_events DEFAULT;
```

### 3.3 Distribuicao de rows

Total de rows em `raw.match_events`: `278635`

Por season:

| Season | Rows | % do total | Particao atual |
|---|---:|---:|---|
| `2020` | `37742` | `13.55%` | `raw.match_events_default` |
| `2021` | `54189` | `19.45%` | `raw.match_events_default` |
| `2022` | `51986` | `18.66%` | `raw.match_events_default` |
| `2023` | `57591` | `20.67%` | `raw.match_events_default` |
| `2024` | `58484` | `20.99%` | `raw.match_events_2024` |
| `2025` | `18643` | `6.69%` | `raw.match_events_default` |

Por particao:

| Particao | Rows | % do total | Tamanho |
|---|---:|---:|---|
| `raw.match_events_default` | `220151` | `79.01%` | `115 MB` |
| `raw.match_events_2024` | `58484` | `20.99%` | `34 MB` |

Distribuicao interna do `default`:

| Season no default | Rows | % do default |
|---|---:|---:|
| `2023` | `57591` | `26.16%` |
| `2021` | `54189` | `24.61%` |
| `2022` | `51986` | `23.61%` |
| `2020` | `37742` | `17.14%` |
| `2025` | `18643` | `8.47%` |

### 3.4 Preenchimento das colunas semanticas/live-only

| Coluna | Rows nao nulas | Leitura |
|---|---:|---|
| `provider` | `278635` | totalmente preenchida |
| `provider_league_id` | `278635` | totalmente preenchida |
| `competition_key` | `278635` | totalmente preenchida |
| `season_label` | `278635` | totalmente preenchida |
| `provider_season_id` | `278635` | totalmente preenchida |
| `source_run_id` | `278635` | totalmente preenchida |
| `provider_event_id` | `0` | coluna viva, sem carga atual |
| `ingested_at` | `0` | coluna viva, sem carga atual |

### 3.5 Chaves e unicidade observadas

| Checagem | Resultado |
|---|---:|
| total de rows | `278635` |
| distintos em `(provider, season, fixture_id, event_id)` | `278635` |
| distintos em `(event_id, season)` | `278635` |
| providers distintos | `1` (`sportmonks`) |

Leitura correta:

- a PK viva esta coerente com o runtime e totalmente satisfeita;
- o conjunto legado `(event_id, season)` tambem e unico hoje;
- isso significa que a divergencia de PK nao esta respondendo a colisao de dados observada hoje;
- ela responde a alinhamento de contrato do runtime e a robustez do grao natural multi-provider.

## 4. Comparativo: Estado Vivo vs Migrations vs Codigo

### 4.1 Estrutura de alto nivel

| Aspecto | Estado vivo observado | Estado descrito nas migrations | Estado assumido pelo codigo | Leitura |
|---|---|---|---|---|
| Tabela pai | `raw.match_events` | `raw.match_events` | `raw.match_events` | alinhado |
| Particionamento | `LIST (season)` | `LIST (season)` | codigo nao define, apenas usa a tabela | alinhado no conceito |
| Particoes existentes | `2024` + `default` | so `2024` explicitamente | codigo assume que a tabela existe e aceita inserts | drift operacional |
| PK | `(provider, season, fixture_id, event_id)` | `(event_id, season)` | `ON CONFLICT (provider, season, fixture_id, event_id)` | drift confirmado |
| FK | `fixture_id -> raw.fixtures(fixture_id)` | mesma FK | joins e semantica dependem de `raw.fixtures` | alinhado |
| Colunas semanticas (`provider`, `provider_league_id`, `competition_key`, `season_label`, `provider_season_id`, `source_run_id`) | presentes e 100% preenchidas | ausentes | obrigatorias na carga atual | drift confirmado |
| `is_time_elapsed_anomalous` | presente | presente via migration propria | obrigatoria na carga atual | alinhado |
| `provider_event_id` | presente, 0% preenchida | ausente | nao usado no loader; so usado no mapper para derivar `event_id` | live-only fria |
| `ingested_at` | presente, 0% preenchida | ausente | nao usada | live-only fria |
| Indice `(competition_key, season_label)` | presente no pai e filhos | ausente | colunas semanticas sao usadas em varias camadas | drift confirmado |
| Indices duplicados no `default` | sim, em `fixture_id`, `player_id`, `team_id` | ausentes | nao ha contrato de codigo exigindo duplicidade | carryover tecnico, nao canônico |

### 4.2 Matriz por coluna

| Coluna | Estado vivo | Migrations | `warehouse_service.py` | Leitura |
|---|---|---|---|---|
| `event_id` | `text not null` | presente | target, required, PK logica do silver | alinhado |
| `season` | `integer not null` | presente | target, key | alinhado |
| `fixture_id` | `bigint not null` | presente | target, required, key | alinhado |
| `time_elapsed` | `integer` | presente | target | alinhado |
| `time_extra` | `integer` | presente | target | alinhado |
| `team_id` | `bigint` | presente | target | alinhado |
| `team_name` | `text` | presente | target | alinhado |
| `player_id` | `bigint` | presente | target | alinhado |
| `player_name` | `text` | presente | target | alinhado |
| `assist_id` | `bigint` | presente | target | alinhado |
| `assist_name` | `text` | presente | target | alinhado |
| `type` | `text` | presente | target | alinhado |
| `detail` | `text` | presente | target | alinhado |
| `comments` | `text` | presente | target | alinhado |
| `ingested_run` | `text` | presente | target | alinhado |
| `updated_at` | `timestamptz not null default now()` | presente | usado no `DO UPDATE` | alinhado |
| `is_time_elapsed_anomalous` | `boolean not null default false` | presente | target | alinhado |
| `provider` | `text not null` | ausente | target, semantica, PK | drift confirmado |
| `provider_league_id` | `bigint` | ausente | target, semantica | drift confirmado |
| `competition_key` | `text` | ausente | target, semantica | drift confirmado |
| `season_label` | `text` | ausente | target, semantica | drift confirmado |
| `provider_season_id` | `bigint` | ausente | target, semantica | drift confirmado |
| `source_run_id` | `text` | ausente | target, semantica | drift confirmado |
| `provider_event_id` | `text` | ausente | ausente do target; usado apenas no mapper para derivar `event_id` | live-only fria |
| `ingested_at` | `timestamptz` | ausente | ausente | live-only fria |

### 4.3 Contrato do codigo

#### Loader `warehouse_service.py`

O loader atual de `raw.match_events`:

- exige semanticamente:
  - `provider`
  - `provider_league_id`
  - `competition_key`
  - `season_label`
  - `provider_season_id`
  - `source_run_id`
- enriquece esses campos a partir de `raw.fixtures` + `control.season_catalog`;
- faz `ON CONFLICT (provider, season, fixture_id, event_id)`;
- usa `provider, season, fixture_id, event_id` como grão de conflito;
- nao usa `provider_event_id`;
- nao usa `ingested_at`.

#### Mapper `events_mapper.py`

O mapper de silver:

- usa `provider_event_id` do payload, quando existir, para derivar o hash de `event_id`;
- se `provider_event_id` nao existir, usa a assinatura do evento;
- nao persiste `provider_event_id` na saida parquet;
- remove duplicatas apenas por `event_id`.

Leitura importante:

- o codigo atual trata `event_id` como identificador tecnico derivado;
- a tabela viva reserva `provider_event_id`, mas a carga atual nao a preenche;
- isso torna `provider_event_id` uma coluna viva, porem sem contrato de persistencia ativo.

#### Downstream dbt

`dbt/models/staging/stg_match_events.sql` usa apenas:

- `event_id`
- `season`
- `fixture_id`
- `time_elapsed`
- `time_extra`
- `is_time_elapsed_anomalous`
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

As colunas semanticas novas nao sao consumidas por esse staging hoje, mas sao parte do contrato de carga e da consistencia do `raw`.

## 5. Divergencias Confirmadas

### 5.1 Divergencias entre schema vivo e migrations

Confirmado:

1. A PK viva e `(provider, season, fixture_id, event_id)`, enquanto as migrations registram `(event_id, season)`.
2. As migrations nao registram as colunas semanticas hoje materialmente usadas:
   - `provider`
   - `provider_league_id`
   - `competition_key`
   - `season_label`
   - `provider_season_id`
   - `source_run_id`
3. As migrations nao registram as colunas vivas:
   - `provider_event_id`
   - `ingested_at`
4. As migrations nao registram o indice vivo:
   - `idx_raw_match_events_competition_season_label`
5. As migrations nao registram o estado operacional atual de particoes:
   - `raw.match_events_default`
6. As migrations tambem nao documentam a duplicidade exata de indices no `default`.

### 5.2 Divergencias entre schema vivo e codigo

Confirmado:

1. O codigo usa a PK viva, nao a PK descrita nas migrations.
2. O codigo depende das colunas semanticas vivas e preenchidas, ausentes nas migrations.
3. O codigo nao usa `provider_event_id` nem `ingested_at` na carga para `raw`.
4. `provider_event_id` aparece apenas no mapper como insumo para gerar `event_id`.

### 5.3 Divergencias internas do proprio codigo

Confirmado:

1. O silver deduplica `match_events` por `event_id`.
2. O raw upserta por `(provider, season, fixture_id, event_id)`.

Leitura correta:

- nao ha quebra observada hoje, porque `event_id` e derivado com `fixture_id` na composicao do hash;
- mas existe uma assimetria implicita de contrato que precisa ficar documentada;
- essa assimetria nao deve ser corrigida junto com o reparticionamento sem benchmark e sem decisao semantica propria.

## 6. Estado Canônico Recomendado

## 6.1 Principio desta rodada

O canônico recomendado para o repositório deve refletir o contrato vivo real que o banco ja opera hoje.  
Esta rodada nao deve "limpar" tudo ao mesmo tempo.

Por isso:

- a tabela pai, PK, FK, particionamento e colunas semanticas devem ser canonizadas exatamente como estao vivas;
- `provider_event_id` e `ingested_at` devem ser preservadas no DDL canônico desta rodada como colunas compatíveis, embora frias;
- a familia de indices particionados viva deve ser reconhecida como canônica;
- os tres indices locais duplicados do `default` nao devem ser tratados como canônicos, apenas como carryover tecnico a ser revisado na futura migracao.

## 6.2 Contrato canônico recomendado do pai

```sql
CREATE TABLE raw.match_events (
    event_id text NOT NULL,
    season integer NOT NULL,
    fixture_id bigint NOT NULL,
    time_elapsed integer,
    time_extra integer,
    team_id bigint,
    team_name text,
    player_id bigint,
    player_name text,
    assist_id bigint,
    assist_name text,
    type text,
    detail text,
    comments text,
    ingested_run text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    is_time_elapsed_anomalous boolean DEFAULT false NOT NULL,
    provider text NOT NULL,
    provider_league_id bigint,
    competition_key text,
    season_label text,
    provider_season_id bigint,
    provider_event_id text,
    ingested_at timestamp with time zone,
    source_run_id text,
    CONSTRAINT pk_match_events PRIMARY KEY (provider, season, fixture_id, event_id),
    CONSTRAINT fk_match_events_fixture FOREIGN KEY (fixture_id) REFERENCES raw.fixtures(fixture_id)
)
PARTITION BY LIST (season);
```

## 6.3 Catalogo canônico minimo de particoes

No estado atual do repositório, o catalogo canônico minimo precisa reconhecer explicitamente:

```sql
CREATE TABLE raw.match_events_2024 PARTITION OF raw.match_events FOR VALUES IN (2024);
CREATE TABLE raw.match_events_default PARTITION OF raw.match_events DEFAULT;
```

Leitura:

- isso nao resolve o excesso de rows no `default`;
- apenas corrige o fato de que o repositório hoje nao espelha nem o minimo do estado vivo.

## 6.4 Familia canônica de indices

Indices canônicos que o repositório precisa refletir para `raw.match_events`:

- `pk_match_events` em `(provider, season, fixture_id, event_id)`
- `idx_raw_match_events_assist_id` em `(assist_id)`
- `idx_raw_match_events_competition_season_label` em `(competition_key, season_label)`
- `idx_raw_match_events_fixture_id` em `(fixture_id)`
- `idx_raw_match_events_fixture_type` em `(fixture_id, type)`
- `idx_raw_match_events_player_id` em `(player_id)`
- `idx_raw_match_events_team_id` em `(team_id)`

Estado nao canônico confirmado:

- `idx_raw_match_events_default_fixture_id`
- `idx_raw_match_events_default_player_id`
- `idx_raw_match_events_default_team_id`

Esses tres indices existem no estado vivo atual, mas sao duplicados exatos de:

- `match_events_default_fixture_id_idx`
- `match_events_default_player_id_idx`
- `match_events_default_team_id_idx`

Como sao duplicados exatos na mesma particao, devem ser tratados como carryover tecnico.  
Nao devem compor o contrato canônico recomendado desta rodada.

## 7. Riscos Tecnicos de Mexer Sem Reconciliacao

1. Reparticionar baseado nas migrations atuais reintroduziria uma PK errada.
2. Criar novas particoes com DDL do repositório atual perderia colunas semanticas hoje obrigatorias ao loader.
3. Limpar indices sem separar o que e familia particionada do que e duplicata local pode remover cobertura errada.
4. Tentar mover rows do `default` antes de canonizar o DDL aumenta risco de:
   - attach errado de indice;
   - falha de roteamento;
   - drift ainda maior entre runtime e repositório.
5. Mexer em `provider_event_id` e `ingested_at` agora misturaria cleanup semantico com migracao estrutural de particionamento.

## 8. Plano Tecnico da Futura Migracao Estrutural

## 8.1 Objetivo da migracao futura

Tirar seasons historicas do `default`, criar particoes explicitas por season necessaria e deixar `raw.match_events` com:

- DDL do repositório alinhado ao runtime;
- distribuicao de rows previsivel por season;
- indices sem duplicidade local desnecessaria;
- risco operacional menor para crescimento futuro.

## 8.2 Pre-requisitos obrigatorios

1. Repositório ja reconciliado com o DDL canônico aprovado nesta rodada.
2. Janela operacional dedicada para `raw.match_events`.
3. Freeze do loader `load_match_events_silver_to_raw` durante a migracao ou serializacao estrita do write path.
4. Snapshot before:
   - contagem total;
   - contagem por season;
   - contagem por particao;
   - inventario de constraints;
   - inventario de indices;
   - tamanho por particao.
5. Validacao previa de unicidade:
   - `(provider, season, fixture_id, event_id)`
6. Plano de rollback transacional por season.

## 8.3 Ordem segura de execucao

### Fase 0 - Congelamento do contrato

1. Aprovar o DDL canônico do pai.
2. Aprovar o catalogo canônico minimo de particoes.
3. Aprovar a familia canônica de indices.
4. Manter `provider_event_id` e `ingested_at` fora do escopo de cleanup estrutural desta migracao.

### Fase 1 - Preparacao fisica

1. Criar particoes explicitas para seasons hoje presas no `default`:
   - `2020`
   - `2021`
   - `2022`
   - `2023`
   - `2025`
2. Garantir que cada nova particao receba a familia canônica de indices anexada ao pai.
3. Nao alterar PK nem FK nesta fase.

### Fase 2 - Drenagem do `default`

Estrategia recomendada: mover season por season, em ordem crescente de risco operacional.

Ordem recomendada de pilotagem:

1. `2025` (`18643` rows)
2. `2020` (`37742` rows)
3. `2022` (`51986` rows)
4. `2021` (`54189` rows)
5. `2023` (`57591` rows)

Racional:

- comecar pela menor slice reduz risco e valida o procedimento;
- deixar a maior slice para o final evita descobrir problema de processo em lote grande.

### Fase 3 - Revisao de carryover tecnico

1. Revalidar o `default` apos drenagem.
2. Revisar os tres indices locais duplicados do `default`.
3. So entao decidir a remocao deles.

### Fase 4 - Pos-migracao

1. Revalidar distribuicao por particao.
2. Revalidar top SQL que usa `raw.match_events`, se houver.
3. Atualizar inventario operacional.

## 8.4 Estrategia para tirar rows do `default`

Estrategia conceitual recomendada:

1. Criar a particao explicita da season alvo.
2. Dentro de uma unidade transacional controlada por season:
   - inserir a season alvo via tabela pai `raw.match_events`, selecionando rows do `default`;
   - deixar o roteamento mandar as rows para a nova particao;
   - validar a contagem da season na nova particao;
   - deletar a mesma season do `default`;
   - validar que o `default` ficou sem aquela season.

Observacao importante:

- depois que uma particao explicita passa a existir para uma season, o `default` deixa de ser destino valido para aquela season;
- por isso o rollback conceitual mais seguro e por transacao inteira da season, nao por ajuste manual posterior.

## 8.5 Estrategia para PK, constraints e indices

### PK e constraints

- manter a PK viva atual durante toda a migracao:
  - `(provider, season, fixture_id, event_id)`
- manter a FK atual para `raw.fixtures(fixture_id)`.
- nao misturar reparticionamento com nova mudanca de PK nesta migracao.

### Indices

- preservar a familia canônica de indices particionados.
- verificar attach correto dos filhos novos.
- postergar a limpeza dos tres indices duplicados do `default` para depois da drenagem das seasons.

### Colunas frias

- `provider_event_id`
- `ingested_at`

Essas colunas nao devem ser removidas nem preenchidas nesta migracao estrutural.  
Elas pedem uma decisao propria de cleanup/uso futuro.

## 8.6 Rollback conceitual

Rollback seguro recomendado: por season e por transacao.

Se a season ainda nao foi commitada:

- rollback simples da transacao inteira.

Se a season ja foi commitada:

1. bloquear novas cargas dessa season;
2. remover ou desanexar a particao criada para a season;
3. reexecutar o estado anterior somente por runbook dedicado.

Leitura correta:

- depois do commit por season, rollback deixa de ser trivial;
- isso reforca a necessidade de migrar por slices pequenas e validadas.

## 8.7 Validacoes necessarias na futura migracao

### Before

- contagem total em `raw.match_events`
- contagem por season
- contagem por particao
- checagem de unicidade da PK viva
- inventario de indices por pai e filhos
- snapshot de tamanho por particao

### After por season

- a contagem da season na nova particao bate com a slice de origem
- o `default` nao tem mais rows daquela season
- a contagem total do pai nao mudou
- a PK viva continua unica
- as constraints continuam validas

### After final

- `default` retido apenas para seasons nao explicitamente particionadas
- distribuicao por particao coerente
- inventario de indices sem ambiguidade
- migrations do repositório finalmente coerentes com o runtime

## 9. Conclusao Objetiva

`raw.match_events` nao esta bloqueando por "falta de particao".  
Ela esta bloqueando porque o runtime ja evoluiu para um contrato estrutural que o repositório ainda nao representa.

O que esta amarrado com evidencia nesta rodada:

- o schema vivo correto;
- a PK viva correta;
- o conjunto de colunas que o loader realmente usa;
- as colunas vivas que existem, mas estao frias;
- a estrategia de particionamento viva;
- a distribuicao real que mostra porque o `default` virou problema;
- a familia de indices canônica e a duplicidade local do `default`.

O que fica para a migracao futura:

- criar particoes faltantes;
- drenar o `default` por season;
- revisar indices duplicados;
- manter rollback por slice e por transacao.
