# Player Transfers Backfill Execution Plan

Data de referencia: `2026-04-01`

## Objetivo

Fechar os `53` escopos faltantes de `player_transfers` com baixo risco operacional, preservando os `3` escopos ja validados como verdes.

## Estado Atual Confirmado

### Escopo analisado

- dominio: `player_transfers`
- provider: `sportmonks`
- elegibilidade do alvo: `player_id` distinto visto em `raw.fixture_lineups` por `league_id + season`
- fallback do codigo: `raw.match_events` so e usado quando o escopo nao possui lineups

### Escopos ja verdes e que nao devem ser tocados sem motivo objetivo

| competition_key | season_label | league_id | season | observacao |
| --- | --- | ---: | ---: | --- |
| `primeira_liga` | `2023_24` | `462` | `2023` | carregado e validado |
| `primeira_liga` | `2024_25` | `462` | `2024` | carregado e validado |
| `brasileirao_a` | `2024` | `648` | `2024` | carregado e validado |

### Evidencia objetiva do estado atual

- `56` escopos com jogadores elegiveis via lineup
- `3` escopos com trilha `player_transfers` executada
- `53` escopos faltantes
- `raw.player_transfers = 11451`
- `mart.stg_player_transfers = 11451`
- duplicidade de grain atual:
  - `raw_duplicate_rows = 0`
  - `stg_duplicate_rows = 0`
- paridade atual:
  - `raw_only_rows = 0`
  - `stg_only_rows = 0`

### Leitura operacional importante

- `status = idle` em `raw.provider_sync_state` nao significa gap de ingestao por si so.
- o criterio de verdade para completude do backfill por escopo e:
  - `eligible_players` no banco
  - `player_id` com `data.json` no Bronze
  - consistencia `raw -> mart.stg`
- jogadores sem linha em `raw.player_transfers` podem ser cobertura vazia legitima do provider (`results=0`, `response=[]`).

## Regras Duras de Execucao

1. Nao reprocessar os `3` escopos verdes no inicio.
2. Nao rodar `pipeline_brasileirao` nem outro orquestrador amplo para este backfill.
3. Rodar apenas os DAGs dedicados:
   - `ingest_player_transfers_bronze`
   - `bronze_to_silver_player_transfers`
   - `silver_to_postgres_player_transfers`
4. Executar um escopo por vez.
5. Validar o escopo atual antes de seguir para o proximo.
6. Parar no primeiro blocker real.
7. Nao corrigir dado manualmente em `raw`.

## Criterio de Sucesso por Escopo

Um escopo so pode ser marcado como `COMPLETO` quando todas as condicoes abaixo forem verdadeiras:

1. Os tres DAGs do escopo concluem sem erro.
2. Existe linha em `raw.provider_sync_state` para `entity_type='player_transfers'`.
3. `eligible_players == bronze_player_ids`.
4. `raw.player_transfers` e `mart.stg_player_transfers` permanecem sem duplicidade de grain.
5. `raw` e `mart.stg` permanecem em paridade por `(provider, transfer_id)`.
6. Se houver jogadores elegiveis sem linha em `raw.player_transfers`, uma amostra objetiva mostra Bronze com `results=0` e `response=[]`.

## Blocker Real

Interromper a execucao dos proximos blocos se ocorrer qualquer um dos itens abaixo:

- falha em qualquer DAG do escopo atual
- `missing_in_bronze > 0`
- duplicidade em `raw.player_transfers`
- divergencia `raw -> mart.stg`
- erro reprodutivel do provider
- necessidade de ampliar escopo alem de `player_transfers`

## Comandos Base

### 1. Executar um escopo

Usar CLI via `airflow-scheduler`.

```powershell
$runDate = "2026-04-01"
$conf = '{"league_id":648,"season":2021,"provider":"sportmonks"}'

docker compose exec -T airflow-scheduler airflow dags test ingest_player_transfers_bronze $runDate --conf $conf
docker compose exec -T airflow-scheduler airflow dags test bronze_to_silver_player_transfers $runDate --conf $conf
docker compose exec -T airflow-scheduler airflow dags test silver_to_postgres_player_transfers $runDate --conf $conf
```

### 2. Validar jogadores elegiveis do escopo

```powershell
docker compose exec -T postgres psql -U football -d football_dw -c "
select count(distinct fl.player_id) as eligible_players
from raw.fixture_lineups fl
join raw.fixtures f on f.fixture_id = fl.fixture_id
where f.league_id = 648
  and f.season = 2021
  and fl.player_id is not null;
"
```

### 3. Validar `player_id` com `data.json` no Bronze

```powershell
$out = docker compose run --rm --entrypoint /bin/sh minio-init -lc "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc find local/football-bronze/player_transfers/league=648/season=2021 --name data.json"
$ids = $out | ForEach-Object { if ($_ -match 'player_id=(\d+)/run=') { $matches[1] } }
($ids | Sort-Object -Unique | Measure-Object).Count
```

### 4. Validar estado operacional do escopo

```powershell
docker compose exec -T postgres psql -U football -d football_dw -c "
select provider, entity_type, scope_key, cursor, status, updated_at
from raw.provider_sync_state
where provider = 'sportmonks'
  and entity_type = 'player_transfers'
  and league_id = 648
  and season = 2021;
"
```

### 5. Validar duplicidade e paridade

```powershell
docker compose exec -T postgres psql -U football -d football_dw -c "
select count(*) as raw_rows,
       count(distinct (provider, transfer_id)) as raw_distinct_keys,
       count(*) - count(distinct (provider, transfer_id)) as raw_duplicate_rows
from raw.player_transfers;

select count(*) as stg_rows,
       count(distinct (provider, transfer_id)) as stg_distinct_keys,
       count(*) - count(distinct (provider, transfer_id)) as stg_duplicate_rows
from mart.stg_player_transfers;

select count(*) as raw_only_rows
from (
    select provider, transfer_id from raw.player_transfers
    except
    select provider, transfer_id from mart.stg_player_transfers
) diff;

select count(*) as stg_only_rows
from (
    select provider, transfer_id from mart.stg_player_transfers
    except
    select provider, transfer_id from raw.player_transfers
) diff;
"
```

### 6. Validar amostra de jogador elegivel sem transferencia

Usar apenas se `players_without_transfers > 0`.

```powershell
$playerId = 29303723
$out = docker compose run --rm --entrypoint /bin/sh minio-init -lc "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc find local/football-bronze/player_transfers/league=648/season=2024/player_id=$playerId --name data.json"
$path = ($out | Where-Object { $_ -match "player_id=$playerId/.*/data.json" } | Sort-Object | Select-Object -Last 1)
docker compose run --rm --entrypoint /bin/sh minio-init -lc "mc alias set local http://minio:9000 minio minio123 >/dev/null && mc cat '$path'"
```

## Ordem Segura

### Sequencia escolhida

1. smoke de muito baixo volume
2. ligas domesticas estaveis da Europa
3. Brasil domestico
4. continentais CONMEBOL
5. copa nacional de alta cardinalidade
6. Champions League por ultimo

### Motivo da sequencia

- reduz blast radius nas primeiras waves
- valida o caminho operacional em escopos pequenos antes dos grandes
- preserva o que ja esta verde
- adia os escopos com maior cardinalidade e maior custo operacional para o final

## Ledger de Execucao por Wave

### Wave 1 - Smoke de Baixo Risco

Objetivo: validar o caminho operacional em escopos minimos antes de abrir ligas grandes.

| status | competition_key | season_label | league_id | season | eligible_players |
| --- | --- | --- | ---: | ---: | ---: |
| `NAO_FEITO` | `supercopa_do_brasil` | `2025` | `1798` | `2025` | `45` |
| `NAO_FEITO` | `fifa_intercontinental_cup` | `2024` | `1452` | `2024` | `152` |

### Wave 2 - Ligas Domesticas Estaveis da Europa

Objetivo: fechar ligas com escopo regular e risco semantico menor.

| status | competition_key | season_label | league_id | season | eligible_players |
| --- | --- | --- | ---: | ---: | ---: |
| `NAO_FEITO` | `bundesliga` | `2020_21` | `82` | `2020` | `566` |
| `NAO_FEITO` | `bundesliga` | `2024_25` | `82` | `2024` | `568` |
| `NAO_FEITO` | `bundesliga` | `2023_24` | `82` | `2023` | `570` |
| `NAO_FEITO` | `bundesliga` | `2021_22` | `82` | `2021` | `588` |
| `NAO_FEITO` | `bundesliga` | `2022_23` | `82` | `2022` | `589` |
| `NAO_FEITO` | `ligue_1` | `2023_24` | `301` | `2023` | `634` |
| `NAO_FEITO` | `premier_league` | `2020_21` | `8` | `2020` | `641` |
| `NAO_FEITO` | `ligue_1` | `2024_25` | `301` | `2024` | `651` |
| `NAO_FEITO` | `premier_league` | `2021_22` | `8` | `2021` | `676` |
| `NAO_FEITO` | `premier_league` | `2024_25` | `8` | `2024` | `683` |
| `NAO_FEITO` | `ligue_1` | `2020_21` | `301` | `2020` | `684` |
| `NAO_FEITO` | `ligue_1` | `2022_23` | `301` | `2022` | `691` |
| `NAO_FEITO` | `premier_league` | `2022_23` | `8` | `2022` | `697` |
| `NAO_FEITO` | `la_liga` | `2020_21` | `564` | `2020` | `700` |
| `NAO_FEITO` | `ligue_1` | `2021_22` | `301` | `2021` | `714` |
| `NAO_FEITO` | `la_liga` | `2022_23` | `564` | `2022` | `717` |
| `NAO_FEITO` | `la_liga` | `2023_24` | `564` | `2023` | `724` |
| `NAO_FEITO` | `premier_league` | `2023_24` | `8` | `2023` | `734` |
| `NAO_FEITO` | `serie_a_it` | `2022_23` | `384` | `2022` | `735` |
| `NAO_FEITO` | `la_liga` | `2024_25` | `564` | `2024` | `736` |
| `NAO_FEITO` | `la_liga` | `2021_22` | `564` | `2021` | `746` |
| `NAO_FEITO` | `serie_a_it` | `2023_24` | `384` | `2023` | `750` |
| `NAO_FEITO` | `serie_a_it` | `2024_25` | `384` | `2024` | `763` |
| `NAO_FEITO` | `serie_a_it` | `2020_21` | `384` | `2020` | `767` |
| `NAO_FEITO` | `serie_a_it` | `2021_22` | `384` | `2021` | `773` |

### Wave 3 - Brasil Domestico

Objetivo: expandir a cobertura para ligas domesticas brasileiras preservando o dominio ja provado em `brasileirao_a/2024`.

| status | competition_key | season_label | league_id | season | eligible_players |
| --- | --- | --- | ---: | ---: | ---: |
| `NAO_FEITO` | `brasileirao_b` | `2021` | `651` | `2021` | `824` |
| `NAO_FEITO` | `brasileirao_a` | `2021` | `648` | `2021` | `837` |
| `NAO_FEITO` | `brasileirao_a` | `2025` | `648` | `2025` | `854` |
| `NAO_FEITO` | `brasileirao_b` | `2024` | `651` | `2024` | `860` |
| `NAO_FEITO` | `brasileirao_a` | `2022` | `648` | `2022` | `861` |
| `NAO_FEITO` | `brasileirao_a` | `2023` | `648` | `2023` | `863` |
| `NAO_FEITO` | `brasileirao_b` | `2023` | `651` | `2023` | `874` |
| `NAO_FEITO` | `brasileirao_b` | `2025` | `651` | `2025` | `875` |
| `NAO_FEITO` | `brasileirao_b` | `2022` | `651` | `2022` | `896` |

### Wave 4 - Continentais CONMEBOL

Objetivo: fechar torneios continentais antes dos maiores escopos de copa nacional e UCL.

| status | competition_key | season_label | league_id | season | eligible_players |
| --- | --- | --- | ---: | ---: | ---: |
| `NAO_FEITO` | `libertadores` | `2025` | `1122` | `2025` | `1380` |
| `NAO_FEITO` | `libertadores` | `2022` | `1122` | `2022` | `1382` |
| `NAO_FEITO` | `libertadores` | `2021` | `1122` | `2021` | `1385` |
| `NAO_FEITO` | `libertadores` | `2024` | `1122` | `2024` | `1410` |
| `NAO_FEITO` | `libertadores` | `2023` | `1122` | `2023` | `1416` |
| `NAO_FEITO` | `sudamericana` | `2025` | `1116` | `2025` | `1582` |
| `NAO_FEITO` | `sudamericana` | `2024` | `1116` | `2024` | `1608` |

### Wave 5 - Copa do Brasil

Objetivo: fechar o maior escopo nacional de copa, com cardinalidade alta e risco operacional maior.

| status | competition_key | season_label | league_id | season | eligible_players |
| --- | --- | --- | ---: | ---: | ---: |
| `NAO_FEITO` | `copa_do_brasil` | `2022` | `654` | `2022` | `1546` |
| `NAO_FEITO` | `copa_do_brasil` | `2021` | `654` | `2021` | `1828` |
| `NAO_FEITO` | `copa_do_brasil` | `2023` | `654` | `2023` | `1923` |
| `NAO_FEITO` | `copa_do_brasil` | `2024` | `654` | `2024` | `2023` |
| `NAO_FEITO` | `copa_do_brasil` | `2025` | `654` | `2025` | `2064` |

### Wave 6 - Champions League

Objetivo: fechar o maior bloco residual por cardinalidade e dispersao de jogadores.

| status | competition_key | season_label | league_id | season | eligible_players |
| --- | --- | --- | ---: | ---: | ---: |
| `NAO_FEITO` | `champions_league` | `2020_21` | `2` | `2020` | `1801` |
| `NAO_FEITO` | `champions_league` | `2022_23` | `2` | `2022` | `1992` |
| `NAO_FEITO` | `champions_league` | `2023_24` | `2` | `2023` | `2030` |
| `NAO_FEITO` | `champions_league` | `2021_22` | `2` | `2021` | `2076` |
| `NAO_FEITO` | `champions_league` | `2024_25` | `2` | `2024` | `2208` |

## Checklist Operacional por Escopo

Usar este checklist para cada linha da wave ativa:

1. montar `conf` com `league_id`, `season`, `provider='sportmonks'`
2. executar Bronze
3. executar Silver
4. executar Load para Postgres
5. validar `eligible_players`
6. validar `bronze_player_ids`
7. validar `provider_sync_state`
8. validar duplicidade e paridade
9. se tudo fechar, marcar escopo como `COMPLETO`
10. se houver blocker real, marcar `PARCIAL` ou manter `NAO_FEITO` nos proximos e parar

## Revisao Final Esperada ao Termino da Execucao

Ao concluir a execucao real deste plano, revisar bloco por bloco com os status:

- `COMPLETO`
- `PARCIAL`
- `NAO_FEITO`

E registrar:

- wave em que parou ou fechou
- primeiro blocker real, se houver
- evidencias objetivas por escopo executado
- confirmacao de preservacao dos `3` escopos verdes iniciais
