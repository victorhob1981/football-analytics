# Historical Season Resolution

## Objetivo
Registrar a correcao estrutural da resolucao historica de seasons no projeto para providers com discovery vivo incompleto, com foco em `sportmonks`.

## Problema corrigido
- O resolver historico do `SportMonksProvider` dependia apenas de `GET /leagues/{id}?include=seasons`.
- O discovery vivo atual retorna so as 3 seasons mais recentes para varias ligas.
- O projeto ja tinha historico materializado e catalogado de 5 temporadas para ligas onboardadas.
- Isso gerava falso `NO-GO` para seasons antigas em `competition_structure` e `standings`.
- `resolve_fixture_windows()` tambem caia em `YYYY-01-01 -> YYYY-12-31` para split-year quando nao havia override manual.

## Comportamento novo

### 1. Catalog-first para competicoes ja onboardadas
- `ingest_competition_structure_raw()` e `ingest_standings_raw()` agora consultam primeiro:
  - `control.competition_provider_map`
  - `control.season_catalog`
- Quando existe escopo catalogado para `(provider, provider_league_id, season-start-year)`:
  - `competition_key`
  - `season_label`
  - `provider_season_id`
  - `season_start_date`
  - `season_end_date`
  sao repassados explicitamente ao provider.

### 2. Discovery vivo deixa de vetar historico catalogado
- `SportMonksProvider.get_standings()` e `SportMonksProvider.get_competition_structure()` agora aceitam:
  - `season_label`
  - `provider_season_id`
  - `season_start_date`
  - `season_end_date`
- Se o discovery vivo nao trouxer a season historica, mas o `provider_season_id` ja vier resolvido do catalogo, o provider:
  - usa o `provider_season_id` explicitamente;
  - sintetiza o `season_row` minimo a partir do catalogo;
  - nao trata a ausencia no discovery recente como inexistencia historica.

### 3. Comportamento para competicoes novas
- Se a competicao ainda nao estiver catalogada, o comportamento continua `provider-discovery-first`.
- Se o discovery vivo nao resolver o historico de uma competicao nova, isso deve ser tratado como:
  - `historical_season_resolution_unverified`
  e nao como prova de inexistencia historica.

### 4. Fixture windows para split-year
- `resolve_fixture_windows()` agora aceita `provider_name` e `league_id`.
- Para seasons split-year com datas reais no catalogo:
  - usa `season_start_date` e `season_end_date`;
  - quebra a janela em chunks de 90 dias;
  - evita o fallback incorreto de ano civil.
- Annual seasons preservam o comportamento anterior:
  - `fixture_windows` explicitas quando configuradas;
  - `DEFAULT_FIXTURE_WINDOWS_BY_SEASON` quando existente;
  - fallback anual so quando nao houver regra mais forte.

## Fonte de verdade operacional
- Competicao onboardada: `control.competition_provider_map + control.season_catalog`
- Competicao nova sem catalogo: discovery vivo do provider
- Ausencia no discovery recente:
  - nao invalida historico catalogado
  - nao prova inexistencia para competicao nova

## Impacto no pipeline
- Escopos afetados:
  - `competition_structure`
  - `standings`
  - `fixtures` apenas no calculo de janelas split-year
- Escopos nao alterados:
  - ingestion de match depth
  - marts
  - BFF/frontend

## Validacao objetiva
- Testes:
  - `tests/test_sportmonks_provider.py`
  - `tests/test_ingestion_sync_state.py`
  - `tests/test_runtime_fixture_windows.py`
- Evidencia pos-correcao:
  - Premier League, La Liga, Bundesliga, Ligue 1 e Serie A IT ficaram com:
    - `catalog_count = 5`
    - `discovery_count = 3`
    - `resolved_count = 5`
  - ids recentes `2023` e `2024` permaneceram iguais aos ids catalogados.
  - `resolve_fixture_windows()` para Premier League `2023_24` passou a retornar a janela real:
    - `2023-08-11 -> 2023-11-08`
    - `2023-11-09 -> 2024-02-06`
    - `2024-02-07 -> 2024-05-06`
    - `2024-05-07 -> 2024-05-19`
  - Annual season `2024` do Brasileirao permaneceu:
    - `2024-04-13 -> 2024-06-30`
    - `2024-07-01 -> 2024-09-30`
    - `2024-10-01 -> 2024-12-08`

## Impacto na Liga Portugal
- O blocker historico original foi resolvido para o que ja estava catalogado e validado.
- Estado operacional atual da competicao:
  - `2023_24 -> 21825`
  - `2024_25 -> 23793`
  - baseline `2023_24` e `2024_25` congelado como `COMPLETA_NO_ESCOPO_CANONICO_DAS_2_SEASONS`
- Pendencias remanescentes:
  - `2020_21`
  - `2021_22`
  - `2022_23`
- Essas 3 seasons continuam bloqueadas exclusivamente por `provider_season_id` nao resolvido/validado.
- Portanto, a unica frente restante para expansao de `2 -> 5 seasons` e:
  - `season_id recovery`
- Nao ha motivo tecnico para reabrir o baseline ja verde de `2023_24` e `2024_25` enquanto esse recovery historico permanecer separado.
