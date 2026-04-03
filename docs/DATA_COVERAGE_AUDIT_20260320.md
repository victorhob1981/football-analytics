# Data Coverage Audit — Portfolio Backfill

Data de referencia: `2026-03-20`  
Projeto: `football-analytics`

## Metodo (ordem obrigatoria aplicada)

1. **FASE 1 — Realidade da base**: consultas SQL diretas no `football_dw` (`raw`, `mart`, `control`), sem assumir plano/documentacao como verdade.
2. **FASE 2 — Comparacao com planejamento**: confronto do estado real contra o planejamento operacional vigente na data da auditoria, hoje consolidado em `docs/WAVES_4_TO_7_FINAL_CLOSURE.md`, e artefatos operacionais de waves.

Evidencias brutas desta auditoria:

- `artifacts/coverage_audit_20260320/base_totals.csv`
- `artifacts/coverage_audit_20260320/structure_scope_matrix.csv`
- `artifacts/coverage_audit_20260320/raw_scope_domain_matrix.csv`
- `artifacts/coverage_audit_20260320/mart_scope_matrix.csv`
- `artifacts/coverage_audit_20260320/plan_scope_comparison.csv`

---

## 1) Estado real da base hoje

### 1.1 Escopo materializado (factual)

- competicoes em `raw.competition_leagues`: `10`
- escopos competicao-temporada em `raw.competition_seasons`: `50`
- fixtures totais em `raw.fixtures`: `15265`
- fixtures finalizados (`FT/AET/PEN/FTP`): `15262`

### 1.2 Cobertura factual dos dominios principais (raw)

Com base em `fixtures` finalizados:

- `match_statistics`: `15216 / 15262` (`missing=46`)
- `head_to_head`: `15262 / 15262` (`missing=0`)
- `lineups`: `15183 / 15262` (`missing=79`)
- `match_events`: `15257 / 15262` (`missing=5`)
- `fixture_player_statistics`: `15217 / 15262` (`missing=45`)

Cobertura de `player_season_statistics` (seed por lineup players):

- lineup players distintos: `20775`
- players distintos em `raw.player_season_statistics`: `20722`
- gap agregado: `53`

### 1.3 Integridade semantica (raw)

- `orphan_rows` (dominios auditados): `0`
- `outside_catalog_rows` (`match_statistics`, `lineups`, `fixture_player_statistics`): `0`
- `match_events_duplicate_groups`: `0`

Observacoes relevantes:

- `raw.fixture_lineups` tem `3103` rows com `player_id` nulo (caveat de payload/cobertura).
- `raw.fixture_player_statistics` tem inconsistencia semantica de metadata:
  - `provider_season_id` nulo em `651169/651169` rows
  - `competition_key/season_label/provider_league_id` nulos em `633940/651169` rows

### 1.4 Camada mart (end-to-end de produto)

`mart` nao acompanha o escopo portfolio atual:

- matriz por escopo em `artifacts/coverage_audit_20260320/mart_scope_matrix.csv`
- status consolidado:
  - `NÃO INGESTADO`: `49` escopos
  - `PARCIAL`: `1` escopo (`brasileirao_a / 2024`)
  - `COMPLETO`: `0` escopos

---

## 2) Matriz de cobertura factual

### 2.1 Matriz completa por competicao/temporada/dominio

Arquivo completo (50 escopos, com colunas de cobertura e status por dominio):

- `artifacts/coverage_audit_20260320/raw_scope_domain_matrix.csv`

Status agregados por dominio nessa matriz:

- `match_statistics`: `39 COMPLETO`, `11 PARCIAL`
- `head_to_head`: `50 COMPLETO`
- `lineups`: `37 COMPLETO`, `10 PARCIAL`, `3 PROVIDER_COVERAGE_GAP`
- `player_season_statistics`: `28 COMPLETO`, `22 PARCIAL`
- `match_events`: `47 COMPLETO`, `3 PROVIDER_COVERAGE_GAP`
- `fixture_player_statistics`: `44 COMPLETO`, `2 PARCIAL`, `4 PROVIDER_COVERAGE_GAP`

### 2.2 Escopos nao-completos (recorte objetivo)

`match_statistics` parciais:

- `brasileirao_a/2021`
- `champions_league/2020_21`, `2021_22`, `2022_23`, `2023_24`, `2024_25`
- `libertadores/2021`, `2022`, `2023`, `2024`, `2025`

`lineups` parciais ou caveat:

- `PROVIDER_COVERAGE_GAP`: `copa_do_brasil/2021`, `2022`, `2023`
- `PARCIAL`: `champions_league/2020_21..2024_25`, `libertadores/2021..2025`

`match_events` com caveat:

- `PROVIDER_COVERAGE_GAP`: `brasileirao_a/2021`, `brasileirao_a/2022`, `champions_league/2020_21`

`fixture_player_statistics` parciais/caveat:

- `PROVIDER_COVERAGE_GAP`: `copa_do_brasil/2021`, `2022`, `2023`; `brasileirao_b/2024`
- `PARCIAL`: `brasileirao_a/2024`, `champions_league/2024_25`

`player_season_statistics` parciais:

- `22` escopos com gap de players (ver detalhes na matriz CSV)
- maiores gaps: `brasileirao_a/2025 (13)`, `ligue_1/2024_25 (10)`, `brasileirao_b/2025 (9)`, `libertadores/2025 (7)`

---

## 3) Comparacao contra o plano

Base de comparacao:

- fechamento consolidado em `docs/WAVES_4_TO_7_FINAL_CLOSURE.md`

### 3.1 Escopo (competicao + temporada)

- esperado pelo plano: `50` escopos
- encontrado em `raw.competition_seasons`: `50`
- `missing_in_actual=0`
- `extra_in_actual=0`
- evidencia: `artifacts/coverage_audit_20260320/plan_scope_comparison.csv`

### 3.2 Dominios planejados vs base real

Planejamento de waves:

- Waves 1-2: structure/standings/fixtures
- Waves 3-5: `match_statistics`, `head_to_head`, `lineups`, `player_season_statistics`
- Waves 6-7: `match_events`, `fixture_player_statistics`

Confronto factual:

- todos os dominios planejados existem e estao materializados no `raw` para os 50 escopos
- parte dos dominios tem cobertura parcial (missing por fixture/player), sem evidencia atual de contaminação semantica (`orphan/outside_catalog/duplicidade` = 0)
- em `mart`, o escopo portfolio **nao** foi materializado (49 escopos nao ingeridos no recorte end-to-end)

### 3.3 Onde bate / onde falta / onde sobra

- **Bate**: escopo planejado de competicoes e temporadas (50/50).
- **Falta**: completude 100% por fixture/player em alguns dominios raw; cobertura mart portfolio.
- **Sobra**: nao foram detectados escopos fora do plano em `raw.competition_seasons`.

---

## 4) Caveats residuais

## 4.1 PROVIDER_COVERAGE_GAP

Documentados em artefatos/runsheets:

- Wave 6 `match_events`: residual final `5` fixtures (`brasileirao_a/2021`, `brasileirao_a/2022`, `champions_league/2020_21`)
- Wave 7 `fixture_player_statistics`:
  - `copa_do_brasil/2021-2023` (`36` fixtures, payload vazio com `results=0`, `errors=[]`, `response=[]`)
  - `brasileirao_b/2024` residual `7` por payload sem `player.id`
- Wave 5 `lineups` em `copa_do_brasil/2021-2023` classificado como caveat de coverage

## 4.2 PIPELINE_BUG / INCONSISTÊNCIA

Sem bug ativo de integridade em `raw` (orphan/outside_catalog/duplicates = 0), mas com inconsistencias de estado:

- `raw.fixture_player_statistics` com metadata semantica incompleta (nulos massivos em colunas de escopo, incluindo `provider_season_id`).
- `raw.provider_sync_state` com estado final `idle` em `12` combinações dominio/escopo (apesar de materializacao existente).
- camada `mart` desalinhada com o escopo portfolio (49 escopos não ingeridos no recorte end-to-end).

## 4.3 DÍVIDA DOCUMENTAL

- Ha documentos de inventario/manual com snapshots antigos e claims que nao refletem a base atual.
- Esta auditoria usa a base real como fonte de verdade e registra os drifts.

## 4.4 DÍVIDA OPERACIONAL NÃO BLOQUEANTE

Conforme runbook consolidado:

- instabilidade eventual de `airflow-webserver` por PID stale
- `dbmate` podendo falhar no compose

Nao bloqueiam o estado de fechamento das waves ja validadas.

---

## 5) Veredito final

### Pergunta: o plano vigente foi integralmente ingerido?

Resposta objetiva em dois niveis:

1. **Ingestao de escopo (raw, por waves/dominios planejados)**: **SIM**, no sentido de execucao de todos os dominios planejados nos 50 escopos, com caveats residuais classificados.
2. **Cobertura 100% + end-to-end warehouse (incluindo mart pronto para produto)**: **NÃO**.
   - faltas de cobertura residual permanecem em alguns dominios raw;
   - `mart` nao foi materializado para o portfolio completo.

### O que exatamente falta para dizer “integral” end-to-end

- fechar gap de materializacao de `mart` para os 50 escopos;
- tratar inconsistencia semantica em `raw.fixture_player_statistics` (colunas de escopo);
- reconciliar drift de `provider_sync_state` (`idle` em escopos com dados).

### Caveats que produto/frontend/portfolio devem conhecer

- gaps residuais classificados como `PROVIDER_COVERAGE_GAP` existem e devem aparecer como estado de cobertura, nao como erro de sistema.

---

## 6) Próximas ações recomendadas (prioridade)

1. **Prioridade alta — End-to-end**  
   Executar/materializar `mart` para escopo portfolio completo (50 escopos), com validação por escopo e cobertura.

2. **Prioridade alta — Coerência semântica**  
   Reconciliar metadata semântica de `raw.fixture_player_statistics` (`competition_key`, `season_label`, `provider_league_id`, `provider_season_id`) de forma idempotente e auditável.

3. **Prioridade média — Estado operacional**  
   Revisar regra de atualização final do `provider_sync_state` para remover estados `idle` residuais quando domínio já está materializado.

4. **Prioridade média — Classificação fina de PARCIAIS**  
   Para escopos `PARCIAL` ainda sem evidência de payload, fazer auditoria amostral bronze/silver para separar formalmente provider gap vs eventual regra de pipeline.

5. **Prioridade baixa — Documentação**  
   Atualizar inventário/manual frontend para refletir esta auditoria factual e separar claramente raw coverage vs product exposure.
