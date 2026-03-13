# Revisão Técnica — Pipeline de Dados de Futebol

**Data:** 2026-03-12  
**Arquivos analisados:** `sportmonks.py`, `ingestion_service.py`, `warehouse_service.py`, `runtime.py`, `pipeline_competition.py`, `baseline_schema.sql`, `PORTFOLIO_BACKFILL_PLAN.md`

---

## Problemas Reais Encontrados

### BUG 1 — `sportmonks.py` linha 208 — Comparação errada de namespace em `_season_row_match_score` ❌ CRÍTICO

**Localização:** `_season_row_match_score`, score 300.

```python
if season is not None and identity["season_id"] == season:
    return 300
```

O parâmetro `season` é sempre um **ano** (ex.: `2024`). O campo `identity["season_id"]` é o **ID interno do SportMonks** (ex.: `21671`). São namespaces completamente diferentes.

- Essa verificação está na posição score=300, ou seja, **acima** da verificação correta `season_year == season` (score=200) e **abaixo** apenas do `provider_season_id` explícito (score=400).
- Na prática, uma colisão acidental entre um ano e um season_id é improvável mas possível em dados históricos.
- Mais importante: a verificação é semanticamente incorreta e gera confusão sobre a lógica de matching.
- O `provider_season_id` já tem sua própria verificação no score=400. Não existe caso de uso legítimo para comparar `season` (ano) com `season_id` (ID do provider).

**Correção:** Remover essa verificação inteiramente.

---

### BUG 2 — `sportmonks.py` linha 1024 — Heurística fraca em `get_team_sidelined` ❌ ALTO

**Localização:** `get_team_sidelined`.

```python
if str(season_id_raw) != str(season) and not start_date.startswith(str(season)):
    continue
```

O `season_id_raw` é o ID interno do provider e `season` é o ano. A primeira condição `str(season_id_raw) != str(season)` será quase sempre `True` (namespaces diferentes), então o filtro real cai sobre `start_date.startswith("2024")`.

Isso é exatamente a heurística fraca que causou o bug documentado no prompt (`2021 → resolvido como 2020`). Especificamente:
- Para temporadas europeias cross-year (ex.: 2020/21), `start_date` começa em `2020`. Se a requisição for para `season=2021`, o filtro exclui registros legítimos.
- `get_team_sidelined` aceita `season: int | None` mas não aceita `provider_season_id`. Como resultado, o código não pode usar o matcher robusto que existe no restante da classe.

**Correção:** Adicionar `season_label: str | None = None` e `provider_season_id: int | None = None` à assinatura. Usar `_season_row_matches` via um objeto de temporada sintético quando `provider_season_id` estiver disponível. Para o caso baseado em datas, cobrir ambos `season` e `season + 1` para acomodar temporadas cross-year.

---

### BUG 3 — `warehouse_service.py` linha 919 — Condição redundante é no-op ❌ MÉDIO

**Localização:** `_enrich_with_season_identity`.

```python
if "season_label" in df.columns and "season_label" in df:
```

A segunda condição (`"season_label" in df`) é idêntica à primeira em pandas — o operador `in` num DataFrame verifica as colunas. Ou seja, se a primeira condição for True, a segunda sempre será True também.

**Consequência real:** Quando o DataFrame chega aqui sem a coluna `season_label` (caso raro mas possível em parquets malformados), a segunda condição não adiciona proteção alguma, apenas dá falsa sensação de segurança. O bloco deveria começar com:

```python
if "season_label" in df.columns:
    fill_mask = df["season_label"].isna()
    if fill_mask.any():
```

---

### BLOQUEIO OPERACIONAL — `baseline_schema.sql` — Partições históricas ausentes em `raw.match_events` 🚨 BLOQUEANTE

O backfill cobre temporadas de **2020 a 2025**, mas o schema SQL define apenas a partição de 2024:

```sql
CREATE TABLE IF NOT EXISTS raw.match_events_2024
  PARTITION OF raw.match_events
  FOR VALUES IN (2024);
```

Inserções em `raw.match_events` com `season IN (2020, 2021, 2022, 2023, 2025)` falharão com erro do PostgreSQL:

> `ERROR: no partition of relation "match_events" found for row`

**Partições que precisam existir para o backfill:**
- `raw.match_events_2020` — Europa 2020/21
- `raw.match_events_2021` — Europa 2021/22, Brasil 2021
- `raw.match_events_2022` — Europa 2022/23, Brasil 2022
- `raw.match_events_2023` — Europa 2023/24, Brasil 2023
- `raw.match_events_2025` — Brasil 2025

---

### RISCO — `ingestion_service.py` — `requests_used` subestima chamadas reais na ingestão de fixtures ⚠️ MÉDIO

**Localização:** `ingest_fixtures_raw`, linha ~484.

```python
for date_from, date_to in windows:
    ...
    payload, headers = provider.get_fixtures(...)
    requests_used += 1  # incrementa 1 por janela
```

`provider.get_fixtures` internamente pagina via `_paginate_fixtures_between`, fazendo N requisições ao provider. O contador `requests_used` incrementa apenas 1 por janela de datas, independentemente de quantas páginas foram necessárias.

Para uma janela de 3 meses de Premier League, a paginação pode fazer 5-15 requisições. O log reporta `requests=3` quando o custo real foi `≥ 45`. **Isso compromete o planejamento de orçamento diário de requisições**, que é crítico com o teto de 40k/dia.

**Correção:** `_paginate_fixtures_between` deveria retornar o número de páginas consumidas. `ingest_fixtures_raw` deveria somar esse valor, não incrementar cegamente em 1.

---

### RISCO — `ingestion_service.py` — `ingest_statistics_raw` e `ingest_match_events_raw` têm loop de fixture duplicado ⚠️ MÉDIO

`ingest_fixture_lineups_raw` e `ingest_fixture_player_statistics_raw` usam corretamente o helper `_ingest_entity_by_numeric_ids`. Já `ingest_statistics_raw` e `ingest_match_events_raw` duplicam manualmente o mesmo padrão stateful (cursor, S3 scan, consecutive_failures, etc.) com ~200 linhas cada.

A diferença é que statistics/events suportam `skip_ingested` com fallback de scan S3, enquanto o helper usa apenas cursor. Mas isso pode ser adicionado ao helper em vez de manter duas implementações paralelas.

**Risco concreto:** Se o comportamento do cursor ou da lógica de `consecutive_failures` precisar ser ajustado, o desenvolvedor deve lembrar de aplicar a mudança em dois lugares. O histórico de bugs futuros estará oculto nessa duplicação.

---

### MELHORIA — `pipeline_competition.py` — `poke_interval=10` é excessivo para backfill ⚠️ BAIXO

```python
def trigger_task(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        ...
        poke_interval=10,
        ...
    )
```

Com `poke_interval=10` segundos, uma task de backfill que leve 2 horas gera ~720 polls ao Airflow scheduler. Para backfills de múltiplas competições em sequência, esse overhead acumula. **Recomendado:** 30 segundos para uso normal, 60 segundos para o contexto de backfill.

---

## O Que Está Bem

- **`runtime.py`** — Robusto após a correção do matcher de temporada. A lógica de `_resolve_runtime_season` e `derive_season_label` está correta. A validação de range `1900 <= season <= 2100` em `resolve_fixture_windows` é um guard correto.
- **`warehouse_service.py` — lógica de upsert** — A estratégia de staging table + `ON CONFLICT DO UPDATE WHERE ... IS DISTINCT FROM` é correta e evita updates desnecessários (skip-if-equal). A função `_load_generic_silver_to_raw` é bem parametrizada.
- **`ingestion_service.py` — cursor e sync state** — O design de `_read_sync_cursor` / `_upsert_sync_state` / `_calculate_next_cursor` é correto para replay seguro. O fallback para full-scan S3 quando o cursor está ausente é uma boa decisão de resiliência.
- **`sportmonks.py` — score matching geral** — Exceto pelo BUG 1, o sistema de scores prioritizando `provider_season_id > season_label > season_year` está correto e é exatamente o que o prompt descreve como hardening necessário.
- **`PORTFOLIO_BACKFILL_PLAN.md`** — O plano é operacionalmente sólido. A separação em waves com budget explícito, a regra de não usar o DAG completo, e a prioridade de lineups antes de player_season_statistics são todas decisões corretas.

---

## Arquivos Gerados

| Arquivo | Status |
|---|---|
| `sportmonks_fixed.py` | Patches para BUG 1 e BUG 2 |
| `warehouse_service_fixed.py` | Patch para BUG 3 |
| `migration_match_events_partitions.sql` | Partições faltantes |
| `ingestion_service_fixtures_counter_fix.py` | Correção do contador de requests |

---

## Resumo de Prioridade de Execução

| # | Problema | Impacto | Arquivo |
|---|---|---|---|
| 1 | Partições históricas ausentes em `match_events` | 🚨 Bloqueia o backfill de Ondas 6 e 7 | `baseline_schema.sql` |
| 2 | Score=300 compara namespace errado | ❌ Potencial match incorreto de temporada | `sportmonks.py` |
| 3 | Filtro fraco em `get_team_sidelined` | ❌ Filtragem incorreta de dados de lesão | `sportmonks.py` |
| 4 | Condição redundante em `_enrich_with_season_identity` | ❌ Enriquecimento silencioso pode falhar | `warehouse_service.py` |
| 5 | Contador de requests subestima paginação | ⚠️ Planejamento de orçamento comprometido | `ingestion_service.py` |
| 6 | Loop de fixture duplicado | ⚠️ Manutenção — risco de divergência futura | `ingestion_service.py` |
| 7 | `poke_interval` excessivo | ⚠️ Overhead de scheduler no Airflow | `pipeline_competition.py` |
