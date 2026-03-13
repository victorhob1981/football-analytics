# Plano de Correção — Pós-Review Wave 4 (v2)

Status date: 2026-03-12
Versão: 2 — incorpora endurecimento pós-revisão do plano original
Veredito original: NO-GO para Wave 4 completa

Mudanças desta versão vs v1:
- H2H: defesa em camadas obrigatória (duas barreiras, não uma)
- Enrichment: proibição explícita de invenção semântica
- PSS: guardrail de domínio próprio, não dependência implícita de orquestração
- Silent drops: threshold de falha, não só log

---

## P0-A — Fail-fast global para season operacional inválido

### Diagnóstico confirmado

`runtime.py` aceita `season=202021` sem rejeitar.
O range-check existe apenas em `resolve_fixture_windows`.
Evidência: `ingest_lineups_bronze` gravou em `lineups/league=301/season=202021/...` com DAG success.

### Correção em `runtime.py`

Em `_resolve_runtime_season`, bloco `candidate.isdigit()`:

```python
if candidate.isdigit():
    parsed = int(candidate)
    if parsed < 1900 or parsed > 2100:
        raise ValueError(
            f"Parametro invalido para {field_name}: {raw_value}. "
            "Use o ano da temporada (ex.: 2024) ou uma season_label canonica "
            "(ex.: '2020/21'). IDs numericos do provider e valores como "
            "'202021' nao sao aceitos aqui."
        )
    return parsed, None
```

Em `resolve_runtime_params`, imediatamente após a linha que define `season`:

```python
if season < 1900 or season > 2100:
    raise ValueError(
        f"Parametro season invalido: {season}. "
        "Use o ano da temporada (ex.: 2024), nao o season_id numerico do provider."
    )
```

### Smoke obrigatório

```bash
# Ambos devem falhar em resolve_runtime_params antes de qualquer I/O:
airflow dags trigger ingest_lineups_bronze \
  --conf '{"league_id": 301, "season": 202021}'

airflow dags trigger ingest_head_to_head_bronze \
  --conf '{"league_id": 301, "season": 202021}'
```

Critério: task `resolve_runtime_params` falha com ValueError.
Nenhum arquivo gravado no S3. Nenhuma chamada ao provider.

---

## P0-B — Vazamento de escopo em head_to_head

### Diagnóstico confirmado

O problema está espalhado por três camadas:

- `sportmonks.py` — retorna todo o histórico do par sem filtrar
- `head_to_head_mapper.py` — aceita tudo que chega do bronze
- `warehouse_service.py` — enriquece por `season_id`, não valida contra `raw.fixtures`

Evidência no banco:
- 19.183 rows fora de `control.season_catalog`
- 23.790 rows órfãs (sem correspondência em `raw.fixtures`)
- 167 rows anteriores a 2020-08-01
- 66 rows com `competition_key NULL`
- 52 rows que conflitam com fixtures de outro escopo

### Arquitetura de defesa obrigatória: duas barreiras

A correção não pode ser só filtro no provider. Exige uma barreira antes de materializar e outra antes de persistir. Qualquer row que passe pela primeira e não passe pela segunda deve ser rejeitada, nunca silenciada.

---

#### Barreira 1 — Provider/Ingest: filtro local antes de gravar no bronze

**`sportmonks.py` — `get_head_to_head`**

Adicionar parâmetros de escopo e filtro local usando `_season_matches`, que já existe e está correto:

```python
def get_head_to_head(
    self,
    *,
    team_id: int,
    opponent_id: int,
    league_id: int | None = None,
    season: int | None = None,
    season_label: str | None = None,
    provider_season_id: int | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    ...
    mapped_rows = []
    rejected = 0
    for row in fixtures:
        if league_id is not None and str(row.get("league_id")) != str(league_id):
            rejected += 1
            continue
        if season is not None or provider_season_id is not None:
            if not self._season_matches(
                row,
                season,
                season_label=season_label,
                provider_season_id=provider_season_id,
            ):
                rejected += 1
                continue
        mapped_rows.append(self._map_h2h_row(row))

    if rejected:
        print(
            f"[h2h] {rejected} fixtures rejeitados por escopo antes do bronze "
            f"| team={team_id} opponent={opponent_id} "
            f"| league_id={league_id} season={season}"
        )
    # rejected_out_of_scope entra no provider_meta para rastreabilidade
    ...
```

**`ingestion_service.py` — `ingest_head_to_head_raw`**

```python
provider_kwargs = {
    "team_id": team_id,
    "opponent_id": opponent_id,
    "league_id": league_id,
    "season": season,
}
if provider_name == "sportmonks":
    provider_kwargs["season_label"] = season_label
    provider_kwargs["provider_season_id"] = provider_season_id

payload, headers = provider.get_head_to_head(**provider_kwargs)
```

---

#### Barreira 2 — Load: validação autoritativa contra `raw.fixtures` antes do upsert

Esta barreira é obrigatória e independente da primeira. Mesmo que a Barreira 1 funcione perfeitamente, a Barreira 2 deve existir como verificação final antes de qualquer escrita em `raw.head_to_head_fixtures`.

**`warehouse_service.py` — nova função `_validate_h2h_scope`**

```python
def _validate_h2h_scope(conn, load_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
    """
    Barreira 2 de H2H: verifica que cada fixture_id existe em raw.fixtures
    (escopo autorizado) e que competition_key não é NULL.

    Esta validação é autoritativa. Rows que falham são rejeitadas, nunca
    preenchidas ou corrigidas aqui. O preenchimento é responsabilidade das
    camadas anteriores.
    """
    df = load_df.copy()

    # Rejeitar rows sem fixture_id — não há como validar escopo
    before = len(df)
    df = df[df["fixture_id"].notna()].copy()
    if len(df) < before:
        print(
            f"[h2h_barrier2] {before - len(df)} rows sem fixture_id descartadas "
            f"| run={run_id}"
        )

    if df.empty:
        return df

    # Verificar quais fixture_ids existem em raw.fixtures
    fixture_ids = df["fixture_id"].astype("int64").unique().tolist()
    query = text(
        "SELECT fixture_id FROM raw.fixtures WHERE fixture_id = ANY(:ids)"
    )
    known_ids = {
        row[0]
        for row in conn.execute(query, {"ids": fixture_ids}).fetchall()
    }
    before = len(df)
    df = df[df["fixture_id"].isin(known_ids)].copy()
    orphan_count = before - len(df)
    if orphan_count:
        print(
            f"[h2h_barrier2] {orphan_count} rows rejeitadas: fixture_id ausente "
            f"em raw.fixtures | run={run_id}"
        )

    # Rejeitar rows com competition_key NULL após enriquecimento
    before = len(df)
    df = df[df["competition_key"].notna()].copy()
    null_key_count = before - len(df)
    if null_key_count:
        print(
            f"[h2h_barrier2] {null_key_count} rows rejeitadas: competition_key NULL "
            f"pos-enrichment | run={run_id}"
        )

    # Falhar o load se taxa de rejeição for excessiva
    total_rejected = orphan_count + null_key_count
    total_before = len(load_df)
    if total_before > 0 and (total_rejected / total_before) > 0.05:
        raise RuntimeError(
            f"[h2h_barrier2] Taxa de rejeicao excessiva: "
            f"{total_rejected}/{total_before} rows ({total_rejected/total_before:.1%}) "
            f"| run={run_id}. Verificar escopo da ingestao."
        )

    return df
```

Registrar como `pre_upsert_transform` para H2H — executada após `_enrich_with_season_identity` e antes do upsert final.

---

#### Limpeza retroativa obrigatória antes do re-run

```sql
-- 1. Remover rows órfãs (fixture_id ausente em raw.fixtures)
DELETE FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1 FROM raw.fixtures f
    WHERE f.fixture_id = h2h.fixture_id
);

-- 2. Remover rows fora de control.season_catalog
DELETE FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1 FROM control.season_catalog sc
    WHERE sc.competition_key = h2h.competition_key
      AND sc.season_label = h2h.season_label
      AND sc.provider = h2h.provider
);

-- 3. Verificar resultado (deve retornar 0 em ambas)
SELECT COUNT(*) AS orfas
FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1 FROM raw.fixtures f WHERE f.fixture_id = h2h.fixture_id
);

SELECT COUNT(*) AS fora_catalogo
FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1 FROM control.season_catalog sc
    WHERE sc.competition_key = h2h.competition_key
      AND sc.season_label = h2h.season_label
);
```

### Smoke obrigatório

```sql
-- Após re-run de H2H para Champions League 2020/21:

-- Volume esperado (~178 rows)
SELECT COUNT(*) FROM raw.head_to_head_fixtures
WHERE competition_key = 'champions_league' AND season_label = '2020_21';

-- Zero rows fora do catálogo
SELECT COUNT(*) FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1 FROM control.season_catalog sc
    WHERE sc.competition_key = h2h.competition_key
      AND sc.season_label = h2h.season_label
);

-- Zero competition_key NULL
SELECT COUNT(*) FROM raw.head_to_head_fixtures
WHERE competition_key IS NULL;

-- Zero rows anteriores ao escopo do backfill
SELECT COUNT(*) FROM raw.head_to_head_fixtures
WHERE match_date < '2020-08-01'::date;

-- Zero rows órfãs
SELECT COUNT(*) FROM raw.head_to_head_fixtures h2h
WHERE NOT EXISTS (
    SELECT 1 FROM raw.fixtures f WHERE f.fixture_id = h2h.fixture_id
);
```

Critério: todos os cinco contadores retornam 0.

---

## P1-A — player_season_statistics com guardrail de domínio próprio

### Diagnóstico confirmado

Dois problemas distintos que o plano v1 não separava com clareza suficiente:

**Problema 1:** o ingest não passa `season_label/provider_season_id` para o provider, deixando o scorer robusto inativo para este domínio.

**Problema 2:** a dependência `lineups → PSS` é garantida só pela orquestração macro. Os DAGs individuais não impõem isso. O fallback silencioso de `_fetch_player_ids_for_scope` para `match_events` pode produzir um seed parcial sem nenhum alarme.

### Correção 1: propagar parâmetros de escopo ao provider

**`ingestion_service.py` — `ingest_player_season_statistics_raw`**

```python
player_kwargs = {
    "player_id": player_id,
    "season": season,
    "league_id": league_id,
}
if provider_name == "sportmonks":
    player_kwargs["season_label"] = runtime.get("season_label")
    player_kwargs["provider_season_id"] = runtime.get("provider_season_id")

payload, headers = provider.get_player_season_statistics(**player_kwargs)
```

### Correção 2: guardrail de cobertura de lineups como contrato do domínio

O fallback silencioso de `_fetch_player_ids_for_scope` para `match_events` deve ser eliminado ou tornar-se uma falha explícita. O domínio PSS não pode ter success com seed parcial sem alarme.

```python
def ingest_player_season_statistics_raw():
    ...

    # --- GUARDRAIL DE DOMÍNIO ---
    # PSS exige lineups carregados. Verificar antes de qualquer chamada ao provider.
    lineup_count = _count_lineups_for_scope(engine, league_id=league_id, season=season)
    if lineup_count == 0:
        raise RuntimeError(
            f"Guardrail PSS: raw.fixture_lineups vazio para "
            f"league_id={league_id} season={season}. "
            "Execute ingest_lineups_bronze e carregue silver+raw antes de "
            "ingest_player_season_statistics_bronze."
        )

    # Seed exclusivo a partir de lineups — sem fallback para match_events
    player_ids = _fetch_player_ids_from_lineups_only(
        engine, league_id=league_id, season=season
    )
    if not player_ids:
        raise RuntimeError(
            f"Guardrail PSS: nenhum player_id encontrado em raw.fixture_lineups "
            f"para league_id={league_id} season={season} "
            f"apesar de lineup_count={lineup_count}. Estado inconsistente."
        )

    log_event(
        ...,
        message=(
            f"Seed PSS | player_ids={len(player_ids)} "
            f"| source=fixture_lineups_only "
            f"| lineup_rows={lineup_count} "
            f"| league_id={league_id} season={season}"
        ),
    )
    ...
```

Criar `_fetch_player_ids_from_lineups_only` — sem fallback para match_events:

```python
def _fetch_player_ids_from_lineups_only(
    engine, *, league_id: int, season: int
) -> list[int]:
    """
    Retorna player_ids exclusivamente de raw.fixture_lineups.
    Não faz fallback para match_events. PSS requer seed limpo de lineups.
    """
    sql = text(
        """
        SELECT DISTINCT fl.player_id
        FROM raw.fixture_lineups fl
        JOIN raw.fixtures f ON f.fixture_id = fl.fixture_id
        WHERE f.league_id = :league_id
          AND f.season = :season
          AND fl.player_id IS NOT NULL
        ORDER BY fl.player_id
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"league_id": league_id, "season": season}).fetchall()
    return [int(row[0]) for row in rows]


def _count_lineups_for_scope(engine, *, league_id: int, season: int) -> int:
    sql = text(
        """
        SELECT COUNT(*)
        FROM raw.fixture_lineups fl
        JOIN raw.fixtures f ON f.fixture_id = fl.fixture_id
        WHERE f.league_id = :league_id AND f.season = :season
        """
    )
    with engine.begin() as conn:
        return conn.execute(sql, {"league_id": league_id, "season": season}).scalar_one()
```

### Smoke obrigatório

```bash
# Com raw.fixture_lineups vazio para o escopo — deve falhar com RuntimeError antes
# de qualquer chamada ao provider:
airflow dags trigger ingest_player_season_statistics_bronze \
  --conf '{"league_id": 301, "season": 2024}'
```

Critério: RuntimeError com mensagem explícita de guardrail. Sem chamadas ao provider. Sem arquivos no bronze.

---

## P1-B — Enrichment semântico proibido de inventar valores

### Diagnóstico confirmado

Quando `_enrich_with_season_identity` não encontra match no catálogo, a lógica atual copia `season_id` (ID interno do provider) para `provider_season_id` e deriva `season_label` do próprio `season_id`. Isso transforma ausência de mapeamento em falsa identidade.

Evidência: rows com `season_label=24218` (ID interno do SportMonks, não um label canônico).

### Regra a impor

**Se não há match no catálogo, o campo fica NULL. Nunca se inventa.**

Isso é mais importante do que parece: um campo NULL é detectável e auditável. Um campo preenchido com valor errado passa por dado válido em todas as etapas seguintes.

### Correção em `warehouse_service.py` — `_enrich_with_season_identity`

```python
# REMOVER esta linha (propaga season_id como provider_season_id sem validação):
# df["provider_season_id"] = df["provider_season_id"].fillna(df["season_id"])

# SUBSTITUIR por: propagar só se houver mapeamento confirmado no catálogo
if "provider_season_id" in df.columns and "season_id" in df.columns:
    if not seasons_df.empty:
        # Só propagar se o season_id constar no catálogo com provider_season_id mapeado
        confirmed_map = (
            seasons_df
            .dropna(subset=["provider_season_id"])
            .drop_duplicates(subset=["provider", "season_id"], keep="last")
            .set_index("season_id")["provider_season_id"]
            .to_dict()
        )
        df["provider_season_id"] = df["provider_season_id"].fillna(
            df["season_id"].map(confirmed_map)
        )
    # Se não há catálogo ou não há mapeamento: deixar NULL.
    # Não propagar season_id diretamente.
```

### Correção em `warehouse_service.py` — validação de `season_label` após enrichment

Adicionar ao final de `_enrich_with_season_identity`, antes do `return`:

```python
def _looks_like_provider_id(label) -> bool:
    """
    Retorna True se o valor parece um ID interno do provider
    disfarçado de season_label (ex.: "24218", "21671").
    Labels válidos: "2024", "2020_21", "2020/21".
    """
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return False  # NULL é aceitável
    label_str = str(label).strip()
    # ID de provider: numérico e maior que 4 dígitos
    return label_str.isdigit() and len(label_str) > 4

if "season_label" in df.columns:
    bad_mask = df["season_label"].map(_looks_like_provider_id)
    bad_count = int(bad_mask.sum())
    if bad_count > 0:
        print(
            f"[enrichment] {bad_count} rows com season_label que parece ID de provider. "
            f"Definindo como NULL para evitar falsa identidade. run={run_id}"
        )
        df.loc[bad_mask, "season_label"] = pd.NA
```

---

## P1-C — DQ/GE incompleto para head_to_head e player_season_statistics

### Checks a adicionar em `data_quality_checks.py`

```python
def check_h2h_scope_integrity(engine, *, competition_key: str, season_label: str) -> dict:
    with engine.begin() as conn:
        orphan_count = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM raw.head_to_head_fixtures h2h
                WHERE h2h.competition_key = :ck
                  AND h2h.season_label = :sl
                  AND NOT EXISTS (
                      SELECT 1 FROM raw.fixtures f
                      WHERE f.fixture_id = h2h.fixture_id
                  )
                """
            ),
            {"ck": competition_key, "sl": season_label},
        ).scalar_one()

        null_key_count = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM raw.head_to_head_fixtures
                WHERE season_label = :sl AND competition_key IS NULL
                """
            ),
            {"sl": season_label},
        ).scalar_one()

    return {
        "passed": orphan_count == 0 and null_key_count == 0,
        "orphan_rows": orphan_count,
        "null_competition_key_rows": null_key_count,
    }


def check_pss_lineups_coverage(engine, *, league_id: int, season: int) -> dict:
    with engine.begin() as conn:
        seeded = conn.execute(
            text(
                """
                SELECT COUNT(DISTINCT fl.player_id)
                FROM raw.fixture_lineups fl
                JOIN raw.fixtures f ON f.fixture_id = fl.fixture_id
                WHERE f.league_id = :lid AND f.season = :s
                """
            ),
            {"lid": league_id, "s": season},
        ).scalar_one()

        loaded = conn.execute(
            text(
                """
                SELECT COUNT(DISTINCT player_id)
                FROM raw.player_season_statistics pss
                JOIN raw.competition_seasons cs
                  ON cs.season_id = pss.season_id
                WHERE cs.provider_league_id = :lid
                  AND pss.player_id IS NOT NULL
                """
            ),
            {"lid": league_id},
        ).scalar_one()

    coverage = (loaded / seeded * 100) if seeded > 0 else 0.0
    return {
        "passed": coverage >= 90.0,
        "seeded_from_lineups": seeded,
        "loaded_in_pss": loaded,
        "coverage_pct": round(coverage, 1),
    }
```

### Suites a adicionar em `raw_checkpoint.yml`

```yaml
suite_head_to_head_fixtures:
  data_asset_name: raw.head_to_head_fixtures
  expectations:
    - expect_column_values_to_not_be_null:
        column: fixture_id
    - expect_column_values_to_not_be_null:
        column: competition_key
    - expect_column_values_to_not_be_null:
        column: season_label
    - expect_column_values_to_be_in_set:
        column: provider
        value_set: ["sportmonks"]

suite_player_season_statistics:
  data_asset_name: raw.player_season_statistics
  expectations:
    - expect_column_values_to_not_be_null:
        column: player_id
    - expect_column_values_to_not_be_null:
        column: season_id
    - expect_column_values_to_not_be_null:
        column: season_label
```

---

## P2-A — Silent drops com threshold de falha

### Problema confirmado

`dropna`, `drop_duplicates` e remoção de rows com chave nula acontecem silenciosamente em mappers e no loader. Melhorar o logging sem definir threshold de falha é insuficiente: aceita amputação silenciosa abaixo do limite e não tem critério objetivo de rejeição.

### Correção nos mappers

Substituir cada `dropna(subset=...)` silencioso por:

```python
def _drop_with_threshold(
    df: pd.DataFrame,
    *,
    subset: list[str],
    threshold: float = 0.10,
    context_label: str,
) -> pd.DataFrame:
    """
    Executa dropna com subset e falha se a taxa de descarte ultrapassar threshold.
    threshold=0.10 significa: mais de 10% descartado é sinal de problema upstream.
    """
    before = len(df)
    df = df.dropna(subset=subset).copy()
    after = len(df)
    dropped = before - after

    if dropped > 0:
        rate = dropped / before
        print(
            f"[mapper:{context_label}] {dropped}/{before} rows descartadas "
            f"({rate:.1%}) por campo obrigatorio nulo | subset={subset}"
        )
        if rate > threshold:
            raise RuntimeError(
                f"[mapper:{context_label}] Taxa de descarte {rate:.1%} excede "
                f"threshold de {threshold:.0%}. "
                f"{dropped}/{before} rows perdidas. "
                "Verificar qualidade dos dados no bronze."
            )
    return df
```

### Correção no loader

Em `_load_generic_silver_to_raw`, após o filtro de chave nula:

```python
invalid_rows = int(invalid_mask.sum())
if invalid_rows > 0:
    discard_rate = invalid_rows / len(load_df) if len(load_df) > 0 else 0
    print(
        f"[loader] {invalid_rows} rows com chave nula descartadas "
        f"| tabela=raw.{target_table} | taxa={discard_rate:.1%} | run={run_id}"
    )
    metric.set_extra("invalid_rows_dropped", invalid_rows)

    # Threshold de falha: mais de 5% de descarte é anômalo
    if discard_rate > 0.05:
        raise RuntimeError(
            f"[loader] Taxa de descarte {discard_rate:.1%} em raw.{target_table} "
            f"excede threshold de 5% ({invalid_rows} rows). "
            "Verificar enriquecimento e qualidade do silver."
        )
```

---

## P2-B — Replay sem validação semântica (débito documentado)

### Problema

`provider_sync_state` registra se a chamada completou, não se o payload estava no escopo correto.

### Correção mínima — campo `scope_validated`

```sql
ALTER TABLE raw.provider_sync_state
  ADD COLUMN IF NOT EXISTS scope_validated BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS scope_validation_notes TEXT;
```

Atualizar `_upsert_sync_state` para aceitar e persistir `scope_validated` e `scope_validation_notes`. Domínios com Barreira 2 ativa (H2H) devem setar `scope_validated=True` após o load bem-sucedido.

---

## Ordem de execução

```
Dia 1 — P0 (bloqueadores)
  1. Aplicar fail-fast em runtime.py (duas posições)
  2. Aplicar filtro local em sportmonks.py get_head_to_head (Barreira 1)
  3. Atualizar ingestion_service.py para passar escopo ao H2H
  4. Implementar _validate_h2h_scope em warehouse_service.py (Barreira 2)
  5. Executar limpeza retroativa do banco (dois DELETEs + verificação)
  6. Rodar smoke P0-A (season=202021 deve falhar antes do bronze)
  7. Rodar smoke P0-B (Champions League 2020/21, cinco critérios SQL)

Dia 2 — P1 (contratos de domínio)
  8. Remover _fetch_player_ids_for_scope (com fallback)
  9. Criar _fetch_player_ids_from_lineups_only e _count_lineups_for_scope
  10. Adicionar guardrail de domínio em ingest_player_season_statistics_raw
  11. Propagar season_label/provider_season_id para provider.get_player_season_statistics
  12. Remover linha de fallback provider_season_id = season_id em _enrich_with_season_identity
  13. Adicionar _looks_like_provider_id e quarentena de season_label inválido
  14. Adicionar checks DQ/GE para H2H e PSS
  15. Rodar smoke PSS sem lineups (deve falhar com RuntimeError)
  16. Re-run PSS com lineups presentes — verificar cobertura >= 90%

Dia 3 — P2 + validação final
  17. Substituir dropna silencioso por _drop_with_threshold nos quatro mappers
  18. Adicionar threshold de descarte no loader
  19. Adicionar campo scope_validated em provider_sync_state
  20. Re-run Wave 4 completo
  21. Validar checklist de go/no-go
  22. Autorizar Wave 5
```

---

## Checklist de go/no-go para re-executar Wave 4

```
P0
[ ] runtime.py rejeita season=202021 com ValueError antes de qualquer I/O
[ ] H2H Champions League 2020/21 após re-run:
    [ ] zero rows com competition_key NULL
    [ ] zero rows fora de control.season_catalog
    [ ] zero rows anteriores a 2020-08-01
    [ ] zero rows sem correspondência em raw.fixtures
    [ ] volume dentro do esperado (~178 rows)

P1
[ ] ingest_pss sem lineups falha com RuntimeError explícito de guardrail
[ ] ingest_pss com lineups passa season_label/provider_season_id ao provider
[ ] _enrich_with_season_identity não gera season_label com formato de ID longo
[ ] _enrich_with_season_identity não propaga season_id como provider_season_id sem match
[ ] data_quality_checks.py inclui check_h2h_scope_integrity
[ ] data_quality_checks.py inclui check_pss_lineups_coverage

P2
[ ] mappers usam _drop_with_threshold com threshold=10%
[ ] loader expõe invalid_rows_dropped e falha acima de 5%
[ ] provider_sync_state tem campo scope_validated

Limpeza retroativa
[ ] DELETE de rows órfãs em raw.head_to_head_fixtures executado e verificado
[ ] DELETE de rows fora de catálogo em raw.head_to_head_fixtures executado e verificado
```
