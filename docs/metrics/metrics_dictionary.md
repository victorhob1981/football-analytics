# Metrics Dictionary (BI)

Este documento define as métricas canônicas usadas nos dashboards do projeto.
Todas as consultas usam apenas modelos dbt finais (`fact_`, `dim_`, `marts.analytics`), sem `raw`.

## Convenções globais
- **Jogos válidos**: `home_goals IS NOT NULL AND away_goals IS NOT NULL`.
- **Ordenação temporal**: `date_day ASC` para histórico e `date_day DESC, match_id DESC` para “recente”.
- **Escopo padrão**: temporada (`season`) e competição (`competition_sk` ou `league_id` via `dim_competition`).

---

## 1) Ranking Mensal

**Nome**: `ranking_mensal`  
**Descrição**: ranking por time/mês com pontos, vitórias, empates, derrotas, saldo, gols pró e gols contra.  
**Grão**: `temporada + ano + mês + time`  
**Filtros padrão**:
- temporada obrigatória
- competição opcional (via join com `fact_matches`)
- somente meses com `matches > 0`
**Fórmula**:
- `points = wins * 3 + draws`
- `goal_diff = goals_for - goals_against`
**Fontes dbt**:
- `marts.analytics.team_monthly_stats`
- `marts.core.fact_matches` (quando filtrar competição)
**Notas / edge cases**:
- empate em ranking: ordenar por `points DESC, goal_diff DESC, goals_for DESC, team_name ASC`.

### SQL canônico
```sql
SELECT
  tms.season,
  tms.year,
  tms.month,
  tms.team_name,
  tms.matches,
  tms.wins,
  tms.draws,
  tms.losses,
  tms.points,
  tms.goal_diff,
  tms.goals_for,
  tms.goals_against
FROM mart.team_monthly_stats tms
WHERE tms.season = :season
  AND tms.matches > 0
ORDER BY
  tms.year::int DESC,
  tms.month::int DESC,
  tms.points DESC,
  tms.goal_diff DESC,
  tms.goals_for DESC,
  tms.team_name ASC;
```

---

## 2) Forma Recente

**Nome**: `forma_recente`  
**Descrição**: desempenho dos últimos `N` jogos de cada time.  
**Grão**: `time` (com base nos últimos N jogos por time)  
**Filtros padrão**:
- `N` configurável (`:n_games`, padrão sugerido `5`)
- temporada obrigatória
- somente jogos válidos
**Fórmula**:
- por jogo: `W=3`, `D=1`, `L=0`
- agregado recente: soma de pontos, vitórias, empates, derrotas, gols pró/contra
**Fontes dbt**:
- `marts.core.fact_matches`
- `marts.core.dim_team`
**Notas / edge cases**:
- desempate recente: `points_last_n DESC, goal_diff_last_n DESC, goals_for_last_n DESC`.

### SQL canônico
```sql
WITH team_matches AS (
  SELECT
    fm.season,
    fm.match_id,
    fm.date_day,
    fm.home_team_sk AS team_sk,
    dt.team_name,
    fm.home_goals AS goals_for,
    fm.away_goals AS goals_against
  FROM mart.fact_matches fm
  JOIN mart.dim_team dt
    ON dt.team_sk = fm.home_team_sk
  WHERE fm.season = :season
    AND fm.home_goals IS NOT NULL
    AND fm.away_goals IS NOT NULL

  UNION ALL

  SELECT
    fm.season,
    fm.match_id,
    fm.date_day,
    fm.away_team_sk AS team_sk,
    dt.team_name,
    fm.away_goals AS goals_for,
    fm.home_goals AS goals_against
  FROM mart.fact_matches fm
  JOIN mart.dim_team dt
    ON dt.team_sk = fm.away_team_sk
  WHERE fm.season = :season
    AND fm.home_goals IS NOT NULL
    AND fm.away_goals IS NOT NULL
),
ranked AS (
  SELECT
    tm.*,
    ROW_NUMBER() OVER (
      PARTITION BY tm.season, tm.team_sk
      ORDER BY tm.date_day DESC, tm.match_id DESC
    ) AS rn
  FROM team_matches tm
),
last_n AS (
  SELECT *
  FROM ranked
  WHERE rn <= :n_games
)
SELECT
  season,
  team_sk,
  team_name,
  COUNT(*) AS games_last_n,
  SUM(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END) AS wins_last_n,
  SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) AS draws_last_n,
  SUM(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END) AS losses_last_n,
  SUM(CASE WHEN goals_for > goals_against THEN 3 WHEN goals_for = goals_against THEN 1 ELSE 0 END) AS points_last_n,
  SUM(goals_for) AS goals_for_last_n,
  SUM(goals_against) AS goals_against_last_n,
  SUM(goals_for - goals_against) AS goal_diff_last_n
FROM last_n
GROUP BY season, team_sk, team_name
ORDER BY points_last_n DESC, goal_diff_last_n DESC, goals_for_last_n DESC, team_name ASC;
```

---

## 3) Desempenho Casa/Fora

**Nome**: `desempenho_casa_fora`  
**Descrição**: comparação de performance como mandante vs visitante por time.  
**Grão**: `temporada + time + contexto (home/away)`  
**Filtros padrão**:
- temporada obrigatória
- somente jogos válidos
**Fórmula**:
- por contexto: jogos, vitórias, empates, derrotas, pontos, gols pró/contra, saldo
**Fontes dbt**:
- `marts.core.fact_matches`
- `marts.core.dim_team`
**Notas / edge cases**:
- contexto fechado: `home` ou `away`.

### SQL canônico
```sql
WITH home_stats AS (
  SELECT
    fm.season,
    fm.home_team_sk AS team_sk,
    dt.team_name,
    'home'::text AS context,
    COUNT(*) AS matches,
    SUM(CASE WHEN fm.home_goals > fm.away_goals THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN fm.home_goals = fm.away_goals THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN fm.home_goals < fm.away_goals THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN fm.home_goals > fm.away_goals THEN 3 WHEN fm.home_goals = fm.away_goals THEN 1 ELSE 0 END) AS points,
    SUM(fm.home_goals) AS goals_for,
    SUM(fm.away_goals) AS goals_against
  FROM mart.fact_matches fm
  JOIN mart.dim_team dt ON dt.team_sk = fm.home_team_sk
  WHERE fm.season = :season
    AND fm.home_goals IS NOT NULL
    AND fm.away_goals IS NOT NULL
  GROUP BY fm.season, fm.home_team_sk, dt.team_name
),
away_stats AS (
  SELECT
    fm.season,
    fm.away_team_sk AS team_sk,
    dt.team_name,
    'away'::text AS context,
    COUNT(*) AS matches,
    SUM(CASE WHEN fm.away_goals > fm.home_goals THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN fm.away_goals = fm.home_goals THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN fm.away_goals < fm.home_goals THEN 1 ELSE 0 END) AS losses,
    SUM(CASE WHEN fm.away_goals > fm.home_goals THEN 3 WHEN fm.away_goals = fm.home_goals THEN 1 ELSE 0 END) AS points,
    SUM(fm.away_goals) AS goals_for,
    SUM(fm.home_goals) AS goals_against
  FROM mart.fact_matches fm
  JOIN mart.dim_team dt ON dt.team_sk = fm.away_team_sk
  WHERE fm.season = :season
    AND fm.home_goals IS NOT NULL
    AND fm.away_goals IS NOT NULL
  GROUP BY fm.season, fm.away_team_sk, dt.team_name
)
SELECT
  season,
  team_sk,
  team_name,
  context,
  matches,
  wins,
  draws,
  losses,
  points,
  goals_for,
  goals_against,
  (goals_for - goals_against) AS goal_diff
FROM (
  SELECT * FROM home_stats
  UNION ALL
  SELECT * FROM away_stats
) s
ORDER BY team_name, context;
```

---

## 4) Gols por Minuto (Buckets)

**Nome**: `gols_por_minuto_bucket`  
**Descrição**: distribuição de gols por faixa de minuto.  
**Grão**: `temporada + bucket_minuto`  
**Filtros padrão**:
- temporada obrigatória
- somente eventos de gol (`is_goal = true`)
- minutos válidos (`time_elapsed` não nulo)
**Fórmula**:
- bucketização fixa:
  - `00-15`, `16-30`, `31-45`, `46-60`, `61-75`, `76-90`, `91-105`, `106-120`
**Fontes dbt**:
- `marts.core.fact_match_events`
- `marts.core.fact_matches`
**Notas / edge cases**:
- usa `time_elapsed` (não soma acréscimo de `time_extra` no bucket principal).

### SQL canônico
```sql
WITH goals AS (
  SELECT
    fm.season,
    fme.time_elapsed
  FROM mart.fact_match_events fme
  JOIN mart.fact_matches fm
    ON fm.match_id = fme.match_id
  WHERE fm.season = :season
    AND fme.is_goal = TRUE
    AND fme.time_elapsed IS NOT NULL
    AND fme.time_elapsed BETWEEN 0 AND 120
),
bucketed AS (
  SELECT
    season,
    CASE
      WHEN time_elapsed BETWEEN 0 AND 15 THEN '00-15'
      WHEN time_elapsed BETWEEN 16 AND 30 THEN '16-30'
      WHEN time_elapsed BETWEEN 31 AND 45 THEN '31-45'
      WHEN time_elapsed BETWEEN 46 AND 60 THEN '46-60'
      WHEN time_elapsed BETWEEN 61 AND 75 THEN '61-75'
      WHEN time_elapsed BETWEEN 76 AND 90 THEN '76-90'
      WHEN time_elapsed BETWEEN 91 AND 105 THEN '91-105'
      ELSE '106-120'
    END AS minute_bucket
  FROM goals
)
SELECT
  season,
  minute_bucket,
  COUNT(*) AS goals
FROM bucketed
GROUP BY season, minute_bucket
ORDER BY
  season,
  CASE minute_bucket
    WHEN '00-15' THEN 1
    WHEN '16-30' THEN 2
    WHEN '31-45' THEN 3
    WHEN '46-60' THEN 4
    WHEN '61-75' THEN 5
    WHEN '76-90' THEN 6
    WHEN '91-105' THEN 7
    ELSE 8
  END;
```

---

## Alinhamento com dashboards versionados
- Dashboards alvo:
  - `Ranking Mensal`
  - `Forma Recente`
  - `Desempenho Casa/Fora`
  - `Gols por Minuto`
- Para manter consistência, os filtros padrão acima devem ser replicados nas perguntas/cards do Metabase.
