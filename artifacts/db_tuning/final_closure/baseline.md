# DB Tuning Capture - final_closure

Generated at: `2026-04-01 15:36:54.752836+00`

## Instrumentation

- shared_preload_libraries: `pg_stat_statements`
- extension installed: `True`

## Endpoint Baseline

- `matches_list` `/api/v1/matches` mean=20.365 ms median=13.951 ms max=34.271 ms
- `match_center` `/api/v1/matches/19620974` mean=344.99 ms median=138.992 ms max=767.843 ms
- `player_profile` `/api/v1/players/929` mean=228.556 ms median=232.443 ms max=240.478 ms
- `team_profile` `/api/v1/teams/696` mean=55.925 ms median=48.843 ms max=70.243 ms
- `rankings_player_goals` `/api/v1/rankings/player-goals` mean=74.931 ms median=75.051 ms max=75.312 ms

## dbt Baseline

- total runtime from dbt.log: `21.17` s
- `model.football_analytics.fact_tie_results` 20.554 s rows=1004
- `model.football_analytics.fact_stage_progression` 0.187 s rows=1476
- `model.football_analytics.int_tie_results` 0.153 s rows=-1

## Physical Inventory

- database `football_dw` size: `6859 MB`
- `raw.fixture_lineups` size=`1518 MB` rows_est=676270
- `raw.fixture_player_statistics` size=`1425 MB` rows_est=676047
- `mart.stg_fixture_player_statistics` size=`1197 MB` rows_est=688084
- `mart.fact_fixture_lineups` size=`907 MB` rows_est=689056
- `mart.fact_fixture_player_stats` size=`722 MB` rows_est=690264
- `raw.player_season_statistics` size=`201 MB` rows_est=56384
- `mart.player_match_summary` size=`198 MB` rows_est=690028
- `mart.stg_player_season_statistics` size=`188 MB` rows_est=59847
- `mart.fact_match_events` size=`122 MB` rows_est=284210
- `raw.match_events_default` size=`48 MB` rows_est=0

## Schema Drift

- status: `confirmed_drift`
- no live-only columns detected in `raw.match_events` reconciliation.

## Top SQL

- calls=`3` total=`583.244` ms mean=`194.415` ms buffers=`49098` query=`select fme.event_id, fme.time_elapsed as minute, cast($2 as integer) as second, cast($3 as text) as period, fme.event_type as type, fme.event_detail as detail, fme.team_id::text as`
- calls=`3` total=`384.089` ms mean=`128.030` ms buffers=`276315` query=`select fps.player_id::text as player_id, fps.player_name, fps.team_id::text as team_id, coalesce(fps.team_name, team.team_name) as team_name, fps.position_name, fps.is_starter, fps`
- calls=`3` total=`207.825` ms mean=`69.275` ms buffers=`118824` query=`with scoped as ( select pms.player_id, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $7) as minutes_played, coalesce(pms.goals, $8) as goal`
- calls=`3` total=`107.582` ms mean=`35.861` ms buffers=`80520` query=`select count(distinct pms.match_id) as count_matches from mart.player_match_summary pms inner join mart.fact_matches fm on fm.match_id = pms.match_id where pms.player_id = $1 and $`
- calls=`3` total=`104.788` ms mean=`34.929` ms buffers=`81183` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $11) as minutes_played, coalesce(pms.`
- calls=`3` total=`104.093` ms mean=`34.698` ms buffers=`80910` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.position_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $6) as minutes_pla`
- calls=`3` total=`100.297` ms mean=`33.432` ms buffers=`81273` query=`with scoped as ( select pms.match_id, pms.match_date, coalesce(pms.minutes_played, $6) as minutes_played, coalesce(pms.goals, $7) as goals, coalesce(pms.assists, $8) as assists, co`
- calls=`3` total=`99.093` ms mean=`33.031` ms buffers=`85437` query=`with scoped_matches as ( select distinct fm.match_id from mart.fact_matches fm where $5=$6 and fm.league_id = any($1) and fm.season = $2 ), scoped_stats as ( select distinct pms.ma`
- calls=`3` total=`95.180` ms mean=`31.727` ms buffers=`77097` query=`with player_history as ( select dc.league_id, dc.league_name, pms.season, pms.team_id, coalesce(max(pms.team_name), dt.team_name) as team_name, count(distinct pms.match_id)::int as`
- calls=`3` total=`30.033` ms mean=`10.011` ms buffers=`6567` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $10) else coalesce(fm.away_goals, $11) end as goals_for, case `

## Explain Baselines

- `match_center_player_stats` planning=`0.521` ms execution=`1322.409` ms
- `player_contexts` planning=`0.385` ms execution=`37.068` ms
