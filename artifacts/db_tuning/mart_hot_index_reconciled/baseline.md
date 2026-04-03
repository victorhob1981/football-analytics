# DB Tuning Capture - mart_hot_index_reconciled

Generated at: `2026-04-01 15:53:00.123094+00`

## Instrumentation

- shared_preload_libraries: `pg_stat_statements`
- extension installed: `True`

## Endpoint Baseline

- `matches_list` `/api/v1/matches` mean=17.823 ms median=14.225 ms max=25.798 ms
- `match_center` `/api/v1/matches/19620974` mean=62.755 ms median=42.578 ms max=106.9 ms
- `player_profile` `/api/v1/players/929` mean=42.82 ms median=42.285 ms max=44.582 ms
- `team_profile` `/api/v1/teams/696` mean=51.079 ms median=50.947 ms max=53.042 ms
- `rankings_player_goals` `/api/v1/rankings/player-goals` mean=103.097 ms median=100.261 ms max=112.696 ms

## dbt Baseline

- total runtime from dbt.log: `21.17` s
- `model.football_analytics.fact_tie_results` 20.554 s rows=1004
- `model.football_analytics.fact_stage_progression` 0.187 s rows=1476
- `model.football_analytics.int_tie_results` 0.153 s rows=-1

## Physical Inventory

- database `football_dw` size: `6891 MB`
- `raw.fixture_lineups` size=`1518 MB` rows_est=676270
- `raw.fixture_player_statistics` size=`1425 MB` rows_est=676047
- `mart.stg_fixture_player_statistics` size=`1197 MB` rows_est=688084
- `mart.fact_fixture_lineups` size=`907 MB` rows_est=689056
- `mart.fact_fixture_player_stats` size=`728 MB` rows_est=690028
- `mart.player_match_summary` size=`225 MB` rows_est=690028
- `raw.player_season_statistics` size=`201 MB` rows_est=56384
- `mart.stg_player_season_statistics` size=`188 MB` rows_est=59847
- `mart.fact_match_events` size=`122 MB` rows_est=284210
- `raw.match_events_default` size=`48 MB` rows_est=0

## Schema Drift

- status: `confirmed_drift`
- no live-only columns detected in `raw.match_events` reconciliation.

## Top SQL

- calls=`3` total=`154.528` ms mean=`51.509` ms buffers=`19497` query=`with scoped_matches as ( select distinct fm.match_id from mart.fact_matches fm where $5=$6 and fm.league_id = any($1) and fm.season = $2 ), scoped_stats as ( select distinct pms.ma`
- calls=`3` total=`132.240` ms mean=`44.080` ms buffers=`81180` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $11) as minutes_played, coalesce(pms.`
- calls=`3` total=`132.197` ms mean=`44.066` ms buffers=`49048` query=`select fme.event_id, fme.time_elapsed as minute, cast($2 as integer) as second, cast($3 as text) as period, fme.event_type as type, fme.event_detail as detail, fme.team_id::text as`
- calls=`3` total=`26.404` ms mean=`8.801` ms buffers=`11211` query=`with scoped as ( select pms.player_id, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $7) as minutes_played, coalesce(pms.goals, $8) as goal`
- calls=`3` total=`25.456` ms mean=`8.485` ms buffers=`10632` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $13) else coalesce(fm.away_goals, $14) end as goals_for, case `
- calls=`3` total=`21.085` ms mean=`7.028` ms buffers=`6567` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $10) else coalesce(fm.away_goals, $11) end as goals_for, case `
- calls=`3` total=`16.571` ms mean=`5.524` ms buffers=`5925` query=`with scoped_matches as ( select fm.match_id, fm.provider, fm.competition_key, fm.competition_type, fm.league_id, fm.season, fm.season_label, fm.round_number, fm.round_name, fm.stag`
- calls=`3` total=`15.599` ms mean=`5.200` ms buffers=`5244` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $11) else coalesce(fm.away_goals, $12) end as goals_for, case `
- calls=`3` total=`11.852` ms mean=`3.951` ms buffers=`4488` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $9) else coalesce(fm.away_goals, $10) end as goals_for, case w`
- calls=`3` total=`11.598` ms mean=`3.866` ms buffers=`5025` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.position_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $6) as minutes_pla`

## Explain Baselines

- `match_center_player_stats` planning=`0.319` ms execution=`0.391` ms
- `player_contexts` planning=`0.211` ms execution=`1.431` ms
