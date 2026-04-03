# DB Tuning Capture - wave1_after

Generated at: `2026-04-01 00:37:15.070674+00`

## Instrumentation

- shared_preload_libraries: `pg_stat_statements`
- extension installed: `True`

## Endpoint Baseline

- `matches_list` `/api/v1/matches` mean=18.207 ms median=14.299 ms max=26.726 ms
- `match_center` `/api/v1/matches/19620974` mean=48.324 ms median=49.286 ms max=51.01 ms
- `player_profile` `/api/v1/players/929` mean=49.462 ms median=48.002 ms max=54.787 ms
- `team_profile` `/api/v1/teams/696` mean=52.96 ms median=52.038 ms max=57.681 ms
- `rankings_player_goals` `/api/v1/rankings/player-goals` mean=96.871 ms median=94.762 ms max=104.409 ms

## dbt Baseline

- total runtime from dbt.log: `283.04` s
- `model.football_analytics.dim_player` 146.554 s rows=22174
- `model.football_analytics.fact_fixture_player_stats` 112.2 s rows=96732
- `model.football_analytics.player_season_summary` 10.637 s rows=54557
- `model.football_analytics.player_match_summary` 2.714 s rows=675734
- `model.football_analytics.fact_matches` 2.11 s rows=2293
- `model.football_analytics.fact_tie_results` 1.803 s rows=921
- `model.football_analytics.dim_tie` 1.773 s rows=921
- `model.football_analytics.head_to_head_summary` 0.321 s rows=4145
- `model.football_analytics.competition_season_config` 0.251 s rows=20
- `model.football_analytics.dim_competition` 0.219 s rows=12

## Physical Inventory

- database `football_dw` size: `5944 MB`
- `raw.fixture_lineups` size=`1499 MB` rows_est=676270
- `raw.fixture_player_statistics` size=`1398 MB` rows_est=676047
- `mart.fact_fixture_player_stats` size=`1272 MB` rows_est=675734
- `mart.fact_fixture_lineups` size=`906 MB` rows_est=674758
- `mart.player_match_summary` size=`220 MB` rows_est=675734
- `raw.player_season_statistics` size=`191 MB` rows_est=56384
- `mart.fact_match_events` size=`122 MB` rows_est=278616
- `raw.match_events_default` size=`115 MB` rows_est=220132
- `raw.head_to_head_fixtures` size=`45 MB` rows_est=15877
- `raw.match_events_2024` size=`34 MB` rows_est=58484

## Schema Drift

- status: `confirmed_drift`
- live columns not referenced in committed `raw.match_events` migrations: `provider`, `provider_league_id`, `competition_key`, `season_label`, `provider_season_id`, `source_run_id`

## Top SQL

- calls=`3` total=`134.121` ms mean=`44.707` ms buffers=`79650` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $11) as minutes_played, coalesce(pms.`
- calls=`3` total=`133.028` ms mean=`44.343` ms buffers=`19285` query=`with scoped_matches as ( select distinct fm.match_id from mart.fact_matches fm where $5=$6 and fm.league_id = any($1) and fm.season = $2 ), scoped_stats as ( select distinct pms.ma`
- calls=`3` total=`85.067` ms mean=`28.356` ms buffers=`48978` query=`select fme.event_id, fme.time_elapsed as minute, cast($2 as integer) as second, cast($3 as text) as period, fme.event_type as type, fme.event_detail as detail, fme.team_id::text as`
- calls=`3` total=`31.151` ms mean=`10.384` ms buffers=`10596` query=`with scoped as ( select pms.player_id, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $7) as minutes_played, coalesce(pms.goals, $8) as goal`
- calls=`3` total=`24.748` ms mean=`8.249` ms buffers=`10056` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $13) else coalesce(fm.away_goals, $14) end as goals_for, case `
- calls=`3` total=`19.987` ms mean=`6.662` ms buffers=`6471` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $10) else coalesce(fm.away_goals, $11) end as goals_for, case `
- calls=`3` total=`17.505` ms mean=`5.835` ms buffers=`5244` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $11) else coalesce(fm.away_goals, $12) end as goals_for, case `
- calls=`3` total=`14.713` ms mean=`4.904` ms buffers=`5931` query=`with scoped_matches as ( select fm.match_id, fm.provider, fm.competition_key, fm.competition_type, fm.league_id, fm.season, fm.season_label, fm.round_number, fm.round_name, fm.stag`
- calls=`3` total=`13.268` ms mean=`4.423` ms buffers=`5004` query=`with scoped as ( select pms.match_id, pms.match_date, coalesce(pms.minutes_played, $6) as minutes_played, coalesce(pms.goals, $7) as goals, coalesce(pms.assists, $8) as assists, co`
- calls=`3` total=`12.381` ms mean=`4.127` ms buffers=`4488` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $9) else coalesce(fm.away_goals, $10) end as goals_for, case w`

## Explain Baselines

- `match_center_player_stats` planning=`0.412` ms execution=`0.261` ms
- `player_contexts` planning=`0.262` ms execution=`0.646` ms
