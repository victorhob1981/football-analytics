# DB Tuning Wave 0 Baseline

Generated at: `2026-04-01 00:30:38.456097+00`

## Instrumentation

- shared_preload_libraries: `pg_stat_statements`
- extension installed: `True`

## Endpoint Baseline

- `matches_list` `/api/v1/matches` mean=31.166 ms median=32.918 ms max=35.448 ms
- `match_center` `/api/v1/matches/19620974` mean=428.134 ms median=422.638 ms max=461.824 ms
- `player_profile` `/api/v1/players/929` mean=347.434 ms median=344.643 ms max=383.037 ms
- `team_profile` `/api/v1/teams/696` mean=675.524 ms median=701.085 ms max=707.198 ms
- `rankings_player_goals` `/api/v1/rankings/player-goals` mean=117.266 ms median=112.663 ms max=127.02 ms

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

- database `football_dw` size: `5907 MB`
- `raw.fixture_lineups` size=`1499 MB` rows_est=676270
- `raw.fixture_player_statistics` size=`1398 MB` rows_est=676047
- `mart.fact_fixture_player_stats` size=`1266 MB` rows_est=617497
- `mart.fact_fixture_lineups` size=`901 MB` rows_est=673725
- `mart.player_match_summary` size=`194 MB` rows_est=675734
- `raw.player_season_statistics` size=`191 MB` rows_est=56384
- `mart.fact_match_events` size=`122 MB` rows_est=278616
- `raw.match_events_default` size=`115 MB` rows_est=220132
- `raw.head_to_head_fixtures` size=`45 MB` rows_est=15877
- `raw.match_events_2024` size=`34 MB` rows_est=58484

## Schema Drift

- status: `confirmed_drift`
- live columns not referenced in committed `raw.match_events` migrations: `provider`, `provider_league_id`, `competition_key`, `season_label`, `provider_season_id`, `source_run_id`

## Top SQL

- calls=`3` total=`1118.569` ms mean=`372.856` ms buffers=`811473` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $10) else coalesce(fm.away_goals, $11) end as goals_for, case `
- calls=`3` total=`424.602` ms mean=`141.534` ms buffers=`485100` query=`select fps.player_id::text as player_id, fps.player_name, fps.team_id::text as team_id, coalesce(fps.team_name, team.team_name) as team_name, fps.position_name, fps.is_starter, fps`
- calls=`3` total=`371.275` ms mean=`123.758` ms buffers=`345459` query=`select ffl.player_id::text as player_id, ffl.player_name, ffl.team_id::text as team_id, team.team_name as team_name, ffl.position_name as position, ffl.formation_field, ffl.formati`
- calls=`3` total=`369.051` ms mean=`123.017` ms buffers=`350355` query=`with scoped_matches as ( select fm.match_id, fm.date_day, case when fm.home_team_id = $1 then coalesce(fm.home_goals, $11) else coalesce(fm.away_goals, $12) end as goals_for, case `
- calls=`3` total=`217.438` ms mean=`72.479` ms buffers=`116700` query=`with scoped as ( select pms.player_id, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $7) as minutes_played, coalesce(pms.goals, $8) as goal`
- calls=`3` total=`130.726` ms mean=`43.575` ms buffers=`83907` query=`with scoped_matches as ( select distinct fm.match_id from mart.fact_matches fm where $5=$6 and fm.league_id = any($1) and fm.season = $2 ), scoped_stats as ( select distinct pms.ma`
- calls=`3` total=`125.472` ms mean=`41.824` ms buffers=`79659` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $11) as minutes_played, coalesce(pms.`
- calls=`3` total=`121.256` ms mean=`40.419` ms buffers=`75489` query=`with player_history as ( select dc.league_id, dc.league_name, pms.season, pms.team_id, coalesce(max(pms.team_name), dt.team_name) as team_name, count(distinct pms.match_id)::int as`
- calls=`3` total=`120.537` ms mean=`40.179` ms buffers=`79740` query=`with scoped as ( select pms.match_id, pms.match_date, coalesce(pms.minutes_played, $6) as minutes_played, coalesce(pms.goals, $7) as goals, coalesce(pms.assists, $8) as assists, co`
- calls=`3` total=`111.923` ms mean=`37.308` ms buffers=`79374` query=`with scoped as ( select pms.player_id, pms.player_name, pms.team_id, pms.team_name, pms.position_name, pms.match_id, pms.match_date, coalesce(pms.minutes_played, $6) as minutes_pla`

## Explain Baselines

- `match_center_player_stats` planning=`0.403` ms execution=`234.439` ms
- `player_contexts` planning=`0.302` ms execution=`48.009` ms
