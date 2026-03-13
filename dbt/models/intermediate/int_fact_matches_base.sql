with matches as (
    select * from {{ ref('int_matches_enriched') }}
),
statistics as (
    select * from {{ ref('stg_match_statistics') }}
)
select
    m.match_id,
    md5(concat(m.provider, ':competition:', coalesce(m.competition_key, m.league_id::text))) as competition_sk,
    md5(concat(m.provider, ':season:', coalesce(m.competition_key, m.league_id::text), ':', coalesce(m.season_label, m.season::text))) as season_sk,
    md5(concat('date:', m.date_day::text)) as date_sk,
    md5(concat('team:', m.home_team_id::text)) as home_team_sk,
    md5(concat('team:', m.away_team_id::text)) as away_team_sk,
    case
        when m.venue_id is not null then md5(concat('venue:', m.venue_id::text))
        else null
    end as venue_sk,
    m.provider,
    m.provider_league_id,
    m.competition_key,
    m.competition_type,
    m.league_id,
    m.season,
    m.season_label,
    m.provider_season_id,
    m.date_day,
    m.round,
    m.round_name,
    m.stage_id,
    case
        when m.stage_id is not null then md5(concat(m.provider, ':stage:', m.stage_id::text))
        else null
    end as stage_sk,
    m.stage_name,
    m.round_id,
    case
        when m.round_id is not null then md5(concat(m.provider, ':round:', m.round_id::text))
        else null
    end as round_sk,
    m.group_name,
    m.leg as leg_number,
    case
        when coalesce(m.round_name, m.round) is not null then coalesce((regexp_match(coalesce(m.round_name, m.round), '([0-9]+)'))[1]::int, 0)
        else 0
    end as round_number,
    case
        when m.competition_type in ('cup', 'continental_cup') then true
        else false
    end as is_knockout,
    m.home_team_id,
    m.away_team_id,
    m.venue_id,
    m.home_goals,
    m.away_goals,
    m.total_goals,
    m.result,
    s_home.total_shots as home_shots,
    s_home.shots_on_goal as home_shots_on_target,
    s_home.ball_possession as home_possession,
    s_home.corner_kicks as home_corners,
    s_home.fouls as home_fouls,
    s_away.total_shots as away_shots,
    s_away.shots_on_goal as away_shots_on_target,
    s_away.ball_possession as away_possession,
    s_away.corner_kicks as away_corners,
    s_away.fouls as away_fouls
from matches m
left join statistics s_home
  on m.match_id = s_home.fixture_id
 and m.home_team_id = s_home.team_id
left join statistics s_away
  on m.match_id = s_away.fixture_id
 and m.away_team_id = s_away.team_id
