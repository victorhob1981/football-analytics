with matches as (
    select * from {{ ref('int_matches_enriched') }}
),
group_memberships as (
    select
        provider,
        competition_key,
        season_label,
        stage_id,
        team_id,
        group_id
    from {{ ref('int_group_memberships') }}
),
ties as (
    select
        match_id,
        tie_id,
        inferred_leg_number
    from {{ ref('int_tie_matches') }}
),
stages as (
    select
        provider,
        stage_id,
        stage_sk,
        stage_name,
        stage_format
    from {{ ref('dim_stage') }}
),
rounds as (
    select
        provider,
        round_id,
        round_sk,
        round_name
    from {{ ref('dim_round') }}
),
statistics as (
    select * from {{ ref('stg_match_statistics') }}
)
select
    m.match_id,
    md5(concat('competition:', m.league_id::text)) as competition_sk,
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
    m.match_ingested_run,
    m.match_ingested_at,
    greatest(
        coalesce(
            case
                when m.match_ingested_run ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}Z$'
                    then m.match_ingested_run::timestamptz
                else null
            end,
            m.match_ingested_at,
            timestamptz '1900-01-01 00:00:00+00'
        ),
        coalesce(s_home.updated_at, timestamptz '1900-01-01 00:00:00+00'),
        coalesce(s_away.updated_at, timestamptz '1900-01-01 00:00:00+00')
    ) as source_watermark,
    m.round,
    coalesce(m.round_name, r.round_name) as round_name,
    m.stage_id,
    s.stage_sk,
    coalesce(m.stage_name, s.stage_name) as stage_name,
    m.round_id,
    r.round_sk,
    case
        when home_group.group_id is not null
         and away_group.group_id is not null
         and home_group.group_id = away_group.group_id
            then home_group.group_id
        else null
    end as group_id,
    t.tie_id,
    coalesce(m.leg_number, t.inferred_leg_number) as leg_number,
    coalesce((regexp_match(m.round, '([0-9]+)'))[1]::int, 0) as round_number,
    case
        when s.stage_format in ('knockout', 'qualification_knockout') then true
        when s.stage_format in ('group_table', 'league_table') then false
        else null
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
left join group_memberships home_group
  on home_group.provider = m.provider
 and home_group.competition_key = m.competition_key
 and home_group.season_label = m.season_label
 and home_group.stage_id = m.stage_id
 and home_group.team_id = m.home_team_id
left join group_memberships away_group
  on away_group.provider = m.provider
 and away_group.competition_key = m.competition_key
 and away_group.season_label = m.season_label
 and away_group.stage_id = m.stage_id
 and away_group.team_id = m.away_team_id
left join ties t
  on t.match_id = m.match_id
left join stages s
  on s.provider = m.provider
 and s.stage_id = m.stage_id
left join rounds r
  on r.provider = m.provider
 and r.round_id = m.round_id
left join statistics s_home
  on m.match_id = s_home.fixture_id
 and m.home_team_id = s_home.team_id
left join statistics s_away
  on m.match_id = s_away.fixture_id
 and m.away_team_id = s_away.team_id
