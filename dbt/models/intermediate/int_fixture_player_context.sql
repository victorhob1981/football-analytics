with fps as (
    select * from {{ ref('stg_fixture_player_statistics') }}
),
lineups as (
    select * from {{ ref('stg_fixture_lineups') }}
),
events as (
    select * from {{ ref('stg_match_events') }}
),
matches as (
    select
        fixture_id,
        league_id,
        season,
        date_utc::date as match_date
    from {{ ref('stg_matches') }}
),
event_rollup as (
    select
        fixture_id,
        player_id,
        sum(case when event_type = 'Goal' then 1 else 0 end)::int as goals_from_events,
        0::int as assists_from_events
    from events
    where fixture_id is not null
      and player_id is not null
    group by fixture_id, player_id

    union all

    select
        fixture_id,
        assist_player_id as player_id,
        0::int as goals_from_events,
        sum(case when assist_player_id is not null then 1 else 0 end)::int as assists_from_events
    from events
    where fixture_id is not null
      and assist_player_id is not null
    group by fixture_id, assist_player_id
),
event_player_totals as (
    select
        fixture_id,
        player_id,
        sum(goals_from_events)::int as goals_from_events,
        sum(assists_from_events)::int as assists_from_events
    from event_rollup
    group by fixture_id, player_id
)
select
    f.provider,
    f.fixture_id,
    m.league_id,
    m.season,
    m.match_date,
    f.team_id,
    coalesce(f.team_name, l.payload -> 'team' ->> 'name') as team_name,
    f.player_id,
    coalesce(f.player_name, l.player_name) as player_name,
    coalesce(f.minutes_played, l.minutes_played) as minutes_played,
    l.position_name,
    l.is_starter,
    coalesce(f.goals, e.goals_from_events, 0) as goals,
    coalesce(f.assists, e.assists_from_events, 0) as assists,
    f.shots_total,
    f.shots_on_goal,
    f.passes_total,
    f.key_passes,
    f.tackles,
    f.interceptions,
    f.duels,
    f.fouls_committed,
    f.yellow_cards,
    f.red_cards,
    f.goalkeeper_saves,
    f.clean_sheets,
    f.xg,
    f.rating,
    f.statistics,
    f.payload,
    f.ingested_run,
    coalesce(f.updated_at, l.updated_at, now()) as updated_at
from fps f
left join lineups l
  on l.provider = f.provider
 and l.fixture_id = f.fixture_id
 and l.team_id = f.team_id
 and l.player_id = f.player_id
left join event_player_totals e
  on e.fixture_id = f.fixture_id
 and e.player_id = f.player_id
left join matches m
  on m.fixture_id = f.fixture_id
