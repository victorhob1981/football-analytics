with source_stats as (
    select * from {{ source('postgres_raw', 'player_season_statistics') }}
),
named_rows as (
    select
        s.provider,
        s.player_id,
        nullif(trim(coalesce(s.payload -> 'player' ->> 'name', s.payload ->> 'player_name')), '') as player_name,
        s.season_id,
        s.league_id,
        s.team_id,
        nullif(trim(coalesce(s.payload -> 'team' ->> 'name', s.payload ->> 'team_name')), '') as team_name,
        nullif(trim(s.season_name), '') as season_name,
        nullif(trim(s.position_name), '') as position_name,
        s.statistics,
        s.payload,
        s.ingested_run,
        s.updated_at
    from source_stats s
),
expanded_metrics as (
    select
        n.provider,
        n.player_id,
        n.season_id,
        n.team_id,
        lower(coalesce(metric.item ->> 'type', metric.item ->> 'developer_name', metric.item ->> 'raw_type_name', '')) as metric_name,
        nullif(
            regexp_replace(
                coalesce(
                    metric.item ->> 'value',
                    metric.item -> 'raw_value' ->> 'value',
                    metric.item ->> 'raw_value',
                    ''
                ),
                '[^0-9\\.-]',
                '',
                'g'
            ),
            ''
        )::numeric as metric_value
    from named_rows n
    left join lateral jsonb_array_elements(
        case jsonb_typeof(n.statistics)
            when 'array' then n.statistics
            when 'object' then jsonb_build_array(n.statistics)
            else '[]'::jsonb
        end
    ) as metric(item) on true
),
pivoted as (
    select
        provider,
        player_id,
        season_id,
        team_id,
        max(case when metric_name in ('goals', 'goal') then metric_value end) as goals,
        max(case when metric_name in ('assists', 'assist') then metric_value end) as assists,
        max(case when metric_name in ('minutes', 'minutes_played', 'time_played') then metric_value end) as minutes_played,
        max(case when metric_name in ('appearances', 'matches', 'games') then metric_value end) as appearances,
        max(case when metric_name in ('rating', 'player_rating') then metric_value end) as rating,
        max(case when metric_name in ('xg', 'expected_goals') then metric_value end) as xg
    from expanded_metrics
    group by provider, player_id, season_id, team_id
)
select
    n.provider,
    n.player_id,
    n.player_name,
    n.season_id,
    n.league_id,
    n.team_id,
    n.team_name,
    n.season_name,
    n.position_name,
    p.goals,
    p.assists,
    p.minutes_played,
    p.appearances,
    p.rating,
    p.xg,
    n.statistics,
    n.payload,
    n.ingested_run,
    n.updated_at
from named_rows n
left join pivoted p
  on p.provider = n.provider
 and p.player_id = n.player_id
 and p.season_id = n.season_id
 and p.team_id = n.team_id
