with source_stats as (
    select * from {{ source('postgres_raw', 'fixture_player_statistics') }}
),
named_rows as (
    select
        s.provider,
        s.fixture_id,
        s.team_id,
        s.player_id,
        nullif(trim(coalesce(s.payload -> 'player' ->> 'name', s.payload ->> 'player_name')), '') as player_name,
        nullif(trim(coalesce(s.payload -> 'team' ->> 'name', s.payload ->> 'team_name')), '') as team_name,
        s.statistics,
        s.payload,
        s.ingested_run,
        s.updated_at
    from source_stats s
),
expanded_metrics as (
    select
        n.provider,
        n.fixture_id,
        n.team_id,
        n.player_id,
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
        fixture_id,
        team_id,
        player_id,
        max(case when metric_name in ('goals', 'goal') then metric_value end) as goals,
        max(case when metric_name in ('assists', 'assist') then metric_value end) as assists,
        max(case when metric_name in ('shots_total', 'total_shots', 'shots') then metric_value end) as shots_total,
        max(case when metric_name in ('shots_on_goal', 'shots_on_target') then metric_value end) as shots_on_goal,
        max(case when metric_name in ('passes', 'total_passes', 'passes_total') then metric_value end) as passes_total,
        max(case when metric_name in ('key_passes', 'passes_key', 'passes_key_passes') then metric_value end) as key_passes,
        max(case when metric_name in ('tackles', 'tackle') then metric_value end) as tackles,
        max(case when metric_name in ('interceptions', 'interception') then metric_value end) as interceptions,
        max(case when metric_name in ('duels', 'duels_won') then metric_value end) as duels,
        max(case when metric_name in ('fouls', 'fouls_committed') then metric_value end) as fouls_committed,
        max(case when metric_name in ('yellow_cards', 'yellowcard') then metric_value end) as yellow_cards,
        max(case when metric_name in ('red_cards', 'redcard') then metric_value end) as red_cards,
        max(case when metric_name in ('saves', 'goalkeeper_saves') then metric_value end) as goalkeeper_saves,
        max(case when metric_name in ('clean_sheets', 'clean_sheet') then metric_value end) as clean_sheets,
        max(case when metric_name in ('xg', 'expected_goals') then metric_value end) as xg,
        max(case when metric_name in ('rating', 'player_rating') then metric_value end) as rating,
        max(case when metric_name in ('minutes', 'minutes_played', 'time_played') then metric_value end) as minutes_played
    from expanded_metrics
    group by provider, fixture_id, team_id, player_id
)
select
    n.provider,
    n.fixture_id,
    n.team_id,
    n.team_name,
    n.player_id,
    n.player_name,
    p.goals,
    p.assists,
    p.shots_total,
    p.shots_on_goal,
    p.passes_total,
    p.key_passes,
    p.tackles,
    p.interceptions,
    p.duels,
    p.fouls_committed,
    p.yellow_cards,
    p.red_cards,
    p.goalkeeper_saves,
    p.clean_sheets,
    p.xg,
    p.rating,
    p.minutes_played,
    n.statistics,
    n.payload,
    n.ingested_run,
    n.updated_at
from named_rows n
left join pivoted p
  on p.provider = n.provider
 and p.fixture_id = n.fixture_id
 and p.team_id = n.team_id
 and p.player_id = n.player_id
