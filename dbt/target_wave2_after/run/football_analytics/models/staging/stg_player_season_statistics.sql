
  
    

  create  table "football_dw"."mart"."stg_player_season_statistics__dbt_tmp"
  
  
    as
  
  (
    

with source_stats as (
    select * from "football_dw"."raw"."player_season_statistics"
),
named_rows as (
    select
        s.provider,
        s.player_id,
        nullif(trim(coalesce(s.payload -> 'player' ->> 'name', s.payload ->> 'player_name')), '') as player_name,
        nullif(trim(coalesce(s.player_nationality, s.payload -> 'player' ->> 'nationality')), '') as player_nationality,
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
        case
            when jsonb_typeof(metric.item -> 'value') = 'number'
             and (metric.item ->> 'value') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then (metric.item ->> 'value')::numeric
            when jsonb_typeof(metric.item -> 'value') = 'string'
             and nullif(trim(metric.item ->> 'value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then nullif(trim(metric.item ->> 'value'), '')::numeric
            when jsonb_typeof(metric.item -> 'value') = 'object'
             and nullif(
                 trim(
                     case
                         when lower(coalesce(metric.item ->> 'type', metric.item ->> 'developer_name', metric.item ->> 'raw_type_name', ''))
                              in ('rating', 'player_rating', 'average_points_per_game')
                             then coalesce(metric.item -> 'value' ->> 'average', metric.item -> 'value' ->> 'total')
                         else coalesce(
                             metric.item -> 'value' ->> 'total',
                             metric.item -> 'value' ->> 'goals',
                             metric.item -> 'value' ->> 'average',
                             metric.item -> 'value' ->> 'value'
                         )
                     end
                 ),
                 ''
             ) ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then nullif(
                    trim(
                        case
                            when lower(coalesce(metric.item ->> 'type', metric.item ->> 'developer_name', metric.item ->> 'raw_type_name', ''))
                                 in ('rating', 'player_rating', 'average_points_per_game')
                                then coalesce(metric.item -> 'value' ->> 'average', metric.item -> 'value' ->> 'total')
                            else coalesce(
                                metric.item -> 'value' ->> 'total',
                                metric.item -> 'value' ->> 'goals',
                                metric.item -> 'value' ->> 'average',
                                metric.item -> 'value' ->> 'value'
                            )
                        end
                    ),
                    ''
                )::numeric
            when nullif(trim(metric.item -> 'raw_value' ->> 'value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then nullif(trim(metric.item -> 'raw_value' ->> 'value'), '')::numeric
            when nullif(trim(metric.item ->> 'raw_value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                then nullif(trim(metric.item ->> 'raw_value'), '')::numeric
            else null
        end as metric_value
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
    n.player_nationality,
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
  );
  