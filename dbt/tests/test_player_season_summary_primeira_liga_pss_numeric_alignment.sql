with raw_scope as (
    select
        split_part(pss.season_label, '_', 1)::int as season,
        pss.player_id,
        pss.team_id,
        bool_or(
            case
                when jsonb_typeof(metric.item -> 'value') = 'number' then true
                when jsonb_typeof(metric.item -> 'value') = 'string'
                 and nullif(trim(metric.item ->> 'value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                when jsonb_typeof(metric.item -> 'value') = 'object'
                 and nullif(
                     trim(
                         coalesce(
                             metric.item -> 'value' ->> 'total',
                             metric.item -> 'value' ->> 'goals',
                             metric.item -> 'value' ->> 'average',
                             metric.item -> 'value' ->> 'value'
                         )
                     ),
                     ''
                 ) ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                when nullif(trim(metric.item -> 'raw_value' ->> 'value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                when nullif(trim(metric.item ->> 'raw_value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                else false
            end
        ) as has_any_numeric_metric
    from {{ source('postgres_raw', 'player_season_statistics') }} pss
    left join lateral jsonb_array_elements(
        case jsonb_typeof(pss.statistics)
            when 'array' then pss.statistics
            when 'object' then jsonb_build_array(pss.statistics)
            else '[]'::jsonb
        end
    ) as metric(item) on true
    where pss.competition_key = 'primeira_liga'
      and pss.season_label in ('2023_24', '2024_25')
    group by
        split_part(pss.season_label, '_', 1)::int,
        pss.player_id,
        pss.team_id
),
expected_scope as (
    select
        season,
        player_id,
        team_id
    from raw_scope
    where has_any_numeric_metric
),
missing_from_mart as (
    select
        e.season,
        e.player_id,
        e.team_id
    from expected_scope e
    left join {{ ref('player_season_summary') }} pss
      on pss.competition_sk = md5('competition:462')
     and pss.season = e.season
     and pss.player_id = e.player_id
     and pss.team_id = e.team_id
    where pss.player_id is null
)
select *
from missing_from_mart
