with source_lineups as (
    select * from {{ source('postgres_raw', 'fixture_lineups') }}
),
enriched as (
    select
        l.provider,
        l.fixture_id,
        l.team_id,
        l.player_id,
        nullif(
            trim(
                coalesce(
                    l.payload -> 'player' ->> 'name',
                    l.payload ->> 'player_name',
                    l.payload -> 'player' ->> 'display_name'
                )
            ),
            ''
        ) as player_name,
        l.lineup_id,
        l.position_id,
        nullif(trim(l.position_name), '') as position_name,
        l.lineup_type_id,
        nullif(trim(l.formation_field), '') as formation_field,
        l.formation_position,
        l.jersey_number,
        l.details,
        l.payload,
        l.ingested_run,
        l.updated_at,
        stats.minutes_played
    from source_lineups l
    left join lateral (
        select
            max(
                nullif(
                    regexp_replace(
                        coalesce(
                            detail ->> 'value',
                            detail -> 'raw_value' ->> 'value',
                            detail ->> 'raw_value',
                            ''
                        ),
                        '[^0-9\\.-]',
                        '',
                        'g'
                    ),
                    ''
                )::numeric
            )::int as minutes_played
        from jsonb_array_elements(
            case jsonb_typeof(l.details)
                when 'array' then l.details
                when 'object' then jsonb_build_array(l.details)
                else '[]'::jsonb
            end
        ) as detail
        where lower(coalesce(detail ->> 'type', detail ->> 'developer_name', detail ->> 'raw_type_name', '')) in (
            'minutes_played',
            'minutes',
            'time_played'
        )
    ) stats on true
)
select
    provider,
    fixture_id,
    team_id,
    player_id,
    player_name,
    lineup_id,
    position_id,
    position_name,
    lineup_type_id,
    case
        when lineup_type_id in (1, 11) then true
        when lineup_type_id is null then null
        else false
    end as is_starter,
    formation_field,
    formation_position,
    jersey_number,
    minutes_played,
    details,
    payload,
    ingested_run,
    updated_at
from enriched
