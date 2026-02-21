with rounds as (
    select * from {{ ref('stg_competition_rounds') }}
),
ordered as (
    select
        provider,
        league_id,
        season_id,
        stage_id,
        round_id,
        round_name,
        starting_at,
        ending_at,
        finished,
        is_current,
        row_number() over (
            partition by provider, season_id
            order by
                starting_at nulls last,
                ending_at nulls last,
                round_id
        ) as round_key
    from rounds
    where round_id is not null
      and season_id is not null
)
select * from ordered
