with context as (
    select * from {{ ref('int_fixture_player_context') }}
),
ordered as (
    select
        provider,
        player_id,
        fixture_id,
        match_date,
        goals,
        assists,
        xg,
        rating,
        minutes_played,
        sum(coalesce(goals, 0)) over (
            partition by provider, player_id
            order by match_date nulls last, fixture_id
            rows between 4 preceding and current row
        ) as goals_last_5,
        sum(coalesce(assists, 0)) over (
            partition by provider, player_id
            order by match_date nulls last, fixture_id
            rows between 4 preceding and current row
        ) as assists_last_5,
        sum(coalesce(xg, 0)) over (
            partition by provider, player_id
            order by match_date nulls last, fixture_id
            rows between 4 preceding and current row
        ) as xg_last_5,
        avg(rating) over (
            partition by provider, player_id
            order by match_date nulls last, fixture_id
            rows between 4 preceding and current row
        ) as rating_avg_last_5
    from context
)
select * from ordered
