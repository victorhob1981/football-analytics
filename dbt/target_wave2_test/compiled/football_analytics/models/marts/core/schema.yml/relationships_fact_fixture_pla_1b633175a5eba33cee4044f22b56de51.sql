
    
    

with child as (
    select match_id as from_field
    from "football_dw"."mart"."fact_fixture_player_stats"
    where match_id is not null
),

parent as (
    select match_id as to_field
    from "football_dw"."mart"."fact_matches"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


