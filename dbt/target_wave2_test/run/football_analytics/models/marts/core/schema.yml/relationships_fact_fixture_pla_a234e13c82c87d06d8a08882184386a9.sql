select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select team_sk as from_field
    from "football_dw"."mart"."fact_fixture_player_stats"
    where team_sk is not null
),

parent as (
    select team_sk as to_field
    from "football_dw"."mart"."dim_team"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test