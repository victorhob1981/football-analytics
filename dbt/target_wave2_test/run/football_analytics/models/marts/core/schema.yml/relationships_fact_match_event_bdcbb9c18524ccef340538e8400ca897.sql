select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select assist_player_sk as from_field
    from (select * from "football_dw"."mart"."fact_match_events" where assist_player_sk is not null) dbt_subquery
    where assist_player_sk is not null
),

parent as (
    select player_sk as to_field
    from "football_dw"."mart"."dim_player"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test