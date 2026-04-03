select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    player_sk as unique_field,
    count(*) as n_records

from "football_dw"."mart"."dim_player"
where player_sk is not null
group by player_sk
having count(*) > 1



      
    ) dbt_internal_test