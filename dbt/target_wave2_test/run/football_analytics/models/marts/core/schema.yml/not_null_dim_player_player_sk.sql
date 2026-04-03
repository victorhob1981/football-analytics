select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select player_sk
from "football_dw"."mart"."dim_player"
where player_sk is null



      
    ) dbt_internal_test