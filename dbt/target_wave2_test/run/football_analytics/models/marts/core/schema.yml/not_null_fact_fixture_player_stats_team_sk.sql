select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select team_sk
from "football_dw"."mart"."fact_fixture_player_stats"
where team_sk is null



      
    ) dbt_internal_test