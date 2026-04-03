select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fixture_player_stat_id
from "football_dw"."mart"."fact_fixture_player_stats"
where fixture_player_stat_id is null



      
    ) dbt_internal_test