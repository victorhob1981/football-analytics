select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    fixture_player_stat_id as unique_field,
    count(*) as n_records

from "football_dw"."mart"."fact_fixture_player_stats"
where fixture_player_stat_id is not null
group by fixture_player_stat_id
having count(*) > 1



      
    ) dbt_internal_test