
    
    

select
    player_id as unique_field,
    count(*) as n_records

from "football_dw"."mart"."dim_player"
where player_id is not null
group by player_id
having count(*) > 1


