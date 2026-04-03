with grouped as (
    select
        count(distinct group_id) as groups,
        count(*) as rows
    from {{ ref('fact_group_standings') }}
    where competition_key = 'champions_league'
      and season_label = '2023_24'
)
select
    groups,
    rows
from grouped
where groups <> 8
   or rows <> 32
