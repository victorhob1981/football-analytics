with champions as (
    select count(distinct season_format_code) as distinct_format_codes
    from {{ ref('competition_season_config') }}
    where competition_key = 'champions_league'
)
select 1
from champions
where distinct_format_codes < 2
