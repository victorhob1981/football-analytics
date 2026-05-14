with tenures as (
    select * from {{ ref('stg_team_coaches') }}
)
select
    provider,
    coach_id,
    max(coach_name) as coach_name,
    max(updated_at) as updated_at
from tenures
where coach_id is not null
group by provider, coach_id
