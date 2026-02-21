with coaches as (
    select * from {{ ref('stg_coaches') }}
)
select
    md5(concat(provider, ':coach:', coach_id::text)) as coach_sk,
    provider,
    coach_id,
    coalesce(coach_name, concat('Unknown Coach #', coach_id::text)) as coach_name,
    max(updated_at) as updated_at
from coaches
where coach_id is not null
group by provider, coach_id, coach_name
