with source_coaches as (
    select * from {{ source('postgres_raw', 'team_coaches') }}
),
coach_identity as (
    select * from {{ ref('stg_coaches') }}
)
select
    tc.provider,
    tc.coach_tenure_id,
    tc.team_id,
    nullif(trim(coalesce(tc.payload -> 'team' ->> 'name', tc.payload ->> 'team_name')), '') as team_name,
    tc.coach_id,
    ci.coach_name,
    ci.image_path,
    coalesce(ci.has_real_photo, false) as has_real_photo,
    coalesce(ci.is_placeholder_image, false) as is_placeholder_image,
    tc.position_id,
    tc.active,
    tc.temporary,
    tc.start_date,
    tc.end_date,
    tc.payload,
    tc.ingested_run,
    tc.updated_at
from source_coaches tc
left join coach_identity ci
  on ci.provider = tc.provider
 and ci.coach_id = tc.coach_id
