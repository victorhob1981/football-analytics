with source_coaches as (
    select * from {{ source('postgres_raw', 'coaches') }}
)
select
    provider,
    coach_id,
    nullif(trim(coach_name), '') as coach_name,
    nullif(trim(image_path), '') as image_path,
    case
        when nullif(trim(image_path), '') is null then false
        when lower(trim(image_path)) like '%placeholder%' then false
        else true
    end as has_real_photo,
    case
        when nullif(trim(image_path), '') is null then false
        when lower(trim(image_path)) like '%placeholder%' then true
        else false
    end as is_placeholder_image,
    payload,
    ingested_run,
    updated_at
from source_coaches
where coach_id is not null
