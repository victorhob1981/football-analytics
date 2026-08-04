{{ config(severity='error') }}

select mc.match_id
from {{ source('serving_v2', 'match_catalog') }} mc
left join {{ source('mart_v2', 'fact_match') }} f
  on f.match_id = mc.match_id
where f.match_id is null
   or f.publication_state <> 'published'

