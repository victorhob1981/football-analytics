{{ config(severity='error') }}

select f.match_id
from {{ source('mart_v2', 'fact_match') }} f
left join {{ source('mart_v2', 'dim_competition') }} c
  on c.competition_key = f.competition_key
left join {{ source('mart_v2', 'dim_edition') }} e
  on e.edition_key = f.edition_key
left join {{ source('mart_v2', 'dim_team') }} ht
  on ht.team_id = f.home_team_id
left join {{ source('mart_v2', 'dim_team') }} at
  on at.team_id = f.away_team_id
where f.publication_state = 'published'
  and (c.competition_key is null
    or e.edition_key is null
    or ht.team_id is null
    or at.team_id is null)

