with rounds as (
    select * from {{ ref('int_competition_round_calendar') }}
)
select
    md5(concat(provider, ':round:', round_id::text)) as round_sk,
    provider,
    round_id,
    stage_id,
    md5(concat(provider, ':stage:', stage_id::text)) as stage_sk,
    provider_league_id,
    competition_key,
    season_label,
    league_id,
    season_id,
    provider_season_id,
    round_key,
    round_name,
    starting_at,
    ending_at,
    finished,
    is_current,
    now() as updated_at
from rounds
