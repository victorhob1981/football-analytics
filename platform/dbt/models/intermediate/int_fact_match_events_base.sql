with events as (
    select * from {{ ref('stg_match_events') }}
),
matches as (
    select match_id from {{ ref('int_matches_enriched') }}
)
select
    e.event_id,
    e.fixture_id as match_id,
    case
        when e.team_id is not null then md5(concat('team:', e.team_id::text))
        else null
    end as team_sk,
    case
        when e.player_id is not null then md5(concat('player:', e.player_id::text))
        else null
    end as player_sk,
    case
        when e.assist_player_id is not null then md5(concat('player:', e.assist_player_id::text))
        else null
    end as assist_player_sk,
    e.team_id,
    e.player_id,
    e.assist_player_id,
    e.time_elapsed,
    e.time_extra,
    e.is_time_elapsed_anomalous,
    e.event_type,
    e.event_detail,
    case when e.event_type = 'Goal' then true else false end as is_goal,
    coalesce(e.updated_at, now()) as updated_at
from events e
inner join matches m
  on m.match_id = e.fixture_id
where e.event_id is not null
  and e.fixture_id is not null
