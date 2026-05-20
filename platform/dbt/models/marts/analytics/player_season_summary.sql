with player_matches as (
    select * from {{ ref('player_match_summary') }}
),
match_aggregate as (
    select
        competition_sk,
        season,
        player_sk,
        player_id,
        player_name,
        team_sk,
        team_id,
        team_name,
        count(distinct match_id) as matches,
        sum(coalesce(minutes_played, 0)) as minutes_played,
        sum(coalesce(goals, 0)) as goals,
        sum(coalesce(assists, 0)) as assists,
        sum(coalesce(shots_total, 0)) as shots_total,
        sum(coalesce(shots_on_goal, 0)) as shots_on_goal,
        sum(coalesce(passes_total, 0)) as passes_total,
        sum(coalesce(key_passes, 0)) as key_passes,
        sum(coalesce(tackles, 0)) as tackles,
        sum(coalesce(interceptions, 0)) as interceptions,
        sum(coalesce(duels, 0)) as duels,
        sum(coalesce(fouls_committed, 0)) as fouls_committed,
        sum(coalesce(yellow_cards, 0)) as yellow_cards,
        sum(coalesce(red_cards, 0)) as red_cards,
        sum(coalesce(goalkeeper_saves, 0)) as goalkeeper_saves,
        sum(coalesce(clean_sheets, 0)) as clean_sheets,
        sum(coalesce(xg, 0)) as xg,
        avg(rating) as avg_rating,
        max(updated_at) as updated_at
    from player_matches
    group by
        competition_sk,
        season,
        player_sk,
        player_id,
        player_name,
        team_sk,
        team_id,
        team_name
),
season_stats as (
    select * from {{ ref('stg_player_season_statistics') }}
),
season_stat_metric_signal as (
    select
        s.provider,
        s.player_id,
        s.season_id,
        s.team_id,
        bool_or(
            case
                when jsonb_typeof(metric.item -> 'value') = 'number' then true
                when jsonb_typeof(metric.item -> 'value') = 'string'
                 and nullif(trim(metric.item ->> 'value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                when jsonb_typeof(metric.item -> 'value') = 'object'
                 and nullif(
                     trim(
                         coalesce(
                             metric.item -> 'value' ->> 'total',
                             metric.item -> 'value' ->> 'goals',
                             metric.item -> 'value' ->> 'average',
                             metric.item -> 'value' ->> 'value'
                         )
                     ),
                     ''
                 ) ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                when nullif(trim(metric.item -> 'raw_value' ->> 'value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                when nullif(trim(metric.item ->> 'raw_value'), '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then true
                else false
            end
        ) as has_any_numeric_metric
    from season_stats s
    left join lateral jsonb_array_elements(
        case jsonb_typeof(s.statistics)
            when 'array' then s.statistics
            when 'object' then jsonb_build_array(s.statistics)
            else '[]'::jsonb
        end
    ) as metric(item) on true
    group by
        s.provider,
        s.player_id,
        s.season_id,
        s.team_id
),
season_stats_scope as (
    select
        md5(concat('competition:', s.league_id::text)) as competition_sk,
        case
            when coalesce(s.season_name, '') ~ '^[0-9]{4}/[0-9]{4}$'
                then split_part(s.season_name, '/', 1)::int
            else null
        end as season,
        md5(concat('player:', s.player_id::text)) as player_sk,
        s.player_id,
        s.player_name,
        md5(concat('team:', s.team_id::text)) as team_sk,
        s.team_id,
        s.team_name,
        s.appearances::bigint as matches,
        s.minutes_played,
        s.goals,
        s.assists,
        cast(null as numeric) as shots_total,
        cast(null as numeric) as shots_on_goal,
        cast(null as numeric) as passes_total,
        cast(null as numeric) as key_passes,
        cast(null as numeric) as tackles,
        cast(null as numeric) as interceptions,
        cast(null as numeric) as duels,
        cast(null as numeric) as fouls_committed,
        cast(null as numeric) as yellow_cards,
        cast(null as numeric) as red_cards,
        cast(null as numeric) as goalkeeper_saves,
        cast(null as numeric) as clean_sheets,
        s.xg,
        s.rating as avg_rating,
        s.updated_at
    from season_stats s
    inner join season_stat_metric_signal signal
      on signal.provider = s.provider
     and signal.player_id = s.player_id
     and signal.season_id = s.season_id
     and signal.team_id = s.team_id
     and signal.has_any_numeric_metric
    where s.league_id is not null
      and s.player_id is not null
      and s.team_id is not null
      and s.team_id <> 0
),
reconciled as (
    -- Keep the match-derived summary as the primary source and use raw season stats
    -- only as a fallback for player/team seasons that carry real numeric signal.
    select
        coalesce(m.competition_sk, s.competition_sk) as competition_sk,
        coalesce(m.season, s.season) as season,
        coalesce(m.player_sk, s.player_sk) as player_sk,
        coalesce(m.player_id, s.player_id) as player_id,
        coalesce(m.player_name, s.player_name) as player_name,
        coalesce(m.team_sk, s.team_sk) as team_sk,
        coalesce(m.team_id, s.team_id) as team_id,
        coalesce(m.team_name, s.team_name) as team_name,
        coalesce(m.matches, s.matches, 0) as matches,
        coalesce(m.minutes_played, s.minutes_played, 0) as minutes_played,
        coalesce(m.goals, s.goals, 0) as goals,
        coalesce(m.assists, s.assists, 0) as assists,
        coalesce(m.shots_total, s.shots_total, 0) as shots_total,
        coalesce(m.shots_on_goal, s.shots_on_goal, 0) as shots_on_goal,
        coalesce(m.passes_total, s.passes_total, 0) as passes_total,
        coalesce(m.key_passes, s.key_passes, 0) as key_passes,
        coalesce(m.tackles, s.tackles, 0) as tackles,
        coalesce(m.interceptions, s.interceptions, 0) as interceptions,
        coalesce(m.duels, s.duels, 0) as duels,
        coalesce(m.fouls_committed, s.fouls_committed, 0) as fouls_committed,
        coalesce(m.yellow_cards, s.yellow_cards, 0) as yellow_cards,
        coalesce(m.red_cards, s.red_cards, 0) as red_cards,
        coalesce(m.goalkeeper_saves, s.goalkeeper_saves, 0) as goalkeeper_saves,
        coalesce(m.clean_sheets, s.clean_sheets, 0) as clean_sheets,
        coalesce(m.xg, s.xg, 0) as xg,
        coalesce(m.avg_rating, s.avg_rating) as avg_rating,
        greatest(
            coalesce(m.updated_at, timestamp with time zone '1900-01-01'),
            coalesce(s.updated_at, timestamp with time zone '1900-01-01')
        ) as updated_at
    from match_aggregate m
    full outer join season_stats_scope s
      on s.competition_sk = m.competition_sk
     and s.season = m.season
     and s.player_id = m.player_id
     and s.team_id = m.team_id
)
select
    competition_sk,
    season,
    player_sk,
    player_id,
    player_name,
    team_sk,
    team_id,
    team_name,
    matches,
    minutes_played,
    goals,
    assists,
    shots_total,
    shots_on_goal,
    passes_total,
    key_passes,
    tackles,
    interceptions,
    duels,
    fouls_committed,
    yellow_cards,
    red_cards,
    goalkeeper_saves,
    clean_sheets,
    xg,
    avg_rating,
    updated_at
from reconciled
where competition_sk is not null
  and season is not null
  and player_id is not null
  and team_id is not null
