with tie_matches as (
    select * from {{ ref('int_tie_matches') }}
),
penalty_shootout_events as (
    select
        events.fixture_id as match_id,
        events.comments,
        row_number() over (
            partition by events.fixture_id
            order by
                coalesce(events.time_elapsed, -1) desc,
                coalesce(events.time_extra, -1) desc,
                events.event_id desc
        ) as penalty_event_rank
    from {{ ref('stg_match_events') }} events
    where events.event_type in ('Penalty Shootout Goal', 'Penalty Shootout Miss')
      and events.comments ~ '^[0-9]+-[0-9]+$'
),
penalty_shootout_outcomes as (
    select
        matches.match_id,
        case
            when split_part(events.comments, '-', 1)::int > split_part(events.comments, '-', 2)::int
                then matches.home_team_id
            when split_part(events.comments, '-', 2)::int > split_part(events.comments, '-', 1)::int
                then matches.away_team_id
            else null
        end as penalty_winner_team_id
    from tie_matches matches
    inner join penalty_shootout_events events
      on events.match_id = matches.match_id
     and events.penalty_event_rank = 1
),
group_memberships as (
    select
        competition_key,
        season_label,
        stage_id,
        team_id
    from {{ ref('int_group_memberships') }}
),
competition_seasons as (
    select distinct
        competition_key,
        season_label
    from tie_matches
),
stage_catalog as (
    select
        stages.competition_key,
        stages.season_label,
        stages.sort_order as stage_order,
        stages.stage_id,
        stages.stage_sk,
        stages.stage_name
    from {{ ref('dim_stage') }} stages
    inner join competition_seasons seasons
      on seasons.competition_key = stages.competition_key
     and seasons.season_label = stages.season_label
    where stages.sort_order is not null
),
stage_progression as (
    select
        competition_key,
        season_label,
        stage_order,
        stage_id,
        stage_sk,
        stage_name,
        lead(stage_id) over (
            partition by competition_key, season_label
            order by stage_order
        ) as next_stage_id,
        lead(stage_name) over (
            partition by competition_key, season_label
            order by stage_order
        ) as next_stage_name
    from stage_catalog
),
stage_participants as (
    select
        competition_key,
        season_label,
        stage_id,
        array_agg(distinct team_id order by team_id) as participant_ids
    from (
        select
            competition_key,
            season_label,
            stage_id,
            home_team_id as team_id
        from tie_matches
        union all
        select
            competition_key,
            season_label,
            stage_id,
            away_team_id as team_id
        from tie_matches
        union all
        select
            competition_key,
            season_label,
            stage_id,
            team_id
        from group_memberships
    ) participants
    group by
        competition_key,
        season_label,
        stage_id
),
tie_aggregates as (
    select
        provider,
        provider_league_id,
        competition_key,
        season_label,
        stage_id,
        min(stage_sk) as stage_sk,
        min(stage_name) as stage_name,
        min(stage_format) as stage_format,
        min(stage_order) as stage_order,
        tie_id,
        min(tie_order) as tie_order,
        min(home_side_team_id) as home_side_team_id,
        min(home_side_team_name) as home_side_team_name,
        min(away_side_team_id) as away_side_team_id,
        min(away_side_team_name) as away_side_team_name,
        count(*) as match_count,
        min(date_utc) as first_leg_at,
        max(date_utc) as last_leg_at,
        sum(home_side_goals) as home_side_goals,
        sum(away_side_goals) as away_side_goals,
        max(penalties.penalty_winner_team_id) as penalty_winner_team_id,
        bool_or(has_extra_time_status) as has_extra_time_match,
        bool_or(has_penalties_status) as has_penalties_match,
        bool_or(is_inferred) as is_inferred
    from tie_matches
    left join penalty_shootout_outcomes penalties
      on penalties.match_id = tie_matches.match_id
    group by
        provider,
        provider_league_id,
        competition_key,
        season_label,
        stage_id,
        tie_id
),
resolved_ties as (
    select
        t.provider,
        t.provider_league_id,
        t.competition_key,
        t.season_label,
        t.stage_id,
        t.stage_sk,
        t.stage_name,
        t.stage_format,
        t.stage_order,
        t.tie_id,
        t.tie_order,
        t.home_side_team_id,
        t.home_side_team_name,
        t.away_side_team_id,
        t.away_side_team_name,
        t.match_count,
        t.first_leg_at,
        t.last_leg_at,
        t.home_side_goals,
        t.away_side_goals,
        t.has_extra_time_match,
        t.has_penalties_match,
        sp.next_stage_id,
        sp.next_stage_name,
        case
            when t.home_side_goals > t.away_side_goals then t.home_side_team_id
            when t.away_side_goals > t.home_side_goals then t.away_side_team_id
            when t.penalty_winner_team_id is not null then t.penalty_winner_team_id
            when nsp.participant_ids is not null
             and t.home_side_team_id = any(nsp.participant_ids)
             and not (t.away_side_team_id = any(nsp.participant_ids))
                then t.home_side_team_id
            when nsp.participant_ids is not null
             and t.away_side_team_id = any(nsp.participant_ids)
             and not (t.home_side_team_id = any(nsp.participant_ids))
                then t.away_side_team_id
            else null
        end as winner_team_id,
        case
            when t.has_penalties_match then 'penalties'
            when t.has_extra_time_match then 'extra_time'
            when t.match_count = 1 and t.home_side_goals <> t.away_side_goals then 'single_match'
            when t.match_count > 1 and t.home_side_goals <> t.away_side_goals then 'aggregate'
            when nsp.participant_ids is not null then 'stage_rule'
            else null
        end as resolution_type,
        t.is_inferred
    from tie_aggregates t
    left join stage_progression sp
      on sp.competition_key = t.competition_key
     and sp.season_label = t.season_label
     and sp.stage_order = t.stage_order
    left join stage_participants nsp
      on nsp.competition_key = t.competition_key
     and nsp.season_label = t.season_label
     and nsp.stage_id = sp.next_stage_id
)
select *
from resolved_ties
