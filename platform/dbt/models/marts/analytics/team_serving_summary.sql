{{ config(
    materialized='table',
    indexes=[
        {'columns': ['team_id'], 'unique': true, 'type': 'btree'},
        {'columns': ['season_count desc', 'competition_count desc', 'matches_played desc', 'first_match_at', 'last_match_at desc', 'team_id'], 'type': 'btree'},
        {'columns': ['points desc', 'team_id'], 'type': 'btree'},
        {'columns': ['goal_diff desc', 'team_id'], 'type': 'btree'},
        {'columns': ['wins desc', 'team_id'], 'type': 'btree'},
        {'columns': ['team_name', 'team_id'], 'type': 'btree'}
    ]
) }}

with identity_registry as (
    select
        canonical_team_id,
        team_type,
        country_or_territory,
        decision_evidence
    from control.team_identity
    where identity_state = 'active'
),
legacy_identity as (
    select
        legacy_member.value::bigint as team_id,
        ir.team_type,
        ir.country_or_territory
    from identity_registry ir
    cross join lateral jsonb_array_elements_text(
        coalesce(ir.decision_evidence -> 'legacy_members', '[]'::jsonb)
    ) as legacy_member(value)
    where legacy_member.value ~ '^[0-9]+$'
),
transfermarkt_clubs as (
    select
        club_id::bigint as team_id,
        max(nullif(trim(stadium_name), '')) as stadium_name
    from raw.tm_clubs
    where club_id is not null
    group by club_id
),
base as (
    select
        tr.*,
        fm.competition_key,
        fm.season_label,
        coalesce(direct_identity.team_type, legacy.team_type, 'unknown') as team_type,
        coalesce(direct_identity.country_or_territory, legacy.country_or_territory) as country_or_territory,
        tmc.stadium_name
    from {{ ref('int_team_match_rows') }} tr
    left join {{ ref('fact_matches') }} fm
      on fm.match_id = tr.match_id
    left join identity_registry direct_identity
      on direct_identity.canonical_team_id = tr.team_id
    left join legacy_identity legacy
      on legacy.team_id = tr.team_id
    left join transfermarkt_clubs tmc
      on tmc.team_id = tr.team_id
    where tr.team_id is not null
)
select
    base.team_id,
    max(coalesce(dt.team_name, base.team_id::text)) as team_name,
    max(base.team_type) as team_type,
    max(base.country_or_territory) as country_or_territory,
    max(base.stadium_name) as stadium_name,
    count(distinct base.competition_key)::int as competition_count,
    count(distinct base.season_label)::int as season_count,
    count(*)::int as matches_played,
    min(base.date_day) as first_match_at,
    max(base.date_day) as last_match_at,
    sum(base.wins)::int as wins,
    sum(base.draws)::int as draws,
    sum(base.losses)::int as losses,
    sum(base.goals_for)::int as goals_for,
    sum(base.goals_against)::int as goals_against,
    sum(base.goals_for - base.goals_against)::int as goal_diff,
    sum(base.points_round)::int as points
from base
left join {{ ref('dim_team') }} dt on dt.team_id = base.team_id
group by base.team_id
