with player_matches as (
    select * from {{ ref('player_match_summary') }}
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
