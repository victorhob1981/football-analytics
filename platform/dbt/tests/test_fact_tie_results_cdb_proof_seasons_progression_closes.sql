select
    tr.tie_id,
    tr.season_label,
    tr.stage_name,
    tr.winner_team_id,
    tr.next_stage_id
from {{ ref('fact_tie_results') }} tr
where tr.competition_key = 'copa_do_brasil'
  and tr.season_label in ('2024', '2025')
  and tr.next_stage_id is not null
  and not exists (
    select 1
    from {{ ref('fact_matches') }} fm
    where fm.competition_key = tr.competition_key
      and fm.season_label = tr.season_label
      and fm.stage_id = tr.next_stage_id
      and tr.winner_team_id in (fm.home_team_id, fm.away_team_id)
  )
