-- Regra: metricas de contagem/minutos em fact_fixture_player_stats nao podem ser negativas.
-- Tabela: fact_fixture_player_stats
-- Rationale: contagens operacionais invalidas causam distorcoes em analytics por jogador.

select *
from {{ ref('fact_fixture_player_stats') }}
where (minutes_played is not null and minutes_played < 0)
   or (goals is not null and goals < 0)
   or (assists is not null and assists < 0)
   or (shots_total is not null and shots_total < 0)
   or (shots_on_goal is not null and shots_on_goal < 0)
   or (passes_total is not null and passes_total < 0)
   or (key_passes is not null and key_passes < 0)
   or (tackles is not null and tackles < 0)
   or (interceptions is not null and interceptions < 0)
   or (duels is not null and duels < 0)
   or (fouls_committed is not null and fouls_committed < 0)
   or (yellow_cards is not null and yellow_cards < 0)
   or (red_cards is not null and red_cards < 0)
   or (goalkeeper_saves is not null and goalkeeper_saves < 0)
   or (clean_sheets is not null and clean_sheets < 0)
   or (xg is not null and xg < 0)
