-- Regra: metricas por 90 em player_90_metrics nao podem ser negativas.
-- Tabela: player_90_metrics
-- Rationale: taxas por 90 devem ser >= 0 quando calculadas sobre minutos validos.

select *
from {{ ref('player_90_metrics') }}
where (goals_per_90 is not null and goals_per_90 < 0)
   or (assists_per_90 is not null and assists_per_90 < 0)
   or (shots_per_90 is not null and shots_per_90 < 0)
   or (shots_on_goal_per_90 is not null and shots_on_goal_per_90 < 0)
   or (key_passes_per_90 is not null and key_passes_per_90 < 0)
   or (tackles_per_90 is not null and tackles_per_90 < 0)
   or (interceptions_per_90 is not null and interceptions_per_90 < 0)
   or (xg_per_90 is not null and xg_per_90 < 0)
