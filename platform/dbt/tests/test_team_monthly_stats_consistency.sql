-- Regra: agregados mensais devem fechar matematicamente.
-- Tabela: team_monthly_stats
-- Rationale: garante consistencia interna das metricas derivadas.

select *
from {{ ref('team_monthly_stats') }}
where matches <> wins + draws + losses
   or points <> wins * 3 + draws
   or goal_diff <> goals_for - goals_against
