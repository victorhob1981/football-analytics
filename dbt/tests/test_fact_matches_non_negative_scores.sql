-- Regra: placar nao pode ser negativo.
-- Tabela: fact_matches
-- Rationale: gols de mandante/visitante sao contagens e devem ser >= 0.

select *
from {{ ref('fact_matches') }}
where coalesce(home_goals, 0) < 0
   or coalesce(away_goals, 0) < 0
   or coalesce(total_goals, 0) < 0
