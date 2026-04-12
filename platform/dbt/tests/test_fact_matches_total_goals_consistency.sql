-- Regra: total_goals deve ser igual a home_goals + away_goals.
-- Tabela: fact_matches
-- Rationale: consistencia basica de metrica derivada.

select *
from {{ ref('fact_matches') }}
where coalesce(total_goals, 0) <> coalesce(home_goals, 0) + coalesce(away_goals, 0)
