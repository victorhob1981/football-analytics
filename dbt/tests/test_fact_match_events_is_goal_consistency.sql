-- Regra: is_goal deve ser coerente com event_type.
-- Tabela: fact_match_events
-- Rationale: coluna booleana derivada precisa ser deterministica.

select *
from {{ ref('fact_match_events') }}
where is_goal <> (event_type = 'Goal')
