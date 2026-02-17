-- Regra: evento de gol deve ter time associado.
-- Tabela: fact_match_events
-- Rationale: gol sem time inviabiliza agregacoes por equipe.

select *
from {{ ref('fact_match_events') }}
where event_type = 'Goal'
  and team_sk is null
