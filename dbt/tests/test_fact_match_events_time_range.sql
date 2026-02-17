-- Regra: minuto de evento deve estar em faixa valida de jogo.
-- Tabela: fact_match_events
-- Rationale: eventos com tempo invalido indicam dado corrompido.

select *
from {{ ref('fact_match_events') }}
where time_elapsed is null
   or time_elapsed < 0
   or time_elapsed > 120
   or (time_extra is not null and (time_extra < 0 or time_extra > 30))
