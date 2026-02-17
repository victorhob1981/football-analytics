-- Regra: resumo da liga deve ter media e intervalo de datas coerentes.
-- Tabela: league_summary
-- Rationale: evita discrepancia no agregado principal de campeonato.

select *
from {{ ref('league_summary') }}
where total_matches < 0
   or total_goals < 0
   or (
       total_matches > 0
       and avg_goals_per_match <> round(total_goals::numeric / total_matches, 4)
   )
   or (first_match_date is not null and last_match_date is not null and first_match_date > last_match_date)
