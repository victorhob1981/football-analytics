-- Regra: coluna result deve refletir corretamente o placar.
-- Tabela: fact_matches
-- Rationale: evita divergencia entre metrica textual e gols da partida.

with expected as (
    select
        match_id,
        result,
        case
            when coalesce(home_goals, 0) > coalesce(away_goals, 0) then 'Home Win'
            when coalesce(home_goals, 0) < coalesce(away_goals, 0) then 'Away Win'
            else 'Draw'
        end as expected_result
    from {{ ref('fact_matches') }}
)
select *
from expected
where result <> expected_result
