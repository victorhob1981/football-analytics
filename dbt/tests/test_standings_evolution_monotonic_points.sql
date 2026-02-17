-- Regra: pontos acumulados nao podem diminuir ao longo das rodadas.
-- Tabela: standings_evolution
-- Rationale: acumulado deve ser monotonicamente nao-decrescente por time/temporada.

with ordered as (
    select
        season,
        team_sk,
        round,
        points_accumulated,
        lag(points_accumulated) over (
            partition by season, team_sk
            order by round
        ) as prev_points
    from {{ ref('standings_evolution') }}
)
select *
from ordered
where prev_points is not null
  and points_accumulated < prev_points
