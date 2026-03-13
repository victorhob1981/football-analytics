-- Regra: pontos acumulados nao podem diminuir ao longo das rodadas de um mesmo stage.
-- Tabela: standings_evolution
-- Rationale: para formatos hibridos, o acumulado deve ser monotonicamente nao-decrescente
-- dentro do stage, sem assumir que toda a competicao usa uma tabela unica de pontos corridos.

with ordered as (
    select
        competition_sk,
        season_label,
        coalesce(stage_sk, '__no_stage__') as stage_scope,
        team_id,
        round_key,
        points_accumulated,
        lag(points_accumulated) over (
            partition by competition_sk, season_label, coalesce(stage_sk, '__no_stage__'), team_id
            order by round_key
        ) as prev_points
    from {{ ref('standings_evolution') }}
)
select *
from ordered
where prev_points is not null
  and points_accumulated < prev_points
