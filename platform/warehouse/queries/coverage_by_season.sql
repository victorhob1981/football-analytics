-- Diagnostico P2.1
-- Cobertura de stats por temporada (season em raw.fixtures).
--
-- metricas:
-- - fixtures_total: total de fixtures na temporada
-- - fixtures_with_any_stats: fixtures com >= 1 linha em raw.match_statistics
-- - fixtures_with_two_teams_stats: fixtures com >= 2 linhas (home + away)
-- - percentuais de cobertura sobre fixtures_total

with fixtures_by_season as (
    select
        f.season,
        count(*) as fixtures_total
    from raw.fixtures f
    where f.fixture_id is not null
    group by f.season
),
stats_rows_by_fixture as (
    select
        s.fixture_id,
        count(*) as team_rows
    from raw.match_statistics s
    group by s.fixture_id
),
coverage as (
    select
        f.season,
        count(*) filter (where sr.fixture_id is not null) as fixtures_with_any_stats,
        count(*) filter (where coalesce(sr.team_rows, 0) >= 2) as fixtures_with_two_teams_stats
    from raw.fixtures f
    left join stats_rows_by_fixture sr
      on sr.fixture_id = f.fixture_id
    where f.fixture_id is not null
    group by f.season
)
select
    fs.season,
    fs.fixtures_total,
    coalesce(c.fixtures_with_any_stats, 0) as fixtures_with_any_stats,
    coalesce(c.fixtures_with_two_teams_stats, 0) as fixtures_with_two_teams_stats,
    round(
        (coalesce(c.fixtures_with_any_stats, 0)::numeric / nullif(fs.fixtures_total, 0)) * 100.0,
        2
    ) as pct_with_any_stats,
    round(
        (coalesce(c.fixtures_with_two_teams_stats, 0)::numeric / nullif(fs.fixtures_total, 0)) * 100.0,
        2
    ) as pct_with_two_teams_stats
from fixtures_by_season fs
left join coverage c
  on c.season = fs.season
order by fs.season;
