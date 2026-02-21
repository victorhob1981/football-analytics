-- Diagnostico P2.1
-- Lista fixtures existentes em raw.fixtures que ainda nao possuem
-- nenhuma linha correspondente em raw.match_statistics.
--
-- Observacao: para focar somente em partidas encerradas, descomente
-- o filtro de status_short no WHERE.

with fixtures as (
    select
        f.fixture_id,
        f.league_id,
        f.season,
        f.date_utc,
        f.status_short,
        f.home_team_id,
        f.away_team_id
    from raw.fixtures f
    where f.fixture_id is not null
),
stats_presence as (
    select
        s.fixture_id,
        count(*) as stats_rows
    from raw.match_statistics s
    group by s.fixture_id
)
select
    fx.fixture_id,
    fx.league_id,
    fx.season,
    fx.date_utc,
    fx.status_short,
    fx.home_team_id,
    fx.away_team_id
from fixtures fx
left join stats_presence sp
  on sp.fixture_id = fx.fixture_id
where sp.fixture_id is null
-- and fx.status_short in ('FT', 'AET', 'PEN')
order by
    fx.season nulls last,
    fx.date_utc nulls last,
    fx.fixture_id;
