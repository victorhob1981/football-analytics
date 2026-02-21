-- Diagnostico P2.1
-- Detecta duplicatas no grain canonico de stats:
--   raw.match_statistics (fixture_id, team_id)
--
-- Resultado esperado: 0 linhas.

select
    s.fixture_id,
    s.team_id,
    count(*) as duplicate_count,
    min(s.updated_at) as first_updated_at,
    max(s.updated_at) as last_updated_at
from raw.match_statistics s
group by
    s.fixture_id,
    s.team_id
having count(*) > 1
order by
    duplicate_count desc,
    s.fixture_id,
    s.team_id;
