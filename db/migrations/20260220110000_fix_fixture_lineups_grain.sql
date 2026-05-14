-- migrate:up

-- Backfill deterministic lineup_id for any legacy row where provider did not send lineup_id.
UPDATE raw.fixture_lineups
SET lineup_id = (
    ('x' || substr(
        md5(
            concat_ws(
                '|',
                provider,
                fixture_id::text,
                team_id::text,
                coalesce(player_id::text, ''),
                coalesce(lineup_type_id::text, ''),
                coalesce(formation_field, ''),
                coalesce(formation_position::text, ''),
                coalesce(jersey_number::text, '')
            )
        ),
        1,
        15
    ))::bit(60)::bigint
)
WHERE lineup_id IS NULL;

ALTER TABLE raw.fixture_lineups
  DROP CONSTRAINT IF EXISTS pk_fixture_lineups;

ALTER TABLE raw.fixture_lineups
  ALTER COLUMN player_id DROP NOT NULL;

ALTER TABLE raw.fixture_lineups
  ALTER COLUMN lineup_id SET NOT NULL;

ALTER TABLE raw.fixture_lineups
  ADD CONSTRAINT pk_fixture_lineups PRIMARY KEY (provider, fixture_id, team_id, lineup_id);

CREATE INDEX IF NOT EXISTS idx_fixture_lineups_player
  ON raw.fixture_lineups (player_id);

-- migrate:down

ALTER TABLE raw.fixture_lineups
  DROP CONSTRAINT IF EXISTS pk_fixture_lineups;

UPDATE raw.fixture_lineups
SET player_id = -1 * COALESCE(lineup_id, 0)
WHERE player_id IS NULL;

ALTER TABLE raw.fixture_lineups
  ALTER COLUMN player_id SET NOT NULL;

ALTER TABLE raw.fixture_lineups
  ALTER COLUMN lineup_id DROP NOT NULL;

ALTER TABLE raw.fixture_lineups
  ADD CONSTRAINT pk_fixture_lineups PRIMARY KEY (provider, fixture_id, team_id, player_id);
