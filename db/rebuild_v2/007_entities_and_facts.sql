\set ON_ERROR_STOP on
\if :{?rebuild_run_key}
\else
  \set rebuild_run_key 'mart-v2-local-current'
\endif

SELECT rebuild_run_id
FROM control.rebuild_run
WHERE run_key = :'rebuild_run_key'\gset rebuild_

DROP TABLE IF EXISTS mart_v2.fact_world_cup_squad CASCADE;
DROP TABLE IF EXISTS mart_v2.team_asset CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_world_cup_goal CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_match_event CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_player_valuation CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_transfer CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_standing CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_lineup CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_match_player_stats CASCADE;
DROP TABLE IF EXISTS mart_v2.fact_match_team_stats CASCADE;
DROP TABLE IF EXISTS mart_v2.dim_venue CASCADE;
DROP TABLE IF EXISTS mart_v2.dim_coach CASCADE;
DROP TABLE IF EXISTS mart_v2.dim_player CASCADE;

CREATE TABLE mart_v2.dim_player (
  player_key            text PRIMARY KEY,
  display_name          text,
  nationality           text,
  date_of_birth         date,
  position_name         text,
  preferred_foot        text,
  source_count          integer NOT NULL DEFAULT 0,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.dim_coach (
  coach_key             text PRIMARY KEY,
  display_name          text NOT NULL,
  image_url             text,
  source_count          integer NOT NULL DEFAULT 0,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.dim_venue (
  venue_key             text PRIMARY KEY,
  source_venue_id       text,
  venue_name            text NOT NULL,
  city                  text,
  country_name          text,
  source_count          integer NOT NULL DEFAULT 0,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.team_asset (
  asset_key             text PRIMARY KEY,
  team_id               bigint NOT NULL REFERENCES mart_v2.dim_team(team_id),
  asset_type            text NOT NULL,
  asset_url             text NOT NULL,
  source_system         text NOT NULL,
  source_record_key     text NOT NULL,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.fact_match_team_stats (
  match_id              bigint NOT NULL REFERENCES mart_v2.fact_match(match_id),
  team_id               bigint REFERENCES mart_v2.dim_team(team_id),
  source_system         text NOT NULL,
  total_shots           integer,
  shots_on_goal         integer,
  shots_off_goal        integer,
  blocked_shots         integer,
  shots_inside_box      integer,
  shots_outside_box     integer,
  fouls                 integer,
  corner_kicks          integer,
  offsides              integer,
  ball_possession       numeric(6,2),
  yellow_cards          integer,
  red_cards             integer,
  goalkeeper_saves      integer,
  total_passes          integer,
  passes_accurate       integer,
  passes_pct            numeric(6,2),
  source_record_key     text NOT NULL,
  source_run_id         text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  PRIMARY KEY (match_id, team_id, source_system, source_record_key)
);

CREATE TABLE mart_v2.fact_match_player_stats (
  match_id              bigint NOT NULL REFERENCES mart_v2.fact_match(match_id),
  player_key            text NOT NULL REFERENCES mart_v2.dim_player(player_key),
  team_id               bigint REFERENCES mart_v2.dim_team(team_id),
  source_system         text NOT NULL,
  minutes_played       numeric(8,2),
  rating                numeric(8,3),
  goals                numeric(8,2),
  assists               numeric(8,2),
  total_shots           numeric(8,2),
  shots_on_target       numeric(8,2),
  total_passes          numeric(8,2),
  accurate_passes       numeric(8,2),
  pass_accuracy_pct     numeric(8,3),
  fouls                 numeric(8,2),
  yellow_cards          numeric(8,2),
  red_cards             numeric(8,2),
  raw_metric_types      jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_record_key     text NOT NULL,
  source_run_id         text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  PRIMARY KEY (match_id, player_key, team_id, source_system, source_record_key)
);

CREATE TABLE mart_v2.fact_lineup (
  match_id              bigint NOT NULL REFERENCES mart_v2.fact_match(match_id),
  player_key            text NOT NULL REFERENCES mart_v2.dim_player(player_key),
  team_id               bigint REFERENCES mart_v2.dim_team(team_id),
  source_system         text NOT NULL,
  lineup_type           text,
  position_name         text,
  jersey_number         integer,
  formation_position     integer,
  is_captain            boolean,
  source_record_key     text NOT NULL,
  source_run_id         text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  PRIMARY KEY (match_id, player_key, team_id, source_system, source_record_key)
);

CREATE TABLE mart_v2.fact_standing (
  edition_key           text NOT NULL REFERENCES mart_v2.dim_edition(edition_key),
  round_key             text REFERENCES mart_v2.dim_round(round_key),
  team_id               bigint NOT NULL REFERENCES mart_v2.dim_team(team_id),
  source_system         text NOT NULL,
  position              integer,
  points                integer,
  games_played         integer,
  wins                  integer,
  draws                 integer,
  losses                integer,
  goals_for             integer,
  goals_against        integer,
  goal_difference       integer,
  source_record_key     text NOT NULL,
  source_run_id         text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  PRIMARY KEY (edition_key, team_id, source_system, source_record_key)
);

CREATE TABLE mart_v2.fact_transfer (
  transfer_key          text PRIMARY KEY,
  player_key            text NOT NULL REFERENCES mart_v2.dim_player(player_key),
  from_team_id          bigint REFERENCES mart_v2.dim_team(team_id),
  to_team_id            bigint REFERENCES mart_v2.dim_team(team_id),
  transfer_date         date,
  transfer_type         text,
  fee_text              text,
  source_system         text NOT NULL,
  source_record_key     text NOT NULL,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.fact_player_valuation (
  valuation_key         text PRIMARY KEY,
  player_key            text NOT NULL REFERENCES mart_v2.dim_player(player_key),
  valuation_date        date,
  market_value_eur      numeric,
  current_club_id       bigint REFERENCES mart_v2.dim_team(team_id),
  source_system         text NOT NULL,
  source_record_key     text NOT NULL,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.fact_match_event (
  event_key             text PRIMARY KEY,
  match_id              bigint NOT NULL REFERENCES mart_v2.fact_match(match_id),
  team_id               bigint REFERENCES mart_v2.dim_team(team_id),
  player_key            text REFERENCES mart_v2.dim_player(player_key),
  assist_player_key     text REFERENCES mart_v2.dim_player(player_key),
  source_system         text NOT NULL,
  source_event_id       text NOT NULL,
  period                integer,
  minute                integer,
  extra_minute          integer,
  event_type            text,
  event_detail          text,
  is_goal               boolean NOT NULL DEFAULT false,
  source_record_key     text NOT NULL,
  source_run_id         text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.fact_world_cup_goal (
  goal_key              text PRIMARY KEY,
  match_id              bigint REFERENCES mart_v2.fact_match(match_id),
  edition_key           text REFERENCES mart_v2.dim_edition(edition_key),
  team_id               bigint REFERENCES mart_v2.dim_team(team_id),
  player_key            text REFERENCES mart_v2.dim_player(player_key),
  minute_regulation     integer,
  minute_stoppage       integer,
  is_penalty            boolean,
  is_own_goal           boolean,
  source_record_key     text NOT NULL,
  source_run_id         text,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id)
);

CREATE TABLE mart_v2.fact_world_cup_squad (
  edition_key           text NOT NULL REFERENCES mart_v2.dim_edition(edition_key),
  team_id               bigint REFERENCES mart_v2.dim_team(team_id),
  player_key            text NOT NULL REFERENCES mart_v2.dim_player(player_key),
  jersey_number         integer,
  position_name         text,
  source_system         text NOT NULL,
  source_record_key     text NOT NULL,
  metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
  rebuild_run_id        bigint NOT NULL REFERENCES control.rebuild_run(rebuild_run_id),
  PRIMARY KEY (edition_key, team_id, player_key, source_system, source_record_key)
);

CREATE INDEX fact_match_player_stats_v2_match_idx ON mart_v2.fact_match_player_stats (match_id, team_id);
CREATE INDEX fact_match_player_stats_v2_player_idx ON mart_v2.fact_match_player_stats (player_key, match_id);
CREATE INDEX fact_lineup_v2_match_idx ON mart_v2.fact_lineup (match_id, team_id);
CREATE INDEX fact_standing_v2_edition_idx ON mart_v2.fact_standing (edition_key, position);
CREATE INDEX fact_event_v2_match_idx ON mart_v2.fact_match_event (match_id, minute);
CREATE INDEX fact_wc_goal_v2_match_idx ON mart_v2.fact_world_cup_goal (match_id);

WITH player_source AS (
  SELECT
    'transfermarkt'::text AS source_system,
    p.player_id::text AS source_entity_id,
    p.name::text AS display_name,
    p.country_of_citizenship::text AS nationality,
    CASE WHEN p.date_of_birth_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN p.date_of_birth_raw::date END AS date_of_birth,
    coalesce(p.position, p.sub_position)::text AS position_name,
    p.foot::text AS preferred_foot,
    jsonb_build_object('current_club_id', p.current_club_id, 'source_url', p.url) AS metadata
  FROM raw_src.tm_players p
  UNION ALL
  SELECT DISTINCT
    'sportmonks',
    e.player_id::text,
    e.player_name,
    NULL::text,
    NULL::date,
    NULL::text,
    NULL::text,
    jsonb_build_object('fixture_id', e.fixture_id)
  FROM raw_src.match_events e
  WHERE e.player_id IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'sportmonks',
    e.assist_id::text,
    e.assist_name,
    NULL::text,
    NULL::date,
    NULL::text,
    NULL::text,
    jsonb_build_object('fixture_id', e.fixture_id, 'role', 'assist')
  FROM raw_src.match_events e
  WHERE e.assist_id IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'statsbomb_open_data',
    l.source_player_id::text,
    l.source_player_name,
    l.country_name::text,
    NULL::date,
    NULL::text,
    NULL::text,
    jsonb_build_object('player_identity_status', l.player_identity_status)
  FROM raw_src.statsbomb_lineups l
  WHERE l.source_player_id IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'sportmonks',
    l.player_id::text,
    NULL::text,
    NULL::text,
    NULL::date,
    NULL::text,
    NULL::text,
    jsonb_build_object('source_table', 'raw.fixture_lineups')
  FROM raw_src.fixture_lineups l
  WHERE l.player_id IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'sportmonks',
    s.player_id::text,
    NULL::text,
    NULL::text,
    NULL::date,
    NULL::text,
    NULL::text,
    jsonb_build_object('source_table', 'raw.fixture_player_statistics')
  FROM raw_src.fixture_player_statistics s
  WHERE s.player_id IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'fjelstul_worldcup',
    coalesce(s.player_internal_id, s.source_player_id)::text,
    s.player_name::text,
    NULL::text,
    NULL::date,
    s.position_name::text,
    NULL::text,
    jsonb_build_object('edition_key', s.edition_key, 'team_internal_id', s.team_internal_id)
  FROM raw_src.wc_squads s
  WHERE coalesce(s.player_internal_id, s.source_player_id) IS NOT NULL
)
INSERT INTO mart_v2.dim_player (
  player_key, display_name, nationality, date_of_birth, position_name,
  preferred_foot, source_count, metadata, rebuild_run_id
)
SELECT
  md5('player:' || source_system || ':' || source_entity_id),
  max(NULLIF(trim(display_name), '')),
  max(NULLIF(trim(nationality), '')),
  max(date_of_birth),
  max(NULLIF(trim(position_name), '')),
  max(NULLIF(trim(preferred_foot), '')),
  count(*)::integer,
  jsonb_agg(metadata),
  :rebuild_rebuild_run_id
FROM player_source
WHERE NULLIF(trim(source_entity_id), '') IS NOT NULL
GROUP BY source_system, source_entity_id
ON CONFLICT (player_key) DO UPDATE
SET display_name = coalesce(EXCLUDED.display_name, mart_v2.dim_player.display_name),
    nationality = coalesce(EXCLUDED.nationality, mart_v2.dim_player.nationality),
    date_of_birth = coalesce(EXCLUDED.date_of_birth, mart_v2.dim_player.date_of_birth),
    position_name = coalesce(EXCLUDED.position_name, mart_v2.dim_player.position_name),
    source_count = greatest(mart_v2.dim_player.source_count, EXCLUDED.source_count),
    metadata = mart_v2.dim_player.metadata || EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.dim_coach (
  coach_key, display_name, image_url, source_count, metadata, rebuild_run_id
)
SELECT
  md5('coach:sportmonks:' || coach_id::text),
  max(coalesce(NULLIF(trim(display_name), ''), NULLIF(trim(coach_name), ''))),
  max(image_path),
  count(*)::integer,
  jsonb_agg(to_jsonb(sportmonks_coaches)),
  :rebuild_rebuild_run_id
FROM raw_src.sportmonks_coaches
WHERE coach_id IS NOT NULL
GROUP BY coach_id
ON CONFLICT (coach_key) DO UPDATE
SET display_name = coalesce(EXCLUDED.display_name, mart_v2.dim_coach.display_name),
    image_url = coalesce(EXCLUDED.image_url, mart_v2.dim_coach.image_url),
    source_count = greatest(mart_v2.dim_coach.source_count, EXCLUDED.source_count),
    metadata = mart_v2.dim_coach.metadata || EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

INSERT INTO mart_v2.dim_coach (
  coach_key, display_name, image_url, source_count, metadata, rebuild_run_id
)
SELECT
  md5('coach:legacy:' || coach_id::text),
  max(coach_name),
  max(image_path),
  count(*)::integer,
  jsonb_agg(jsonb_build_object('provider', provider)),
  :rebuild_rebuild_run_id
FROM raw_src.coaches
WHERE coach_id IS NOT NULL
GROUP BY coach_id
ON CONFLICT (coach_key) DO UPDATE
SET display_name = coalesce(EXCLUDED.display_name, mart_v2.dim_coach.display_name),
    image_url = coalesce(EXCLUDED.image_url, mart_v2.dim_coach.image_url),
    source_count = greatest(mart_v2.dim_coach.source_count, EXCLUDED.source_count),
    metadata = mart_v2.dim_coach.metadata || EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

WITH venue_source AS (
  SELECT DISTINCT
    'sportmonks'::text AS source_system,
    f.venue_id::text AS source_venue_id,
    f.venue_name,
    f.venue_city,
    NULL::text AS country_name
  FROM raw_src.fixtures f
  WHERE f.venue_id IS NOT NULL OR NULLIF(trim(f.venue_name), '') IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'statsbomb_open_data',
    m.stadium_id::text,
    m.stadium_name,
    NULL,
    NULL
  FROM raw_src.statsbomb_matches m
  WHERE m.stadium_id IS NOT NULL OR NULLIF(trim(m.stadium_name), '') IS NOT NULL
  UNION ALL
  SELECT DISTINCT
    'transfermarkt',
    NULL,
    g.stadium,
    NULL,
    NULL
  FROM raw_src.tm_games g
  WHERE NULLIF(trim(g.stadium), '') IS NOT NULL
)
INSERT INTO mart_v2.dim_venue (
  venue_key, source_venue_id, venue_name, city, country_name,
  source_count, metadata, rebuild_run_id
)
SELECT
  md5('venue:' || source_system || ':' || coalesce(source_venue_id, lower(regexp_replace(unaccent(venue_name), '[^a-zA-Z0-9]+', '_', 'g')))),
  max(source_venue_id),
  coalesce(max(NULLIF(trim(venue_name), '')), 'Local não informado'),
  max(NULLIF(trim(venue_city), '')),
  max(NULLIF(trim(country_name), '')),
  count(*)::integer,
  jsonb_agg(jsonb_build_object('source_system', source_system)),
  :rebuild_rebuild_run_id
FROM venue_source
GROUP BY source_system, coalesce(source_venue_id, lower(regexp_replace(unaccent(venue_name), '[^a-zA-Z0-9]+', '_', 'g')));

INSERT INTO mart_v2.team_asset (
  asset_key, team_id, asset_type, asset_url, source_system, source_record_key,
  metadata, rebuild_run_id
)
SELECT
  md5('team-asset:transfermarkt:flag:' || nt.national_team_id::text),
  si.canonical_entity_id::bigint,
  'flag',
  nt.team_image_url,
  'transfermarkt',
  nt.national_team_id::text,
  jsonb_build_object('team_name', nt.name, 'country_code', nt.country_code),
  :rebuild_rebuild_run_id
FROM raw_src.tm_national_teams nt
JOIN control.entity_source_identity si
  ON si.source_system = 'transfermarkt'
 AND si.entity_type = 'team'
 AND si.source_entity_id = nt.national_team_id::text
 AND si.mapping_state = 'approved'
WHERE NULLIF(trim(nt.team_image_url), '') IS NOT NULL
ON CONFLICT (asset_key) DO UPDATE
SET asset_url = EXCLUDED.asset_url,
    metadata = EXCLUDED.metadata,
    rebuild_run_id = EXCLUDED.rebuild_run_id;

WITH team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'legacy_control' AND entity_type = 'team' AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_match_team_stats (
  match_id, team_id, source_system, total_shots, shots_on_goal, shots_off_goal,
  blocked_shots, shots_inside_box, shots_outside_box, fouls, corner_kicks,
  offsides, ball_possession, yellow_cards, red_cards, goalkeeper_saves,
  total_passes, passes_accurate, passes_pct, source_record_key, source_run_id,
  metadata, rebuild_run_id
)
SELECT
  ms.fixture_id,
  coalesce(tm.canonical_id, fm.home_team_id, fm.away_team_id),
  coalesce(NULLIF(trim(ms.provider), ''), 'sportmonks'),
  ms.total_shots, ms.shots_on_goal, ms.shots_off_goal, ms.blocked_shots,
  ms.shots_inside_box, ms.shots_outside_box, ms.fouls, ms.corner_kicks,
  ms.offsides, ms.ball_possession, ms.yellow_cards, ms.red_cards,
  ms.goalkeeper_saves, ms.total_passes, ms.passes_accurate, ms.passes_pct,
  ms.fixture_id::text || ':' || ms.team_id::text,
  ms.source_run_id,
  jsonb_build_object('team_name', ms.team_name),
  :rebuild_rebuild_run_id
FROM raw_src.match_statistics ms
JOIN mart_v2.fact_match fm ON fm.match_id = ms.fixture_id
LEFT JOIN team_map tm ON tm.source_entity_id = ms.team_id::text
ON CONFLICT DO NOTHING;

WITH player_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id,
    md5('player:sportmonks:' || source_entity_id) AS player_key
  FROM control.entity_source_identity
  WHERE false
), team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'legacy_control' AND entity_type = 'team' AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_match_player_stats (
  match_id, player_key, team_id, source_system, minutes_played, rating,
  goals, assists, total_shots, shots_on_target, total_passes, accurate_passes,
  pass_accuracy_pct, fouls, yellow_cards, red_cards, raw_metric_types,
  source_record_key, source_run_id, metadata, rebuild_run_id
)
SELECT
  ps.fixture_id,
  md5('player:sportmonks:' || ps.player_id::text),
  tm.canonical_id,
  coalesce(NULLIF(trim(ps.provider), ''), 'sportmonks'),
  metrics.minutes_played,
  metrics.rating,
  metrics.goals,
  metrics.assists,
  metrics.total_shots,
  metrics.shots_on_target,
  metrics.total_passes,
  metrics.accurate_passes,
  metrics.pass_accuracy_pct,
  metrics.fouls,
  metrics.yellow_cards,
  metrics.red_cards,
  metrics.raw_metric_types,
  ps.fixture_id::text || ':' || ps.team_id::text || ':' || ps.player_id::text,
  ps.source_run_id,
  jsonb_build_object('provider_league_id', ps.provider_league_id, 'provider_season_id', ps.provider_season_id),
  :rebuild_rebuild_run_id
FROM raw_src.fixture_player_statistics ps
JOIN mart_v2.fact_match fm ON fm.match_id = ps.fixture_id
LEFT JOIN team_map tm ON tm.source_entity_id = ps.team_id::text
JOIN LATERAL (
  SELECT
    max(CASE WHEN e->>'type' = 'minutes_played' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS minutes_played,
    max(CASE WHEN e->>'type' = 'rating' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS rating,
    max(CASE WHEN e->>'type' = 'goals' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS goals,
    max(CASE WHEN e->>'type' = 'assists' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS assists,
    max(CASE WHEN e->>'type' = 'total_shots' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS total_shots,
    max(CASE WHEN e->>'type' IN ('shots_on_goal', 'shots_on_target') THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS shots_on_target,
    max(CASE WHEN e->>'type' = 'total_passes' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS total_passes,
    max(CASE WHEN e->>'type' = 'accurate_passes' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS accurate_passes,
    max(CASE WHEN e->>'type' = 'accurate_passes_percentage' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS pass_accuracy_pct,
    max(CASE WHEN e->>'type' = 'fouls' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS fouls,
    max(CASE WHEN e->>'type' = 'yellow_cards' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS yellow_cards,
    max(CASE WHEN e->>'type' = 'red_cards' THEN NULLIF(regexp_replace(coalesce(e->'raw_value'->>'value', e->>'value'), '[^0-9.-]', '', 'g'), '')::numeric END) AS red_cards,
    coalesce(jsonb_object_agg(e->>'type', coalesce(e->'raw_value'->'value', e->'value')) FILTER (WHERE e->>'type' IS NOT NULL), '{}'::jsonb) AS raw_metric_types
  FROM jsonb_array_elements(coalesce(ps.statistics, '[]'::jsonb)) e
) metrics ON true
ON CONFLICT DO NOTHING;

WITH team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'legacy_control' AND entity_type = 'team' AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_lineup (
  match_id, player_key, team_id, source_system, lineup_type, position_name,
  jersey_number, formation_position, is_captain, source_record_key,
  source_run_id, metadata, rebuild_run_id
)
SELECT
  l.fixture_id,
  md5('player:sportmonks:' || l.player_id::text),
  tm.canonical_id,
  coalesce(NULLIF(trim(l.provider), ''), 'sportmonks'),
  l.lineup_type_id::text,
  l.position_name,
  l.jersey_number,
  l.formation_position,
  NULLIF(lower(l.details ->> 'captain'), '') IN ('true', '1', 'yes'),
  l.fixture_id::text || ':' || l.team_id::text || ':' || l.player_id::text || ':' || coalesce(l.lineup_id::text, ''),
  l.source_run_id,
  jsonb_build_object('position_id', l.position_id, 'formation_field', l.formation_field),
  :rebuild_rebuild_run_id
FROM raw_src.fixture_lineups l
JOIN mart_v2.fact_match fm ON fm.match_id = l.fixture_id
LEFT JOIN team_map tm ON tm.source_entity_id = l.team_id::text
WHERE l.player_id IS NOT NULL
ON CONFLICT DO NOTHING;

WITH team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'legacy_control' AND entity_type = 'team' AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_standing (
  edition_key, round_key, team_id, source_system, position, points,
  games_played, wins, draws, losses, goals_for, goals_against,
  goal_difference, source_record_key, source_run_id, metadata, rebuild_run_id
)
SELECT
  s.competition_key || ':' || s.season_label,
  r.round_key,
  tm.canonical_id,
  coalesce(NULLIF(trim(s.provider), ''), 'sportmonks'),
  s.position, s.points, s.games_played, s.won, s.draw, s.lost,
  s.goals_for, s.goals_against, s.goal_diff,
  coalesce(s.provider_league_id::text, '') || ':' || coalesce(s.provider_season_id::text, '') || ':' || s.team_id::text || ':' || coalesce(s.round_id::text, ''),
  s.source_run_id,
  jsonb_build_object('stage_id', s.stage_id, 'round_id', s.round_id),
  :rebuild_rebuild_run_id
FROM raw_src.standings_snapshots s
JOIN mart_v2.dim_edition e ON e.edition_key = s.competition_key || ':' || s.season_label
LEFT JOIN mart_v2.dim_round r ON r.edition_key = e.edition_key AND r.round_id = s.round_id
LEFT JOIN team_map tm ON tm.source_entity_id = s.team_id::text
WHERE tm.canonical_id IS NOT NULL
ON CONFLICT DO NOTHING;

WITH team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'transfermarkt' AND entity_type = 'team' AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_transfer (
  transfer_key, player_key, from_team_id, to_team_id, transfer_date,
  transfer_type, fee_text, source_system, source_record_key, metadata, rebuild_run_id
)
SELECT
  md5('transfer:transfermarkt:' || coalesce(t.player_id, '') || ':' || coalesce(t.transfer_date_raw, '') || ':' || coalesce(t.from_club_id, '') || ':' || coalesce(t.to_club_id, '')),
  md5('player:transfermarkt:' || t.player_id),
  fm.canonical_id,
  tm.canonical_id,
  CASE WHEN t.transfer_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN t.transfer_date_raw::date END,
  'transfer',
  t.transfer_fee,
  'transfermarkt',
  coalesce(t.player_id, '') || ':' || coalesce(t.transfer_date_raw, '') || ':' || coalesce(t.from_club_id, '') || ':' || coalesce(t.to_club_id, ''),
  jsonb_build_object('player_name', t.player_name, 'market_value_eur', t.market_value_in_eur),
  :rebuild_rebuild_run_id
FROM raw_src.tm_transfers t
LEFT JOIN team_map fm ON fm.source_entity_id = t.from_club_id
LEFT JOIN team_map tm ON tm.source_entity_id = t.to_club_id
JOIN mart_v2.dim_player p ON p.player_key = md5('player:transfermarkt:' || t.player_id)
ON CONFLICT DO NOTHING;

INSERT INTO mart_v2.fact_player_valuation (
  valuation_key, player_key, valuation_date, market_value_eur,
  current_club_id, source_system, source_record_key, metadata, rebuild_run_id
)
SELECT
  md5('valuation:transfermarkt:' || t.player_id || ':' || t.valuation_date_raw),
  md5('player:transfermarkt:' || t.player_id),
  CASE WHEN t.valuation_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN t.valuation_date_raw::date END,
  CASE WHEN regexp_replace(t.market_value_in_eur, '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\\.[0-9]+)?$'
       THEN regexp_replace(t.market_value_in_eur, '[^0-9.-]', '', 'g')::numeric END,
  NULL,
  'transfermarkt',
  t.player_id || ':' || t.valuation_date_raw,
  jsonb_build_object('current_club_id', t.current_club_id, 'current_club_name', t.current_club_name),
  :rebuild_rebuild_run_id
FROM raw_src.tm_player_valuations t
JOIN mart_v2.dim_player p ON p.player_key = md5('player:transfermarkt:' || t.player_id)
ON CONFLICT DO NOTHING;

WITH event_source AS (
  SELECT * FROM raw_src.match_events
), team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'legacy_control' AND entity_type = 'team' AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_match_event (
  event_key, match_id, team_id, player_key, assist_player_key, source_system,
  source_event_id, period, minute, extra_minute, event_type, event_detail,
  is_goal, source_record_key, source_run_id, metadata, rebuild_run_id
)
SELECT
  md5('event:sportmonks:' || e.fixture_id::text || ':' || e.event_id),
  e.fixture_id,
  tm.canonical_id,
  CASE WHEN e.player_id IS NOT NULL THEN md5('player:sportmonks:' || e.player_id::text) END,
  CASE WHEN e.assist_id IS NOT NULL THEN md5('player:sportmonks:' || e.assist_id::text) END,
  coalesce(NULLIF(trim(e.provider), ''), 'sportmonks'),
  e.event_id,
  NULL,
  e.time_elapsed,
  e.time_extra,
  e.type,
  e.detail,
  lower(coalesce(e.type, '')) = 'goal' OR lower(coalesce(e.detail, '')) IN ('normal goal', 'penalty', 'own goal'),
  e.fixture_id::text || ':' || e.event_id,
  e.source_run_id,
  jsonb_build_object('team_name', e.team_name, 'player_name', e.player_name, 'assist_name', e.assist_name, 'comments', e.comments),
  :rebuild_rebuild_run_id
FROM event_source e
JOIN mart_v2.fact_match fm ON fm.match_id = e.fixture_id
LEFT JOIN team_map tm ON tm.source_entity_id = e.team_id::text
ON CONFLICT DO NOTHING;

WITH wc_team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'fjelstul_worldcup'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_world_cup_goal (
  goal_key, match_id, edition_key, team_id, player_key, minute_regulation,
  minute_stoppage, is_penalty, is_own_goal, source_record_key,
  source_run_id, metadata, rebuild_run_id
)
SELECT
  'wc:' || g.wc_goal_pk::text,
  coalesce(g.fixture_id, ms.canonical_match_id),
  g.competition_key || ':' || g.season_label,
  wtm.canonical_id,
  CASE WHEN dp.player_key IS NOT NULL THEN dp.player_key END,
  g.minute_regulation,
  g.minute_stoppage,
  g.is_penalty,
  g.is_own_goal,
  g.wc_goal_pk::text,
  g.source_run_id,
  jsonb_build_object('edition_key', g.edition_key, 'source_match_id', g.source_match_id, 'team_name', g.team_name, 'player_name', g.player_name),
  :rebuild_rebuild_run_id
FROM raw_src.wc_goals g
LEFT JOIN mart_v2.match_source ms ON ms.source_system = 'fjelstul_worldcup' AND ms.source_match_id = g.source_match_id
LEFT JOIN wc_team_map wtm ON wtm.source_entity_id = g.team_id::text
LEFT JOIN mart_v2.dim_player dp ON dp.player_key = md5('player:fjelstul_worldcup:' || coalesce(g.player_internal_id, g.source_player_id))
WHERE g.wc_goal_pk IS NOT NULL
ON CONFLICT DO NOTHING;

WITH wc_team_map AS (
  SELECT DISTINCT ON (source_entity_id)
    source_entity_id, canonical_entity_id::bigint AS canonical_id
  FROM control.entity_source_identity
  WHERE source_system = 'fjelstul_worldcup'
    AND entity_type = 'team'
    AND mapping_state = 'approved'
  ORDER BY source_entity_id, confidence DESC NULLS LAST
)
INSERT INTO mart_v2.fact_world_cup_squad (
  edition_key, team_id, player_key, jersey_number, position_name,
  source_system, source_record_key, metadata, rebuild_run_id
)
SELECT
  s.competition_key || ':' || s.season_label,
  wtm.canonical_id,
  md5('player:fjelstul_worldcup:' || coalesce(s.player_internal_id, s.source_player_id)),
  s.jersey_number,
  s.position_name,
  coalesce(NULLIF(trim(s.provider), ''), 'fjelstul_worldcup'),
  s.wc_squad_pk::text,
  jsonb_build_object('edition_key', s.edition_key, 'team_name', s.team_name, 'player_name', s.player_name, 'team_code', s.team_code),
  :rebuild_rebuild_run_id
FROM raw_src.wc_squads s
JOIN mart_v2.dim_edition e ON e.edition_key = s.competition_key || ':' || s.season_label
JOIN wc_team_map wtm ON wtm.source_entity_id = s.team_id::text
JOIN mart_v2.dim_player p ON p.player_key = md5('player:fjelstul_worldcup:' || coalesce(s.player_internal_id, s.source_player_id))
ON CONFLICT DO NOTHING;

INSERT INTO control.coverage_snapshot (
  rebuild_run_id, entity_type, source_system, competition_key, edition_key,
  observed_rows, accepted_rows, quarantined_rows, first_date, last_date, metadata
)
SELECT :rebuild_rebuild_run_id, 'player_match_stats', source_system, NULL, NULL,
       count(*), count(*), 0, NULL, NULL, '{}'::jsonb
FROM mart_v2.fact_match_player_stats
GROUP BY source_system
ON CONFLICT (rebuild_run_id, entity_type, source_system, competition_key, edition_key) DO UPDATE
SET observed_rows = EXCLUDED.observed_rows,
    accepted_rows = EXCLUDED.accepted_rows,
    quarantined_rows = EXCLUDED.quarantined_rows;

UPDATE control.rebuild_run
SET phase = 'entities_and_facts', status = 'succeeded', finished_at = now()
WHERE rebuild_run_id = :rebuild_rebuild_run_id;
