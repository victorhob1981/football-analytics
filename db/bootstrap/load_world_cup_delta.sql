\set ON_ERROR_STOP on

begin;

create temp table incoming_competition_seasons (like raw.competition_seasons including defaults) on commit drop;
create temp table incoming_fixtures (like raw.fixtures including defaults) on commit drop;
create temp table incoming_standings_snapshots (like raw.standings_snapshots including defaults) on commit drop;
create temp table incoming_wc_player_identity_map (like raw.wc_player_identity_map including defaults) on commit drop;
create temp table incoming_wc_team_identity_map (like raw.wc_team_identity_map including defaults) on commit drop;
create temp table incoming_wc_squads (like raw.wc_squads including defaults) on commit drop;
create temp table incoming_wc_goals (like raw.wc_goals including defaults) on commit drop;
create temp table incoming_wc_match_events (like raw.wc_match_events including defaults) on commit drop;
create temp table incoming_wc_bookings (like raw.wc_bookings including defaults) on commit drop;
create temp table incoming_wc_substitutions (like raw.wc_substitutions including defaults) on commit drop;
create temp table incoming_wc_entity_match_review_queue (like control.wc_entity_match_review_queue including defaults) on commit drop;
create temp table incoming_wc_source_snapshots (like control.wc_source_snapshots including defaults) on commit drop;

\copy incoming_competition_seasons from '/tmp/wc_delta/raw_competition_seasons.csv' with (format csv, header true)
\copy incoming_fixtures from '/tmp/wc_delta/raw_fixtures.csv' with (format csv, header true)
\copy incoming_standings_snapshots from '/tmp/wc_delta/raw_standings_snapshots.csv' with (format csv, header true)
\copy incoming_wc_player_identity_map from '/tmp/wc_delta/raw_wc_player_identity_map.csv' with (format csv, header true)
\copy incoming_wc_team_identity_map from '/tmp/wc_delta/raw_wc_team_identity_map.csv' with (format csv, header true)
\copy incoming_wc_squads from '/tmp/wc_delta/raw_wc_squads.csv' with (format csv, header true)
\copy incoming_wc_goals from '/tmp/wc_delta/raw_wc_goals.csv' with (format csv, header true)
\copy incoming_wc_match_events from '/tmp/wc_delta/raw_wc_match_events.csv' with (format csv, header true)
\copy incoming_wc_bookings from '/tmp/wc_delta/raw_wc_bookings.csv' with (format csv, header true)
\copy incoming_wc_substitutions from '/tmp/wc_delta/raw_wc_substitutions.csv' with (format csv, header true)
\copy incoming_wc_entity_match_review_queue from '/tmp/wc_delta/control_wc_entity_match_review_queue.csv' with (format csv, header true)
\copy incoming_wc_source_snapshots from '/tmp/wc_delta/control_wc_source_snapshots.csv' with (format csv, header true)

delete from raw.wc_match_events target using incoming_wc_match_events incoming
where target.wc_match_event_pk = incoming.wc_match_event_pk;
delete from raw.wc_bookings target using incoming_wc_bookings incoming
where target.wc_booking_pk = incoming.wc_booking_pk;
delete from raw.wc_substitutions target using incoming_wc_substitutions incoming
where target.wc_substitution_pk = incoming.wc_substitution_pk;
delete from raw.wc_goals target using incoming_wc_goals incoming
where target.wc_goal_pk = incoming.wc_goal_pk;
delete from raw.wc_squads target using incoming_wc_squads incoming
where target.wc_squad_pk = incoming.wc_squad_pk;
delete from raw.wc_player_identity_map target using incoming_wc_player_identity_map incoming
where target.wc_player_id = incoming.wc_player_id;
delete from raw.wc_team_identity_map target using incoming_wc_team_identity_map incoming
where target.wc_team_id = incoming.wc_team_id;
delete from raw.standings_snapshots target using incoming_standings_snapshots incoming
where target.provider = incoming.provider
  and target.season_id = incoming.season_id
  and target.stage_id = incoming.stage_id
  and target.round_id = incoming.round_id
  and target.team_id = incoming.team_id;
delete from raw.fixtures target using incoming_fixtures incoming
where target.fixture_id = incoming.fixture_id;
delete from raw.competition_seasons target using incoming_competition_seasons incoming
where target.provider = incoming.provider
  and target.season_id = incoming.season_id;
delete from control.wc_entity_match_review_queue target using incoming_wc_entity_match_review_queue incoming
where target.review_pk = incoming.review_pk;
delete from control.wc_source_snapshots target using incoming_wc_source_snapshots incoming
where target.snapshot_pk = incoming.snapshot_pk;

insert into raw.competition_seasons select * from incoming_competition_seasons;
insert into raw.fixtures select * from incoming_fixtures;
insert into raw.standings_snapshots select * from incoming_standings_snapshots;
insert into raw.wc_player_identity_map select * from incoming_wc_player_identity_map;
insert into raw.wc_team_identity_map select * from incoming_wc_team_identity_map;
insert into raw.wc_squads select * from incoming_wc_squads;
insert into raw.wc_goals select * from incoming_wc_goals;
insert into raw.wc_match_events select * from incoming_wc_match_events;
insert into raw.wc_bookings select * from incoming_wc_bookings;
insert into raw.wc_substitutions select * from incoming_wc_substitutions;
insert into control.wc_entity_match_review_queue select * from incoming_wc_entity_match_review_queue;
insert into control.wc_source_snapshots select * from incoming_wc_source_snapshots;

analyze raw.competition_seasons;
analyze raw.fixtures;
analyze raw.standings_snapshots;
analyze raw.wc_player_identity_map;
analyze raw.wc_team_identity_map;
analyze raw.wc_squads;
analyze raw.wc_goals;
analyze raw.wc_match_events;
analyze raw.wc_bookings;
analyze raw.wc_substitutions;

commit;
