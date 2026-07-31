-- Web-verified identity decisions for the 18 2026 World Cup review rows.
-- Safe to rerun: child rows and identity maps are updated idempotently.
BEGIN;

CREATE TEMP TABLE manual_wc2026_players (
  source_external_id text PRIMARY KEY,
  source_wc_id bigint NOT NULL,
  candidate_sportmonks_id bigint,
  canonical_wc_id bigint,
  decision text NOT NULL CHECK (decision IN ('approved', 'rejected')),
  matched_name text NOT NULL,
  source_team text NOT NULL,
  evidence_url text NOT NULL,
  decision_note text NOT NULL
) ON COMMIT DROP;

INSERT INTO manual_wc2026_players VALUES
('1128',8347520331047797631,96091,7040652494743856981,'approved','James Rodríguez','Colombia','https://fcf.com.co/2024/06/15/100-partidos-del-10-de-james-rodriguez-en-nuestra-seleccion-colombia/','Nome completo e nascimento 1991-07-12 confirmados pela Federação Colombiana.'),
('1147',4085984253277996440,37562487,NULL,'approved','Nico O''Reilly','England','https://www.fifa.com/en/tournaments/mens/worldcup/canadamexicousa2026/articles/world-cup-wonderkids-nico-oreilly','Variação de apóstrofo; nascimento 2005-03-21 coincide.'),
('1218',3689717612460138334,37438667,NULL,'approved','Kamaldeen Sulemana','Ghana','https://www.staderennais.com/equipe-pro/kamaldeen-sulemana','Nome abreviado/variante; nascimento 2002-02-15 e seleção coincidem.'),
('1236',6408245659314856047,29311982,NULL,'rejected','Carlos Miguel','Panama','https://www.premierleague.com/en/players/475123/carlos-miguel/overview','Candidato é goleiro brasileiro nascido em 1998-10-09; fonte é defensor panamenho nascido em 2000-02-03.'),
('165',8760047024514089941,301745,7040676452472864076,'approved','Mohammed Muntari','Qatar','https://www.mlssoccer.com/players/mohammed-muntari/','Nome duplicado no dataset; jogador do Qatar, nascido em 1993-12-20, coincide com o histórico.'),
('189',6472301964130272941,32538,7040821579699442700,'approved','Breel Embolo','Switzerland','https://www.football.ch/it/asf/squadre-nazionali/nazionale-a/team.aspx/t-41641/p-720750/','Nome completo Breel Donald Embolo e nascimento 1997-02-14 confirmados pela federação suíça.'),
('242',6723403011038064313,21803033,NULL,'approved','Azzedine Ounahi','Morocco','https://www.pao.gr/en/player/79015/','Variação ortográfica; nascimento 2000-04-19 e seleção coincidem.'),
('339',1675805033419182651,37568657,NULL,'rejected','Roberto Fernández','Paraguay','https://www.skysports.com/football/player/104122/roberto-junior-fernandez','Candidato do inventário é outro Roberto Fernández; fonte é o goleiro paraguaio Roberto Júnior Fernández, nascido em 1988-03-29.'),
('418',2455945685111077308,31837,NULL,'approved','Antonio Rüdiger','Germany','https://datencenter.dfb.de/personen/antonio-ruediger/spieler','Variação de acento; nascimento 1993-03-03 coincide.'),
('507',920990500636313793,1244,7040025149329710625,'approved','Enner Valencia','Ecuador','https://www.tff.org/Default.aspx?kisiID=2292562&pageID=526','Nome abreviado; nome completo Enner Remberto Valencia e nascimento 1989-11-04 coincidem.'),
('56',8207977347615906586,322964,NULL,'approved','Min-jae Kim','South Korea','https://datencenter.dfb.de/datencenter/personen/min-jae-kim/spieler','Variação de hífen; nascimento 1996-11-15 e seleção coincidem.'),
('675',2981187261262565658,293951,NULL,'rejected','Mostafa Mohamed','Egypt','https://www.premierleague.com/en/players/431245/mostafa-mohamed/career','Candidato nasceu em 1997-11-28; a fonte é Zizo/Ahmed Mostafa Mohamed Sayed, nascido em 1996-01-10.'),
('71',5418044014027283417,9967153,NULL,'approved','Kang-in Lee','South Korea','https://en.psg.fr/teams/first-team/squad/lee-kang-in','Variação de hífen/ordem; nascimento 2001-02-19 e seleção coincidem.'),
('762',2070791314929473529,219922,NULL,'rejected','João Paulo','Cabo Verde','https://www.cbf.com.br/futebol-brasileiro/atletas/copa-do-brasil/masculino/2025/362877','Candidato é goleiro brasileiro nascido em 1995-06-29; fonte é meio-campista de Cabo Verde, nascido em 1998-05-26.'),
('816',1115966719291159676,219418,7040773337158347187,'approved','Giorgian de Arrascaeta','Uruguay','https://www.auf.org.uy/giorgian-de-arrascaeta/','Nome com nomes intermediários; nascimento 1994-06-01 e seleção coincidem.'),
('832',2609889149332001331,22126556,NULL,'approved','Rodrigo Zalazar','Uruguay','https://www.auf.org.uy/rodrigo-zalazar/','Erro de digitação Radrigo/Rodrigo; nascimento 1999-08-12 coincide.'),
('920',5792841629191237743,26823,NULL,'approved','Martin Ødegaard','Norway','https://www.fotball.no/landslag/kampprogram/a-lag-herrer/martin-odegaard/?fiksId=3100801&p=n','Variação de acento; nascimento 1998-12-17 coincide.'),
('985',5088083815965077314,407775,7040498750393165334,'rejected','Zinedine Zidane','Algeria','https://www.fff.fr/equipe-nationale/joueur/9447-zidane-luca/fiche.html','Candidato é Zinedine Zidane; fonte é Luca Zinedine Zidane, goleiro argelino nascido em 1998-05-13.');

UPDATE raw.wc_squads s SET player_id = d.canonical_wc_id
FROM manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NOT NULL
  AND s.edition_key='fifa_world_cup_mens__2026' AND s.source_player_id=d.source_external_id;
UPDATE raw.wc_goals s SET player_id = d.canonical_wc_id
FROM manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NOT NULL
  AND s.edition_key='fifa_world_cup_mens__2026' AND s.source_player_id=d.source_external_id;
UPDATE raw.wc_bookings s SET player_id = d.canonical_wc_id
FROM manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NOT NULL
  AND s.edition_key='fifa_world_cup_mens__2026' AND s.source_player_id=d.source_external_id;
UPDATE raw.wc_substitutions s SET player_id = d.canonical_wc_id
FROM manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NOT NULL
  AND s.edition_key='fifa_world_cup_mens__2026' AND s.source_player_id=d.source_external_id;

UPDATE raw.wc_player_identity_map m
SET sportmonks_player_id=d.candidate_sportmonks_id,
    match_confidence='confirmed',
    match_signals=jsonb_build_object('normalized_name',true,'manual_web_research',true,'source_url',d.evidence_url,'source_team',d.source_team),
    match_score=120, match_method='manual_web_research', audited_by='codex_web_research',
    audit_notes=d.decision_note, blocked_reason=NULL, updated_at=now()
FROM manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NOT NULL AND m.wc_player_id=d.canonical_wc_id;
UPDATE raw.wc_player_identity_map m
SET sportmonks_player_id=d.candidate_sportmonks_id,
    match_confidence='confirmed',
    match_signals=jsonb_build_object('normalized_name',true,'date_of_birth',true,'manual_web_research',true,'source_url',d.evidence_url,'source_team',d.source_team),
    match_score=120, match_method='manual_web_research', audited_by='codex_web_research',
    audit_notes=d.decision_note, blocked_reason=NULL, updated_at=now()
FROM manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NULL AND m.wc_player_id=d.source_wc_id;
DELETE FROM raw.wc_player_identity_map m USING manual_wc2026_players d
WHERE d.decision='approved' AND d.canonical_wc_id IS NOT NULL AND m.wc_player_id=d.source_wc_id;
UPDATE raw.wc_player_identity_map m
SET sportmonks_player_id=NULL, match_confidence='none',
    match_signals=jsonb_build_object('manual_web_research',true,'source_team',d.source_team,'candidate_sportmonks_id',d.candidate_sportmonks_id,'source_url',d.evidence_url),
    match_score=NULL, match_method='manual_web_research_rejected', audited_by='codex_web_research',
    audit_notes=d.decision_note, blocked_reason='manual_web_research_rejected_candidate', updated_at=now()
FROM manual_wc2026_players d
WHERE d.decision='rejected' AND m.wc_player_id=d.source_wc_id;

DELETE FROM control.wc_entity_match_review_queue
WHERE entity_type='player' AND edition_key='fifa_world_cup_mens__2026' AND source_name='mominullptr_wc2026';
INSERT INTO control.wc_entity_match_review_queue (
  entity_type, edition_key, source_name, source_external_id, candidate_internal_id,
  confidence_level, review_reason, candidate_payload, review_status,
  reviewer_name, reviewed_at, resolved_internal_id
)
SELECT 'player', 'fifa_world_cup_mens__2026', 'mominullptr_wc2026', source_external_id,
       candidate_sportmonks_id::text,
       CASE WHEN decision='approved' THEN 'high' ELSE 'medium' END,
       decision_note,
       jsonb_build_object('source_name',matched_name,'source_team',source_team,'candidate_sportmonks_id',candidate_sportmonks_id,'canonical_wc_id',canonical_wc_id,'evidence_url',evidence_url,'decision',decision),
       decision, 'codex_web_research', now(),
       CASE WHEN decision='approved' THEN candidate_sportmonks_id::text ELSE NULL END
FROM manual_wc2026_players;

ANALYZE raw.wc_player_identity_map;
ANALYZE raw.wc_squads;
ANALYZE raw.wc_goals;
ANALYZE raw.wc_bookings;
ANALYZE raw.wc_substitutions;
COMMIT;
