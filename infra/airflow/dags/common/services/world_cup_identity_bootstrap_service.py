from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import re
import secrets
import time
import uuid
from typing import Any

from airflow.operators.python import get_current_context
from sqlalchemy import create_engine, text

from common.observability import StepMetrics, log_event


WORLD_CUP_EDITION_KEY = "fifa_world_cup_mens__2022"
STATSBOMB_SOURCE = "statsbomb_open_data"
FJELSTUL_SOURCE = "fjelstul_worldcup"
WORLD_CUP_TEAM_TYPE = "national_team"

EXPECTED_COUNTS = {
    "statsbomb_matches": 64,
    "statsbomb_events_matches": 64,
    "statsbomb_lineups_matches": 64,
    "statsbomb_three_sixty_matches": 64,
    "fjelstul_matches": 64,
    "fjelstul_groups": 8,
    "fjelstul_group_standings": 32,
}

STATSBOMB_STAGE_KEY_MAP = {
    "Group Stage": "group_stage_1",
    "Round of 16": "round_of_16",
    "Quarter-finals": "quarter_final",
    "Semi-finals": "semi_final",
    "3rd Place Final": "third_place",
    "Final": "final",
}

FJELSTUL_STAGE_KEY_MAP = {
    "group stage": "group_stage_1",
    "round of 16": "round_of_16",
    "quarter-finals": "quarter_final",
    "semi-finals": "semi_final",
    "third-place match": "third_place",
    "final": "final",
}

GROUP_PATTERN = re.compile(r"Group\s+([A-Z])$", re.IGNORECASE)


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria ausente: {name}")
    return value


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid7() -> str:
    timestamp_ms = int(time.time_ns() // 1_000_000)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    uuid_int = (
        ((timestamp_ms & ((1 << 48) - 1)) << 80)
        | (0x7 << 76)
        | (rand_a << 64)
        | (0b10 << 62)
        | rand_b
    )
    return str(uuid.UUID(int=uuid_int))


def _team_internal_id(team_code: str) -> str:
    return f"team__{WORLD_CUP_TEAM_TYPE}__{team_code}"


def _stage_internal_id(stage_key: str) -> str:
    return f"stage__{WORLD_CUP_EDITION_KEY}__{stage_key}"


def _group_internal_id(stage_key: str, group_code: str) -> str:
    return f"group__{WORLD_CUP_EDITION_KEY}__{stage_key}__{group_code}"


def _match_internal_id() -> str:
    return f"match__wc__{_uuid7()}"


def _player_internal_id() -> str:
    return f"player__{_uuid7()}"


def _require_group_code(group_name: str) -> str:
    match = GROUP_PATTERN.match(group_name.strip())
    if not match:
        raise RuntimeError(f"Nome de grupo invalido para canonicalizacao: {group_name}")
    return match.group(1).upper()


def _fetch_active_snapshots(engine) -> dict[str, dict[str, Any]]:
    sql = text(
        """
        SELECT source_name, source_version, checksum_sha256, local_path
        FROM control.wc_source_snapshots
        WHERE edition_scope = :edition_key
          AND usage_decision = 'now'
          AND is_active = TRUE
          AND source_name IN (:statsbomb_source, :fjelstul_source)
        ORDER BY source_name
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(
            sql,
            {
                "edition_key": WORLD_CUP_EDITION_KEY,
                "statsbomb_source": STATSBOMB_SOURCE,
                "fjelstul_source": FJELSTUL_SOURCE,
            },
        ).mappings().all()

    snapshots = {row["source_name"]: dict(row) for row in rows}
    missing = [source for source in (STATSBOMB_SOURCE, FJELSTUL_SOURCE) if source not in snapshots]
    if missing:
        raise RuntimeError(f"Snapshots ativos ausentes para o Bloco 4: {missing}")
    return snapshots


def _validate_bronze_counts(conn) -> None:
    checks = {
        "statsbomb_matches": "SELECT count(*) FROM bronze.statsbomb_wc_matches WHERE edition_key = :edition_key",
        "statsbomb_events_matches": "SELECT count(DISTINCT match_id) FROM bronze.statsbomb_wc_events WHERE edition_key = :edition_key",
        "statsbomb_lineups_matches": "SELECT count(DISTINCT match_id) FROM bronze.statsbomb_wc_lineups WHERE edition_key = :edition_key",
        "statsbomb_three_sixty_matches": "SELECT count(DISTINCT match_id) FROM bronze.statsbomb_wc_three_sixty WHERE edition_key = :edition_key",
        "fjelstul_matches": "SELECT count(*) FROM bronze.fjelstul_wc_matches WHERE edition_key = :edition_key",
        "fjelstul_groups": "SELECT count(*) FROM bronze.fjelstul_wc_groups WHERE edition_key = :edition_key",
        "fjelstul_group_standings": "SELECT count(*) FROM bronze.fjelstul_wc_group_standings WHERE edition_key = :edition_key",
    }
    for name, sql in checks.items():
        actual = conn.execute(text(sql), {"edition_key": WORLD_CUP_EDITION_KEY}).scalar_one()
        expected = EXPECTED_COUNTS[name]
        if actual != expected:
            raise RuntimeError(f"Precondicao do bronze invalida para {name}: esperado={expected} atual={actual}")


def _validate_bronze_snapshot_versions(conn, snapshots: dict[str, dict[str, Any]]) -> None:
    version_checks = [
        ("bronze.statsbomb_wc_matches", STATSBOMB_SOURCE),
        ("bronze.statsbomb_wc_events", STATSBOMB_SOURCE),
        ("bronze.statsbomb_wc_lineups", STATSBOMB_SOURCE),
        ("bronze.statsbomb_wc_three_sixty", STATSBOMB_SOURCE),
        ("bronze.fjelstul_wc_matches", FJELSTUL_SOURCE),
        ("bronze.fjelstul_wc_groups", FJELSTUL_SOURCE),
        ("bronze.fjelstul_wc_group_standings", FJELSTUL_SOURCE),
        ("bronze.fjelstul_wc_manager_appointments", FJELSTUL_SOURCE),
    ]
    for table_name, source_name in version_checks:
        rows = conn.execute(
            text(f"SELECT DISTINCT source_version FROM {table_name} WHERE edition_key = :edition_key ORDER BY source_version"),
            {"edition_key": WORLD_CUP_EDITION_KEY},
        ).scalars().all()
        expected = [snapshots[source_name]["source_version"]]
        if rows != expected:
            raise RuntimeError(
                f"Versao do bronze divergente do snapshot ativo em {table_name}: bronze={rows} ativo={expected}"
            )


def _fetch_existing_map(conn) -> dict[tuple[str, str, str], str]:
    rows = conn.execute(
        text(
            """
            SELECT provider, entity_type, source_id, canonical_id
            FROM raw.provider_entity_map
            WHERE provider IN (:statsbomb_source, :fjelstul_source)
              AND entity_type IN ('team', 'match', 'stage', 'group', 'player')
            """
        ),
        {"statsbomb_source": STATSBOMB_SOURCE, "fjelstul_source": FJELSTUL_SOURCE},
    ).mappings().all()
    return {(row["provider"], row["entity_type"], row["source_id"]): row["canonical_id"] for row in rows}


def _fetch_team_rows(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            WITH sb AS (
              SELECT DISTINCT (payload->'home_team'->>'home_team_id')::text AS statsbomb_team_id, payload->'home_team'->>'home_team_name' AS team_name
              FROM bronze.statsbomb_wc_matches
              WHERE edition_key = :edition_key
              UNION
              SELECT DISTINCT (payload->'away_team'->>'away_team_id')::text AS statsbomb_team_id, payload->'away_team'->>'away_team_name' AS team_name
              FROM bronze.statsbomb_wc_matches
              WHERE edition_key = :edition_key
            ),
            fj AS (
              SELECT DISTINCT team_id AS fjelstul_team_id, team_name, team_code
              FROM bronze.fjelstul_wc_group_standings
              WHERE edition_key = :edition_key
            )
            SELECT sb.statsbomb_team_id, fj.fjelstul_team_id, sb.team_name AS statsbomb_team_name, fj.team_name AS fjelstul_team_name, fj.team_code
            FROM sb
            JOIN fj ON lower(sb.team_name) = lower(fj.team_name)
            ORDER BY fj.team_code
            """
        ),
        {"edition_key": WORLD_CUP_EDITION_KEY},
    ).mappings().all()
    if len(rows) != 32:
        raise RuntimeError(f"Team bootstrap exige 32 joins StatsBomb<->Fjelstul. Encontrei {len(rows)}.")
    return [dict(row) for row in rows]


def _fetch_stage_rows(conn) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    statsbomb_rows = conn.execute(
        text(
            """
            SELECT DISTINCT (payload->'competition_stage'->>'id')::text AS source_id, payload->'competition_stage'->>'name' AS stage_name
            FROM bronze.statsbomb_wc_matches
            WHERE edition_key = :edition_key
            ORDER BY source_id
            """
        ),
        {"edition_key": WORLD_CUP_EDITION_KEY},
    ).mappings().all()
    fjelstul_rows = conn.execute(
        text(
            """
            SELECT DISTINCT stage_name
            FROM bronze.fjelstul_wc_matches
            WHERE edition_key = :edition_key
            ORDER BY stage_name
            """
        ),
        {"edition_key": WORLD_CUP_EDITION_KEY},
    ).mappings().all()
    if len(statsbomb_rows) != 6 or len(fjelstul_rows) != 6:
        raise RuntimeError(
            f"Stage bootstrap invalido: statsbomb={len(statsbomb_rows)} fjelstul={len(fjelstul_rows)}"
        )
    return [dict(row) for row in statsbomb_rows], [dict(row) for row in fjelstul_rows]


def _fetch_group_rows(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT stage_name, group_name
            FROM bronze.fjelstul_wc_groups
            WHERE edition_key = :edition_key
            ORDER BY group_name
            """
        ),
        {"edition_key": WORLD_CUP_EDITION_KEY},
    ).mappings().all()
    if len(rows) != 8:
        raise RuntimeError(f"Group bootstrap exige 8 grupos. Encontrei {len(rows)}.")
    return [dict(row) for row in rows]


def _fetch_match_rows(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            WITH fj_team_map AS (
              SELECT DISTINCT team_id, team_name, team_code
              FROM bronze.fjelstul_wc_group_standings
              WHERE edition_key = :edition_key
            ),
            fj AS (
              SELECT
                m.match_id AS fjelstul_match_id,
                m.match_date,
                m.stage_name AS fjelstul_stage_name,
                m.group_name,
                m.home_team_id AS fjelstul_home_team_id,
                m.away_team_id AS fjelstul_away_team_id,
                h.team_name AS home_team_name,
                h.team_code AS home_team_code,
                a.team_name AS away_team_name,
                a.team_code AS away_team_code
              FROM bronze.fjelstul_wc_matches m
              JOIN fj_team_map h ON m.home_team_id = h.team_id
              JOIN fj_team_map a ON m.away_team_id = a.team_id
              WHERE m.edition_key = :edition_key
            ),
            sb AS (
              SELECT
                match_id::text AS statsbomb_match_id,
                payload->>'match_date' AS match_date,
                (payload->'home_team'->>'home_team_id')::text AS statsbomb_home_team_id,
                (payload->'away_team'->>'away_team_id')::text AS statsbomb_away_team_id,
                payload->'competition_stage'->>'name' AS statsbomb_stage_name,
                payload->'home_team'->>'home_team_name' AS home_team_name,
                payload->'away_team'->>'away_team_name' AS away_team_name
              FROM bronze.statsbomb_wc_matches
              WHERE edition_key = :edition_key
            )
            SELECT
              sb.statsbomb_match_id,
              sb.statsbomb_home_team_id,
              sb.statsbomb_away_team_id,
              sb.statsbomb_stage_name,
              fj.fjelstul_match_id,
              fj.fjelstul_home_team_id,
              fj.fjelstul_away_team_id,
              fj.fjelstul_stage_name,
              fj.group_name,
              fj.home_team_code,
              fj.away_team_code,
              sb.match_date
            FROM sb
            JOIN fj
              ON sb.match_date = fj.match_date
             AND lower(sb.home_team_name) = lower(fj.home_team_name)
             AND lower(sb.away_team_name) = lower(fj.away_team_name)
            ORDER BY sb.match_date, sb.statsbomb_match_id
            """
        ),
        {"edition_key": WORLD_CUP_EDITION_KEY},
    ).mappings().all()
    if len(rows) != 64:
        raise RuntimeError(f"Match bootstrap exige 64 joins StatsBomb<->Fjelstul. Encontrei {len(rows)}.")
    return [dict(row) for row in rows]


def _fetch_player_rows(conn) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            """
            SELECT DISTINCT
              (team->>'team_id')::text AS statsbomb_team_id,
              team->>'team_name' AS team_name,
              (player->>'player_id')::text AS player_id,
              player->>'player_name' AS player_name,
              player->>'player_nickname' AS player_nickname,
              player->>'jersey_number' AS jersey_number
            FROM bronze.statsbomb_wc_lineups l
            CROSS JOIN LATERAL jsonb_array_elements(l.payload) team
            CROSS JOIN LATERAL jsonb_array_elements(team->'lineup') player
            WHERE l.edition_key = :edition_key
            ORDER BY team_name, player_name, player_id
            """
        ),
        {"edition_key": WORLD_CUP_EDITION_KEY},
    ).mappings().all()
    if not rows:
        raise RuntimeError("Nenhum player encontrado em bronze.statsbomb_wc_lineups.")
    return [dict(row) for row in rows]


def _delete_obsolete_stage_rows(conn) -> None:
    conn.execute(
        text(
            """
            DELETE FROM raw.provider_entity_map
            WHERE provider = :provider
              AND entity_type = 'stage'
              AND edition_key = :edition_key
              AND source_id ~ '^[0-9]+$'
            """
        ),
        {"provider": STATSBOMB_SOURCE, "edition_key": WORLD_CUP_EDITION_KEY},
    )


def _upsert_provider_entity_map(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    conn.execute(
        text(
            """
            INSERT INTO raw.provider_entity_map (
              provider, entity_type, source_id, canonical_id, edition_key, source_version,
              mapping_confidence, resolution_method, needs_manual_review, review_reason,
              is_active, team_type, updated_at
            ) VALUES (
              :provider, :entity_type, :source_id, :canonical_id, :edition_key, :source_version,
              :mapping_confidence, :resolution_method, :needs_manual_review, :review_reason,
              :is_active, :team_type, :updated_at
            )
            ON CONFLICT (provider, entity_type, source_id)
            DO UPDATE SET
              canonical_id = EXCLUDED.canonical_id,
              edition_key = EXCLUDED.edition_key,
              source_version = EXCLUDED.source_version,
              mapping_confidence = EXCLUDED.mapping_confidence,
              resolution_method = EXCLUDED.resolution_method,
              needs_manual_review = EXCLUDED.needs_manual_review,
              review_reason = EXCLUDED.review_reason,
              is_active = EXCLUDED.is_active,
              team_type = EXCLUDED.team_type,
              updated_at = EXCLUDED.updated_at
            """
        ),
        rows,
    )


def _upsert_review_queue(conn, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    conn.execute(
        text(
            """
            INSERT INTO control.wc_entity_match_review_queue (
              entity_type, edition_key, source_name, source_external_id, candidate_internal_id,
              confidence_level, review_reason, candidate_payload, review_status
            ) VALUES (
              :entity_type, :edition_key, :source_name, :source_external_id, :candidate_internal_id,
              :confidence_level, :review_reason, CAST(:candidate_payload AS jsonb), :review_status
            )
            ON CONFLICT (
              entity_type, source_name, source_external_id, COALESCE(edition_key, 'GLOBAL')
            )
            DO UPDATE SET
              candidate_internal_id = EXCLUDED.candidate_internal_id,
              confidence_level = EXCLUDED.confidence_level,
              review_reason = EXCLUDED.review_reason,
              candidate_payload = EXCLUDED.candidate_payload,
              review_status = EXCLUDED.review_status
            """
        ),
        rows,
    )


def bootstrap_world_cup_2022_identity_map() -> dict[str, Any]:
    context = get_current_context()
    engine = create_engine(_get_required_env("FOOTBALL_PG_DSN"))
    now = _utc_now()

    with StepMetrics(
        service="airflow",
        module="world_cup_identity_bootstrap_service",
        step="bootstrap_world_cup_2022_identity_map",
        context=context,
        dataset="raw.provider_entity_map",
        table="raw.provider_entity_map",
    ):
        snapshots = _fetch_active_snapshots(engine)
        with engine.begin() as conn:
            _validate_bronze_counts(conn)
            _validate_bronze_snapshot_versions(conn, snapshots)
            _delete_obsolete_stage_rows(conn)
            existing_map = _fetch_existing_map(conn)
            team_rows = _fetch_team_rows(conn)
            statsbomb_stage_rows, fjelstul_stage_rows = _fetch_stage_rows(conn)
            group_rows = _fetch_group_rows(conn)
            match_rows = _fetch_match_rows(conn)
            player_rows = _fetch_player_rows(conn)

            provider_rows: list[dict[str, Any]] = []
            review_rows: list[dict[str, Any]] = []
            team_internal_by_statsbomb_id: dict[str, str] = {}
            player_confidence_counts = {"exact": 0, "high": 0, "medium": 0, "low": 0}

            for row in team_rows:
                canonical_id = _team_internal_id(row["team_code"])
                team_internal_by_statsbomb_id[row["statsbomb_team_id"]] = canonical_id
                provider_rows.extend(
                    [
                        {
                            "provider": STATSBOMB_SOURCE,
                            "entity_type": "team",
                            "source_id": row["statsbomb_team_id"],
                            "canonical_id": canonical_id,
                            "edition_key": None,
                            "source_version": snapshots[STATSBOMB_SOURCE]["source_version"],
                            "mapping_confidence": "high",
                            "resolution_method": "team_name_to_team_code_match",
                            "needs_manual_review": False,
                            "review_reason": None,
                            "is_active": True,
                            "team_type": WORLD_CUP_TEAM_TYPE,
                            "updated_at": now,
                        },
                        {
                            "provider": FJELSTUL_SOURCE,
                            "entity_type": "team",
                            "source_id": row["fjelstul_team_id"],
                            "canonical_id": canonical_id,
                            "edition_key": None,
                            "source_version": snapshots[FJELSTUL_SOURCE]["source_version"],
                            "mapping_confidence": "high",
                            "resolution_method": "team_name_to_team_code_match",
                            "needs_manual_review": False,
                            "review_reason": None,
                            "is_active": True,
                            "team_type": WORLD_CUP_TEAM_TYPE,
                            "updated_at": now,
                        },
                    ]
                )

            for row in statsbomb_stage_rows:
                stage_key = STATSBOMB_STAGE_KEY_MAP.get(row["stage_name"])
                if stage_key is None:
                    raise RuntimeError(f"Stage StatsBomb sem mapeamento canonico: {row['stage_name']}")
                provider_rows.append(
                    {
                        "provider": STATSBOMB_SOURCE,
                        "entity_type": "stage",
                        "source_id": f"{WORLD_CUP_EDITION_KEY}::stage::{row['source_id']}",
                        "canonical_id": _stage_internal_id(stage_key),
                        "edition_key": WORLD_CUP_EDITION_KEY,
                        "source_version": snapshots[STATSBOMB_SOURCE]["source_version"],
                        "mapping_confidence": "high",
                        "resolution_method": "canonical_stage_mapping",
                        "needs_manual_review": False,
                        "review_reason": None,
                        "is_active": True,
                        "team_type": None,
                        "updated_at": now,
                    }
                )

            for row in fjelstul_stage_rows:
                stage_key = FJELSTUL_STAGE_KEY_MAP.get(row["stage_name"])
                if stage_key is None:
                    raise RuntimeError(f"Stage Fjelstul sem mapeamento canonico: {row['stage_name']}")
                provider_rows.append(
                    {
                        "provider": FJELSTUL_SOURCE,
                        "entity_type": "stage",
                        "source_id": f"WC-2022::stage::{row['stage_name']}",
                        "canonical_id": _stage_internal_id(stage_key),
                        "edition_key": WORLD_CUP_EDITION_KEY,
                        "source_version": snapshots[FJELSTUL_SOURCE]["source_version"],
                        "mapping_confidence": "high",
                        "resolution_method": "canonical_stage_mapping",
                        "needs_manual_review": False,
                        "review_reason": None,
                        "is_active": True,
                        "team_type": None,
                        "updated_at": now,
                    }
                )

            for row in group_rows:
                stage_key = FJELSTUL_STAGE_KEY_MAP.get(row["stage_name"])
                if stage_key is None:
                    raise RuntimeError(f"Group com stage sem mapeamento canonico: {row['stage_name']}")
                provider_rows.append(
                    {
                        "provider": FJELSTUL_SOURCE,
                        "entity_type": "group",
                        "source_id": f"WC-2022::group::{row['stage_name']}::{row['group_name']}",
                        "canonical_id": _group_internal_id(stage_key, _require_group_code(row["group_name"])),
                        "edition_key": WORLD_CUP_EDITION_KEY,
                        "source_version": snapshots[FJELSTUL_SOURCE]["source_version"],
                        "mapping_confidence": "high",
                        "resolution_method": "canonical_group_mapping",
                        "needs_manual_review": False,
                        "review_reason": None,
                        "is_active": True,
                        "team_type": None,
                        "updated_at": now,
                    }
                )

            match_cluster_ids: dict[tuple[str, str, str], str] = {}
            for row in match_rows:
                match_key = (row["match_date"], row["home_team_code"], row["away_team_code"])
                canonical_id = (
                    match_cluster_ids.get(match_key)
                    or existing_map.get((STATSBOMB_SOURCE, "match", row["statsbomb_match_id"]))
                    or existing_map.get((FJELSTUL_SOURCE, "match", row["fjelstul_match_id"]))
                    or _match_internal_id()
                )
                match_cluster_ids[match_key] = canonical_id
                provider_rows.extend(
                    [
                        {
                            "provider": STATSBOMB_SOURCE,
                            "entity_type": "match",
                            "source_id": row["statsbomb_match_id"],
                            "canonical_id": canonical_id,
                            "edition_key": WORLD_CUP_EDITION_KEY,
                            "source_version": snapshots[STATSBOMB_SOURCE]["source_version"],
                            "mapping_confidence": "high",
                            "resolution_method": "date_teams_match",
                            "needs_manual_review": False,
                            "review_reason": None,
                            "is_active": True,
                            "team_type": None,
                            "updated_at": now,
                        },
                        {
                            "provider": FJELSTUL_SOURCE,
                            "entity_type": "match",
                            "source_id": row["fjelstul_match_id"],
                            "canonical_id": canonical_id,
                            "edition_key": WORLD_CUP_EDITION_KEY,
                            "source_version": snapshots[FJELSTUL_SOURCE]["source_version"],
                            "mapping_confidence": "high",
                            "resolution_method": "date_teams_match",
                            "needs_manual_review": False,
                            "review_reason": None,
                            "is_active": True,
                            "team_type": None,
                            "updated_at": now,
                        },
                    ]
                )

            for row in player_rows:
                if not row["player_id"]:
                    confidence = "low"
                    review_reason = "statsbomb_player_id_missing"
                elif row["statsbomb_team_id"] not in team_internal_by_statsbomb_id:
                    confidence = "medium"
                    review_reason = "statsbomb_team_not_mapped"
                else:
                    confidence = "exact"
                    review_reason = None
                player_confidence_counts[confidence] += 1

                if confidence in {"exact", "high"}:
                    canonical_id = existing_map.get((STATSBOMB_SOURCE, "player", row["player_id"])) or _player_internal_id()
                    provider_rows.append(
                        {
                            "provider": STATSBOMB_SOURCE,
                            "entity_type": "player",
                            "source_id": row["player_id"],
                            "canonical_id": canonical_id,
                            "edition_key": WORLD_CUP_EDITION_KEY,
                            "source_version": snapshots[STATSBOMB_SOURCE]["source_version"],
                            "mapping_confidence": confidence,
                            "resolution_method": "source_id_exact",
                            "needs_manual_review": False,
                            "review_reason": None,
                            "is_active": True,
                            "team_type": None,
                            "updated_at": now,
                        }
                    )
                else:
                    review_rows.append(
                        {
                            "entity_type": "player",
                            "edition_key": WORLD_CUP_EDITION_KEY,
                            "source_name": STATSBOMB_SOURCE,
                            "source_external_id": row["player_id"] or f"missing::{row['team_name']}::{row['player_name']}",
                            "candidate_internal_id": None,
                            "confidence_level": confidence,
                            "review_reason": review_reason,
                            "candidate_payload": json.dumps(
                                {
                                    "team_name": row["team_name"],
                                    "statsbomb_team_id": row["statsbomb_team_id"],
                                    "player_name": row["player_name"],
                                    "player_nickname": row["player_nickname"],
                                    "jersey_number": row["jersey_number"],
                                },
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            "review_status": "pending",
                        }
                    )

            if any(row["entity_type"] == "player" and row["mapping_confidence"] == "low" for row in provider_rows):
                raise RuntimeError("Existem players low auto-homologados, o que viola o contrato do Bloco 4.")

            _upsert_provider_entity_map(conn, provider_rows)
            _upsert_review_queue(conn, review_rows)

            summary = {
                "team_rows_upserted": sum(1 for row in provider_rows if row["entity_type"] == "team"),
                "match_rows_upserted": sum(1 for row in provider_rows if row["entity_type"] == "match"),
                "stage_rows_upserted": sum(1 for row in provider_rows if row["entity_type"] == "stage"),
                "group_rows_upserted": sum(1 for row in provider_rows if row["entity_type"] == "group"),
                "player_rows_homologated": sum(1 for row in provider_rows if row["entity_type"] == "player"),
                "player_review_rows_upserted": len(review_rows),
                "player_exact": player_confidence_counts["exact"],
                "player_high": player_confidence_counts["high"],
                "player_medium": player_confidence_counts["medium"],
                "player_low": player_confidence_counts["low"],
                "distinct_matches": len(match_cluster_ids),
            }

    log_event(
        service="airflow",
        module="world_cup_identity_bootstrap_service",
        step="summary",
        status="success",
        context=context,
        dataset="raw.provider_entity_map",
        row_count=(
            summary["team_rows_upserted"]
            + summary["match_rows_upserted"]
            + summary["stage_rows_upserted"]
            + summary["group_rows_upserted"]
            + summary["player_rows_homologated"]
            + summary["player_review_rows_upserted"]
        ),
        message=(
            "Bootstrap World Cup 2022 concluido | "
            f"team_rows={summary['team_rows_upserted']} | "
            f"match_rows={summary['match_rows_upserted']} | "
            f"stage_rows={summary['stage_rows_upserted']} | "
            f"group_rows={summary['group_rows_upserted']} | "
            f"player_homologated={summary['player_rows_homologated']} | "
            f"player_review_rows={summary['player_review_rows_upserted']} | "
            f"player_exact={summary['player_exact']} | "
            f"player_medium={summary['player_medium']} | "
            f"player_low={summary['player_low']}"
        ),
    )
    return summary
