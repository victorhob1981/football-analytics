from __future__ import annotations

import logging
from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response, build_coverage_from_counts, build_pagination
from ..core.errors import AppError
from ..core.filters import VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/insights", tags=["insights"])

EntityType = Literal["player", "team", "match", "competition", "global"]
Severity = Literal["info", "warning", "critical"]
SortBy = Literal["severity", "referencePeriod"]
SortDirection = Literal["asc", "desc"]

SEVERITY_PRIORITY = {"critical": 0, "warning": 1, "info": 2}


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _match_filters_sql(filters: Any) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(clauses, params, alias="fm", filters=filters)
    return " AND ".join(clauses), params


def _compute_biggest_win(where_sql: str, where_params: list[Any]) -> dict[str, Any] | None:
    """Maior goleada da temporada."""
    query = f"""
        SELECT
            fm.match_id,
            ht.team_name AS home_team,
            at.team_name AS away_team,
            fm.home_goals,
            fm.away_goals,
            fm.total_goals,
            ABS(fm.home_goals - fm.away_goals) AS goal_diff,
            fm.date_day,
            fm.round_number
        FROM mart.fact_matches fm
        JOIN mart.dim_team ht ON ht.team_sk = fm.home_team_sk
        JOIN mart.dim_team at ON at.team_sk = fm.away_team_sk
        WHERE {where_sql}
          AND fm.home_goals IS NOT NULL
          AND fm.away_goals IS NOT NULL
          AND fm.home_goals != fm.away_goals
        ORDER BY ABS(fm.home_goals - fm.away_goals) DESC, fm.total_goals DESC
        LIMIT 1;
    """
    row = db_client.fetch_one(query, where_params)
    if not row:
        return None

    winner = row["home_team"] if row["home_goals"] > row["away_goals"] else row["away_team"]
    score = f"{row['home_goals']}-{row['away_goals']}"
    round_label = f"Rodada {row['round_number']}" if row.get("round_number") else ""

    return {
        "insight_id": "biggest_win",
        "severity": "info",
        "explanation": (
            f"Maior goleada: {row['home_team']} {score} {row['away_team']}. "
            f"{winner} aplicou uma goleada com {row['goal_diff']} gols de diferença"
            f"{f' na {round_label}' if round_label else ''}."
        ),
        "evidences": {
            "goal_difference": int(row["goal_diff"]),
            "total_goals": int(row["total_goals"]),
            "home_goals": int(row["home_goals"]),
            "away_goals": int(row["away_goals"]),
        },
        "reference_period": round_label or "Temporada",
        "data_source": ["fact_matches"],
    }


def _compute_most_exciting_match(where_sql: str, where_params: list[Any]) -> dict[str, Any] | None:
    """Jogo mais emocionante — mais gols, chutes, posse equilibrada."""
    query = f"""
        SELECT
            fm.match_id,
            ht.team_name AS home_team,
            at.team_name AS away_team,
            fm.home_goals,
            fm.away_goals,
            fm.total_goals,
            COALESCE(fm.home_shots, 0) + COALESCE(fm.away_shots, 0) AS total_shots,
            COALESCE(fm.home_shots_on_target, 0) + COALESCE(fm.away_shots_on_target, 0) AS total_shots_on_target,
            fm.home_possession,
            fm.away_possession,
            fm.date_day,
            fm.round_number,
            (
                COALESCE(fm.total_goals, 0) * 3
                + (COALESCE(fm.home_shots, 0) + COALESCE(fm.away_shots, 0))
                + (COALESCE(fm.home_shots_on_target, 0) + COALESCE(fm.away_shots_on_target, 0)) * 2
                + CASE
                    WHEN fm.home_possession IS NOT NULL
                         AND ABS(fm.home_possession - 50) < 10
                    THEN 5
                    ELSE 0
                  END
            ) AS excitement_score
        FROM mart.fact_matches fm
        JOIN mart.dim_team ht ON ht.team_sk = fm.home_team_sk
        JOIN mart.dim_team at ON at.team_sk = fm.away_team_sk
        WHERE {where_sql}
          AND fm.home_goals IS NOT NULL
          AND fm.away_goals IS NOT NULL
        ORDER BY excitement_score DESC, fm.total_goals DESC
        LIMIT 1;
    """
    row = db_client.fetch_one(query, where_params)
    if not row:
        return None

    score = f"{row['home_goals']}-{row['away_goals']}"
    total_shots = int(row["total_shots"])
    round_label = f"Rodada {row['round_number']}" if row.get("round_number") else ""
    poss_note = ""
    if row.get("home_possession") is not None:
        poss_note = f", posse {row['home_possession']}%-{row['away_possession']}%"

    return {
        "insight_id": "most_exciting_match",
        "severity": "info",
        "explanation": (
            f"Jogo mais emocionante: {row['home_team']} {score} {row['away_team']}. "
            f"{row['total_goals']} gols, {total_shots} finalizações{poss_note}"
            f"{f' — {round_label}' if round_label else ''}."
        ),
        "evidences": {
            "total_goals": int(row["total_goals"]),
            "total_shots": total_shots,
            "shots_on_target": int(row["total_shots_on_target"]),
            "excitement_score": int(row["excitement_score"]),
        },
        "reference_period": round_label or "Temporada",
        "data_source": ["fact_matches"],
    }


def _compute_standout_player(where_sql: str, where_params: list[Any]) -> dict[str, Any] | None:
    """Jogador destaque — melhor rating médio com mínimo de 3 jogos."""
    query = f"""
        SELECT
            pms.player_name,
            pms.team_name,
            ROUND(AVG(pms.rating)::numeric, 2) AS avg_rating,
            COUNT(*) AS matches_played,
            SUM(COALESCE(pms.goals, 0)) AS total_goals,
            SUM(COALESCE(pms.assists, 0)) AS total_assists,
            SUM(COALESCE(pms.minutes_played, 0)) AS total_minutes
        FROM mart.player_match_summary pms
        INNER JOIN mart.fact_matches fm ON fm.match_id = pms.match_id
        WHERE {where_sql}
          AND pms.rating IS NOT NULL
        GROUP BY pms.player_id, pms.player_name, pms.team_name
        HAVING COUNT(*) >= 3
        ORDER BY AVG(pms.rating) DESC
        LIMIT 1;
    """
    row = db_client.fetch_one(query, where_params)
    if not row:
        return None

    goals_assists = f"{row['total_goals']}G / {row['total_assists']}A"

    return {
        "insight_id": "standout_player",
        "severity": "warning",
        "explanation": (
            f"Jogador destaque: {row['player_name']} ({row['team_name']}). "
            f"Rating médio de {row['avg_rating']} em {row['matches_played']} jogos, "
            f"com {goals_assists}."
        ),
        "evidences": {
            "avg_rating": float(row["avg_rating"]),
            "matches_played": int(row["matches_played"]),
            "total_goals": int(row["total_goals"]),
            "total_assists": int(row["total_assists"]),
        },
        "reference_period": "Temporada",
        "data_source": ["player_match_summary"],
    }


def _compute_most_efficient_team(where_sql: str, where_params: list[Any]) -> dict[str, Any] | None:
    """Time mais eficiente — melhor razão gols/chutes."""
    query = f"""
        WITH team_stats AS (
            SELECT
                t.team_name,
                SUM(CASE WHEN fm.home_team_sk = t.team_sk THEN fm.home_goals ELSE fm.away_goals END) AS goals,
                SUM(CASE WHEN fm.home_team_sk = t.team_sk THEN fm.home_shots ELSE fm.away_shots END) AS shots,
                SUM(CASE WHEN fm.home_team_sk = t.team_sk
                         THEN fm.home_shots_on_target
                         ELSE fm.away_shots_on_target END) AS shots_on_target,
                COUNT(*) AS matches_played
            FROM mart.fact_matches fm
            JOIN mart.dim_team t ON t.team_sk IN (fm.home_team_sk, fm.away_team_sk)
            WHERE {where_sql}
              AND fm.home_goals IS NOT NULL
              AND fm.home_shots IS NOT NULL
            GROUP BY t.team_sk, t.team_name
            HAVING COUNT(*) >= 3
        )
        SELECT
            team_name,
            goals,
            shots,
            shots_on_target,
            matches_played,
            ROUND((goals::numeric / NULLIF(shots, 0)) * 100, 1) AS efficiency_pct
        FROM team_stats
        WHERE shots > 0
        ORDER BY (goals::numeric / NULLIF(shots, 0)) DESC
        LIMIT 1;
    """
    row = db_client.fetch_one(query, where_params)
    if not row:
        return None

    return {
        "insight_id": "most_efficient_team",
        "severity": "info",
        "explanation": (
            f"Time mais eficiente: {row['team_name']}. "
            f"Converte {row['efficiency_pct']}% das finalizações em gol "
            f"({row['goals']} gols em {row['shots']} chutes, {row['matches_played']} jogos)."
        ),
        "evidences": {
            "efficiency_pct": float(row["efficiency_pct"]),
            "goals": int(row["goals"]),
            "shots": int(row["shots"]),
            "matches_played": int(row["matches_played"]),
        },
        "reference_period": "Temporada",
        "data_source": ["fact_matches"],
    }


def _compute_most_active_round(where_sql: str, where_params: list[Any]) -> dict[str, Any] | None:
    """Rodada mais movimentada — mais gols totais."""
    query = f"""
        SELECT
            fm.round_number,
            SUM(fm.total_goals) AS round_total_goals,
            COUNT(*) AS matches_in_round,
            ROUND(AVG(fm.total_goals)::numeric, 1) AS avg_goals_per_match,
            SUM(COALESCE(fm.home_shots, 0) + COALESCE(fm.away_shots, 0)) AS round_total_shots
        FROM mart.fact_matches fm
        WHERE {where_sql}
          AND fm.round_number IS NOT NULL
          AND fm.total_goals IS NOT NULL
        GROUP BY fm.round_number
        HAVING COUNT(*) >= 2
        ORDER BY SUM(fm.total_goals) DESC, AVG(fm.total_goals) DESC
        LIMIT 1;
    """
    row = db_client.fetch_one(query, where_params)
    if not row:
        return None

    return {
        "insight_id": "most_active_round",
        "severity": "info",
        "explanation": (
            f"Rodada mais movimentada: Rodada {row['round_number']}. "
            f"{row['round_total_goals']} gols em {row['matches_in_round']} jogos "
            f"(média de {row['avg_goals_per_match']} por partida)."
        ),
        "evidences": {
            "round_number": int(row["round_number"]),
            "total_goals": int(row["round_total_goals"]),
            "matches_in_round": int(row["matches_in_round"]),
            "avg_goals_per_match": float(row["avg_goals_per_match"]),
        },
        "reference_period": f"Rodada {row['round_number']}",
        "data_source": ["fact_matches"],
    }


def _collect_insights(where_sql: str, where_params: list[Any]) -> list[dict[str, Any]]:
    """Run all insight generators and collect non-None results."""
    generators = [
        _compute_biggest_win,
        _compute_most_exciting_match,
        _compute_standout_player,
        _compute_most_efficient_team,
        _compute_most_active_round,
    ]

    insights: list[dict[str, Any]] = []
    for generator in generators:
        try:
            result = generator(where_sql, list(where_params))
            if result is not None:
                insights.append(result)
        except Exception:
            logger.exception("Failed to compute insight %s", generator.__name__)
            continue

    insights.sort(key=lambda x: SEVERITY_PRIORITY.get(x.get("severity", "info"), 99))
    return insights


@router.get("")
def get_insights(
    request: Request,
    entityType: EntityType = "global",
    entityId: str | None = None,
    competitionId: str | None = None,
    seasonId: str | None = None,
    roundId: str | None = None,
    venue: VenueFilter = VenueFilter.all,
    lastN: int | None = Query(default=None, gt=0),
    dateStart: date | None = None,
    dateEnd: date | None = None,
    dateRangeStart: date | None = None,
    dateRangeEnd: date | None = None,
    month: str | None = None,
    severity: Severity | None = None,
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=20, ge=1, le=100),
    sortBy: SortBy = "severity",
    sortDirection: SortDirection = "desc",
) -> dict[str, Any]:
    global_filters = validate_and_build_global_filters(
        competition_id=competitionId,
        season_id=seasonId,
        round_id=roundId,
        venue=venue,
        last_n=lastN,
        date_start=dateStart,
        date_end=dateEnd,
        date_range_start=dateRangeStart,
        date_range_end=dateRangeEnd,
        month=month,
    )

    if entityType != "global" and (entityId is None or entityId.strip() == ""):
        raise AppError(
            message="Invalid insight context. 'entityId' is required when entityType is not 'global'.",
            code="INVALID_INSIGHT_CONTEXT",
            status=400,
            details={"entityType": entityType},
        )

    where_sql, where_params = _match_filters_sql(global_filters)
    insights = _collect_insights(where_sql, where_params)

    if severity is not None:
        insights = [i for i in insights if i["severity"] == severity]

    total_count = len(insights)
    start = (page - 1) * pageSize
    end = start + pageSize
    page_insights = insights[start:end]

    pagination = build_pagination(page, pageSize, total_count)
    coverage = build_coverage_from_counts(
        available_count=total_count,
        total_count=5,
        label="Insights computados da temporada",
    )

    return build_api_response(
        page_insights,
        request_id=_request_id(request),
        pagination=pagination,
        coverage=coverage,
    )
