from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
import re
from typing import Any

from fastapi import APIRouter, Request

from ..core.context_registry import (
    build_canonical_context,
    get_canonical_competition,
    get_canonical_competition_by_key,
    select_default_context,
)
from ..core.contracts import build_api_response, build_coverage_from_counts, build_pagination
from ..core.errors import AppError
from ..db.client import db_client


router = APIRouter(tags=["serving-v2"])

_SPLIT_SEASON = re.compile(r"^(\d{4})_(\d{2}|\d{4})$")


def _q(request: Request, name: str, default: str | None = None) -> str | None:
    value = request.query_params.get(name)
    if value is None:
        return default
    value = value.strip()
    return value or default


def _int_q(request: Request, name: str, default: int | None = None) -> int | None:
    value = _q(request, name)
    if value is None or value.lower() == "all":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise AppError(
            message=f"Invalid value for '{name}'. Expected integer.",
            code="INVALID_QUERY_PARAM",
            status=400,
            details={name: value},
        ) from exc


def _bool_q(request: Request, name: str, default: bool = False) -> bool:
    value = _q(request, name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _public_season_label(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    match = _SPLIT_SEASON.fullmatch(normalized)
    if match:
        end = match.group(2)
        end_year = end if len(end) == 4 else f"{match.group(1)[:2]}{end}"
        return f"{match.group(1)}/{end_year}"
    return normalized or None


def _internal_season_candidates(value: str | None) -> tuple[str, ...]:
    if value is None:
        return tuple()
    normalized = value.strip()
    if not normalized:
        return tuple()
    candidates = {normalized, normalized.replace("/", "_")}
    if normalized.isdigit() and len(normalized) == 4:
        candidates.add(f"{normalized}_{str(int(normalized) + 1)[-2:]}")
        candidates.add(f"{normalized}/{int(normalized) + 1}")
    return tuple(candidates)


def _competition_key(request: Request) -> str | None:
    key = _q(request, "competitionKey")
    if key:
        return "serie_a_it" if key == "serie_a_italy" else key
    raw_competition_id = _q(request, "competitionId")
    if raw_competition_id and not raw_competition_id.isdigit():
        return raw_competition_id
    competition_id = _int_q(request, "competitionId")
    canonical = get_canonical_competition(competition_id)
    return canonical.competition_key if canonical else None


def _canonical_context(competition_key: str | None, season_label: Any) -> dict[str, str] | None:
    if competition_key is None or season_label is None:
        return None
    canonical = get_canonical_competition_by_key(
        "serie_a_it" if competition_key == "serie_a_italy" else competition_key
    )
    if canonical is None:
        return None
    public_season = _public_season_label(season_label)
    if public_season is None:
        return None
    return build_canonical_context(
        competition_id=canonical.competition_id,
        competition_name=canonical.default_name,
        season_id=public_season,
    )


def _edition_predicate(alias: str, request: Request, params: list[Any]) -> list[str]:
    clauses: list[str] = []
    competition_key = _competition_key(request)
    if competition_key:
        clauses.append(f"{alias}.competition_key = %s")
        params.append(competition_key)
    season = _q(request, "seasonLabel") or _q(request, "seasonId")
    candidates = _internal_season_candidates(season)
    if candidates:
        clauses.append(f"{alias}.edition_key in (select e.edition_key from mart_v2.dim_edition e where e.season_label = any(%s))")
        params.append(list(candidates))
    stage_id = _int_q(request, "stageId")
    if stage_id is not None:
        clauses.append(
            f"exists (select 1 from mart_v2.fact_match fm_scope join mart_v2.dim_stage ds on ds.stage_key = fm_scope.stage_key where fm_scope.match_id = {alias}.match_id and ds.stage_id = %s)"
        )
        params.append(stage_id)
    round_id = _int_q(request, "roundId")
    if round_id is not None:
        clauses.append(
            f"exists (select 1 from mart_v2.fact_match fm_scope join mart_v2.dim_round dr on dr.round_key = fm_scope.round_key where fm_scope.match_id = {alias}.match_id and dr.round_id = %s)"
        )
        params.append(round_id)
    team_id = _int_q(request, "teamId")
    if team_id is not None:
        clauses.append(f"({alias}.home_team_id = %s or {alias}.away_team_id = %s)")
        params.extend([team_id, team_id])
    date_start = _q(request, "dateStart") or _q(request, "dateRangeStart")
    date_end = _q(request, "dateEnd") or _q(request, "dateRangeEnd")
    if date_start:
        clauses.append(f"{alias}.match_date >= %s")
        params.append(date_start)
    if date_end:
        clauses.append(f"{alias}.match_date <= %s")
        params.append(date_end)
    return clauses


def _response(
    data: Any,
    request: Request,
    *,
    page: int | None = None,
    page_size: int | None = None,
    total: int | None = None,
    coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pagination = None
    if page is not None and page_size is not None and total is not None:
        pagination = build_pagination(page, page_size, total)
    return build_api_response(
        data,
        request_id=getattr(request.state, "request_id", None),
        pagination=pagination,
        coverage=coverage,
    )


def _team_name(team_id: Any, names: dict[int, str]) -> str | None:
    return names.get(int(team_id)) if team_id is not None else None


def _team_names(rows: list[dict[str, Any]]) -> dict[int, str]:
    return {
        int(row["team_id"]): str(row["team_name"])
        for row in rows
        if row.get("team_id") is not None and row.get("team_name")
    }


@router.get("/api/v1/home")
def get_home(request: Request) -> dict[str, Any]:
    rows = db_client.fetch_all(
        """
        select competition_key, competition_name, country_name, confederation_name,
               competition_type, is_international, is_world_cup, edition_count,
               selectable_edition_count, published_match_count, first_match_date,
               last_match_date, href, metadata
        from serving_v2.competition_catalog
        where is_selectable
        order by competition_name;
        """
    )
    highlights = db_client.fetch_all(
        """
        select player_key, display_name as player_name, match_count,
               first_match_date, last_match_date
        from serving_v2.player_profile
        where match_count > 0
        order by match_count desc, display_name asc nulls last, player_key
        limit 2;
        """
    )
    edition_rows = db_client.fetch_all(
        """
        select competition_key, season_label
        from serving_v2.edition_catalog
        where is_selectable
        order by season_start_date desc nulls last, season_label desc;
        """
    )
    seasons_by_competition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edition in edition_rows:
        seasons_by_competition[str(edition["competition_key"])].append(edition)
    competitions = []
    for row in rows:
        key = str(row["competition_key"])
        public_id = str(get_canonical_competition_by_key(key).competition_id) if get_canonical_competition_by_key(key) else key
        seasons = seasons_by_competition.get(key, [])
        latest = seasons[0] if seasons else None
        competitions.append(
            {
                "competitionId": public_id,
                "competitionKey": key,
                "competitionName": row["competition_name"],
                "assetId": "wc_mens" if row.get("is_world_cup") else public_id,
                "source": "published",
                "dominantSource": "published",
                "additionalSources": [],
                "country": row.get("country_name"),
                "region": row.get("confederation_name"),
                "scope": "global" if row.get("is_international") else "domestic",
                "type": row.get("competition_type"),
                "matchesCount": int(row.get("published_match_count") or 0),
                "seasonsCount": int(row.get("selectable_edition_count") or 0),
                "range": {
                    "fromSeasonId": _public_season_label(seasons[-1]["season_label"]) if seasons else None,
                    "fromSeasonLabel": _public_season_label(seasons[-1]["season_label"]) if seasons else None,
                    "toSeasonId": _public_season_label(seasons[0]["season_label"]) if seasons else None,
                    "toSeasonLabel": _public_season_label(seasons[0]["season_label"]) if seasons else None,
                },
                "latestContext": _canonical_context(key, latest["season_label"]) if latest else None,
                "coverage": {
                    "status": "complete" if row.get("published_match_count") else "empty",
                    "percentage": 100 if row.get("published_match_count") else 0,
                    "label": "Published match coverage",
                },
            }
        )
    archive = db_client.fetch_one(
        """
        select
          (select count(*) from serving_v2.competition_catalog where is_selectable)::int as competitions,
          (select count(*) from serving_v2.edition_catalog where is_selectable)::int as seasons,
          (select count(*) from serving_v2.match_catalog)::int as matches,
          (select count(*) from serving_v2.player_profile where match_count > 0)::int as players,
          (select count(*) from serving_v2.team_profile where match_count > 0 and team_type = 'club')::int as clubs;
        """
    ) or {}
    editorial = [
        {
            "id": f"highlight-{index}",
            "eyebrow": "Curadoria de dados reais",
            "competitionLabel": "Acervo publicado",
            "title": f"{row.get('player_name')}: destaque do acervo",
            "description": f"{int(row.get('match_count') or 0)} partidas disponíveis no acervo publicado.",
            "playerId": row.get("player_key"),
            "playerName": row.get("player_name"),
            "teamId": None,
            "teamName": None,
            "imageAssetId": row.get("player_key"),
            "context": None,
            "metrics": {
                "matchesPlayed": int(row.get("match_count") or 0),
                "goals": None,
                "assists": None,
                "rating": None,
            },
        }
        for index, row in enumerate(highlights, 1)
    ]
    archive.update({"competitions": len(competitions), "seasons": sum(int(item["seasonsCount"]) for item in competitions), "matches": sum(int(item["matchesCount"]) for item in competitions)})
    return _response(
        {"archiveSummary": archive, "competitions": competitions, "editorialHighlights": editorial},
        request,
        coverage=build_coverage_from_counts(sum(bool(value) for value in (competitions, archive.get("matches"), editorial)), 3, "Home coverage"),
    )


@router.get("/api/v1/competition-editions")
def get_competition_editions(request: Request) -> dict[str, Any]:
    key = _competition_key(request)
    if key is None:
        raise AppError("'competitionKey' is required.", "INVALID_QUERY_PARAM", 400, {"missing": ["competitionKey"]})
    row = db_client.fetch_one(
        "select competition_key, competition_name from serving_v2.competition_catalog where competition_key = %s;",
        [key],
    )
    if row is None:
        raise AppError("Competition not found.", "COMPETITION_NOT_FOUND", 404, {"competitionKey": key})
    editions = db_client.fetch_all(
        """
        select edition_key, season_label, published_match_count, publication_state,
               first_match_date, last_match_date, is_selectable, href
        from serving_v2.edition_catalog
        where competition_key = %s
        order by season_start_date desc nulls last, season_label desc;
        """,
        [key],
    )
    payload = {
        "competitionKey": key,
        "editions": [
            {
                "seasonLabel": _public_season_label(item["season_label"]),
                "matchCount": int(item.get("published_match_count") or 0),
                "champion": None,
                "runnerUp": None,
                "topScorer": None,
                "availability": {
                    "status": "complete" if item.get("is_selectable") else "unavailable",
                    "publicationState": item.get("publication_state"),
                    "href": item.get("href"),
                },
            }
            for item in editions
        ],
    }
    return _response(payload, request)


def _scope_row(request: Request) -> dict[str, Any] | None:
    key = _competition_key(request)
    season = _q(request, "seasonLabel") or _q(request, "seasonId")
    params: list[Any] = []
    clauses = ["e.is_selectable"]
    if key:
        clauses.append("e.competition_key = %s")
        params.append(key)
    candidates = _internal_season_candidates(season)
    if candidates:
        clauses.append("e.season_label = any(%s)")
        params.append(list(candidates))
    return db_client.fetch_one(
        f"""
        select e.edition_key, e.competition_key, e.season_label, e.publication_state,
               c.competition_name, c.is_world_cup
        from serving_v2.edition_catalog e
        join serving_v2.competition_catalog c using (competition_key)
        where {' and '.join(clauses)}
        order by e.season_start_date desc nulls last, e.edition_key
        limit 1;
        """,
        params,
    )


@router.get("/api/v1/competition-structure")
def get_competition_structure(request: Request) -> dict[str, Any]:
    scope = _scope_row(request)
    if scope is None:
        return _response({"competition": None, "stages": [], "updatedAt": None}, request, coverage={"status": "empty", "percentage": 0, "label": "Competition structure coverage"})
    stage_rows = db_client.fetch_all(
        """
        select s.stage_key, s.stage_id, s.stage_name, s.sort_order,
               count(distinct g.group_key)::int as group_count,
               count(distinct f.match_id)::int as match_count
        from mart_v2.dim_stage s
        left join mart_v2.dim_group g on g.stage_key = s.stage_key
        left join mart_v2.fact_match f on f.stage_key = s.stage_key and f.publication_state = 'published'
        where s.edition_key = %s
        group by s.stage_key, s.stage_id, s.stage_name, s.sort_order
        order by s.sort_order nulls last, s.stage_id;
        """,
        [scope["edition_key"]],
    )
    groups = db_client.fetch_all(
        "select group_key, stage_key, group_name from mart_v2.dim_group where edition_key = %s order by group_key;",
        [scope["edition_key"]],
    )
    groups_by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in groups:
        groups_by_stage[str(row["stage_key"])].append(row)
    stages = [
        {
            "stageId": str(row["stage_id"]),
            "stageName": row.get("stage_name"),
            "stageCode": row.get("stage_key"),
            "stageFormat": "group_table" if groups_by_stage.get(str(row["stage_key"])) else "knockout",
            "stageOrder": row.get("sort_order"),
            "isCurrent": row.get("sort_order") == max((int(item.get("sort_order") or 0) for item in stage_rows), default=0),
            "expectedTeams": None,
            "groups": [
                {"groupId": item["group_key"], "groupName": item.get("group_name"), "groupOrder": None, "expectedTeams": None}
                for item in groups_by_stage.get(str(row["stage_key"]), [])
            ],
            "transitions": [],
        }
        for row in stage_rows
    ]
    return _response(
        {
            "competition": {
                "competitionId": str(get_canonical_competition_by_key(scope["competition_key"]).competition_id) if get_canonical_competition_by_key(scope["competition_key"]) else scope["competition_key"],
                "competitionKey": scope["competition_key"],
                "competitionName": scope["competition_name"],
                "seasonId": _public_season_label(scope["season_label"]),
                "seasonLabel": _public_season_label(scope["season_label"]),
                "formatFamily": "group_and_knockout" if groups else "league",
                "seasonFormatCode": "canonical_v2",
                "participantScope": "published_matches",
                "groupRankingRuleCode": None,
                "tieRuleCode": None,
            },
            "stages": stages,
            "updatedAt": None,
        },
        request,
        coverage=build_coverage_from_counts(len(stages), 1, "Competition structure coverage"),
    )


@router.get("/api/v1/competition-analytics")
def get_competition_analytics(request: Request) -> dict[str, Any]:
    scope = _scope_row(request)
    if scope is None:
        return _response({"competition": None, "seasonSummary": {}, "stageAnalytics": [], "seasonComparisons": []}, request)
    row = db_client.fetch_one(
        """
        select count(*)::int as match_count,
               count(distinct stage_key)::int as stage_count,
               avg(coalesce(home_goals, 0) + coalesce(away_goals, 0))::numeric as average_goals,
               count(*) filter (where home_goals > away_goals)::int as home_wins,
               count(*) filter (where home_goals = away_goals)::int as draws,
               count(*) filter (where away_goals > home_goals)::int as away_wins
        from mart_v2.fact_match
        where edition_key = %s and publication_state = 'published';
        """,
        [scope["edition_key"]],
    ) or {}
    stage_rows = db_client.fetch_all(
        """
        select s.stage_id, s.stage_key, s.stage_name, s.sort_order,
               count(f.match_id)::int as match_count,
               count(distinct f.home_team_id)::int + count(distinct f.away_team_id)::int as team_count,
               avg(coalesce(f.home_goals, 0) + coalesce(f.away_goals, 0))::numeric as average_goals,
               count(*) filter (where f.home_goals > f.away_goals)::int as home_wins,
               count(*) filter (where f.home_goals = f.away_goals)::int as draws,
               count(*) filter (where f.away_goals > f.home_goals)::int as away_wins
        from mart_v2.dim_stage s
        left join mart_v2.fact_match f on f.stage_key = s.stage_key and f.publication_state = 'published'
        where s.edition_key = %s
        group by s.stage_id, s.stage_key, s.stage_name, s.sort_order
        order by s.sort_order nulls last, s.stage_id;
        """,
        [scope["edition_key"]],
    )
    competition = {
        "competitionKey": scope["competition_key"],
        "competitionName": scope["competition_name"],
        "seasonId": _public_season_label(scope["season_label"]),
        "seasonLabel": _public_season_label(scope["season_label"]),
    }
    return _response(
        {
            "competition": competition,
            "seasonSummary": {
                "matchCount": int(row.get("match_count") or 0),
                "totalStages": len(stage_rows),
                "tableStages": 0,
                "knockoutStages": len(stage_rows),
                "groupCount": 0,
                "tieCount": 0,
                "averageGoals": float(row["average_goals"]) if row.get("average_goals") is not None else None,
            },
            "stageAnalytics": [
                {
                    "stageId": str(item["stage_id"]),
                    "stageName": item.get("stage_name"),
                    "stageCode": item.get("stage_key"),
                    "stageFormat": "knockout",
                    "stageOrder": item.get("sort_order"),
                    "isCurrent": False,
                    "matchCount": int(item.get("match_count") or 0),
                    "teamCount": int(item.get("team_count") or 0),
                    "groupCount": 0,
                    "averageGoals": float(item["average_goals"]) if item.get("average_goals") is not None else None,
                    "homeWins": int(item.get("home_wins") or 0),
                    "draws": int(item.get("draws") or 0),
                    "awayWins": int(item.get("away_wins") or 0),
                    "tieCount": 0,
                    "resolvedTies": 0,
                    "inferredTies": 0,
                }
                for item in stage_rows
            ],
            "seasonComparisons": [],
        },
        request,
    )


def _team_filter_clauses(request: Request, alias: str, params: list[Any]) -> list[str]:
    clauses = [f"{alias}.match_count > 0"]
    entity_type = _q(request, "entityType")
    if entity_type and entity_type != "all":
        clauses.append(f"{alias}.team_type = %s")
        params.append(entity_type)
    search = _q(request, "search")
    if search:
        clauses.append(f"{alias}.team_name ilike %s")
        params.append(f"%{search}%")
    competition_key = _competition_key(request)
    season = _q(request, "seasonLabel") or _q(request, "seasonId")
    if competition_key:
        clauses.append(
            f"exists (select 1 from serving_v2.match_catalog mc where mc.competition_key = %s and (mc.home_team_id = {alias}.team_id or mc.away_team_id = {alias}.team_id)"
            + (" and mc.edition_key in (select edition_key from mart_v2.dim_edition where season_label = any(%s))" if season else "")
            + ")"
        )
        params.append(competition_key)
        if season:
            params.append(list(_internal_season_candidates(season)))
    return clauses


@router.get("/api/v1/teams")
def get_teams(request: Request) -> dict[str, Any]:
    page = max(_int_q(request, "page", 1) or 1, 1)
    page_size = min(max(_int_q(request, "pageSize", 24) or 24, 1), 100)
    offset = (page - 1) * page_size
    params: list[Any] = []
    clauses = _team_filter_clauses(request, "t", params)
    sort_by = _q(request, "sortBy", "relevance")
    sort_direction = "asc" if _q(request, "sortDirection") == "asc" else "desc"
    sort_column = {
        "teamName": "t.team_name",
        "matches": "t.match_count",
        "wins": "wins",
        "relevance": "t.match_count",
    }.get(sort_by or "relevance", "t.match_count")
    order = f"{sort_column} {sort_direction} nulls last, t.team_name asc, t.team_id"
    rows = db_client.fetch_all(
        f"""
        with totals as (
          select t.*,
                 count(*) over()::int as _total_count
          from serving_v2.team_profile t
          where {' and '.join(clauses)}
        )
        select * from totals t
        order by {order}
        limit %s offset %s;
        """,
        [*params, page_size, offset],
    )
    total = int(rows[0].get("_total_count") or 0) if rows else 0
    items = [
        {
            "teamId": str(row["team_id"]),
            "teamName": row["team_name"],
            "teamType": row.get("team_type") or "unknown",
            "countryOrTerritory": row.get("country_or_territory"),
            "competitionCount": int(row.get("competition_count") or 0),
            "seasonCount": int(row.get("edition_count") or 0),
            "firstMatchAt": row.get("first_match_date"),
            "lastMatchAt": row.get("last_match_date"),
            "stadiumName": None,
            "visualAssetId": str(row["team_id"]) if row.get("asset_url") else None,
            "visualAssetUrl": row.get("asset_url"),
            "matchesPlayed": int(row.get("match_count") or 0),
        }
        for row in rows
    ]
    filtered = any(_q(request, name) for name in ("competitionId", "seasonId", "search", "entityType"))
    return _response(
        {"items": items, "scope": {"kind": "filtered" if filtered else "archive", "label": "Recorte atual" if filtered else "Acervo publicado", "isExhaustive": False}},
        request,
        page=page,
        page_size=page_size,
        total=total,
        coverage=build_coverage_from_counts(len(items), min(total, page_size), "Teams list coverage"),
    )


@router.get("/api/v1/teams/{team_id}/contexts")
def get_team_contexts(team_id: str, request: Request) -> dict[str, Any]:
    try:
        team_id_int = int(team_id)
    except ValueError as exc:
        raise AppError("Invalid team id.", "INVALID_QUERY_PARAM", 400, {"teamId": team_id}) from exc
    rows = db_client.fetch_all(
        """
        select competition_key, competition_name, season_label,
               max(match_date) as last_match_date, count(*)::int as matches_played
        from serving_v2.match_catalog
        where home_team_id = %s or away_team_id = %s
        group by competition_key, competition_name, season_label
        order by last_match_date desc nulls last, matches_played desc, competition_key, season_label desc;
        """,
        [team_id_int, team_id_int],
    )
    contexts: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        context = _canonical_context(row.get("competition_key"), row.get("season_label"))
        if context is None or (context["competitionId"], context["seasonId"]) in seen:
            continue
        seen.add((context["competitionId"], context["seasonId"]))
        contexts.append(context)
    if not contexts:
        raise AppError("Team not found.", "TEAM_NOT_FOUND", 404, {"teamId": team_id})
    return _response(
        {
            "defaultContext": select_default_context(
                contexts,
                preferred_competition_id=_int_q(request, "competitionId"),
                preferred_season_id=_int_q(request, "seasonId"),
            ),
            "availableContexts": contexts,
        },
        request,
    )


def _match_catalog_where(request: Request, alias: str = "m") -> tuple[str, list[Any]]:
    params: list[Any] = []
    clauses = [f"{alias}.match_id is not null"]
    clauses.extend(_edition_predicate(alias, request, params))
    search = _q(request, "search")
    if search:
        clauses.append(f"({alias}.home_team_name ilike %s or {alias}.away_team_name ilike %s)")
        params.extend([f"%{search}%", f"%{search}%"])
    status = _q(request, "status")
    if status:
        clauses.append(f"coalesce({alias}.status, '') ilike %s")
        params.append(f"%{status}%")
    return " and ".join(clauses), params


def _match_item(row: dict[str, Any]) -> dict[str, Any]:
    canonical = get_canonical_competition_by_key(row.get("competition_key"))
    return {
        "matchId": str(row["match_id"]),
        "fixtureId": str(row["match_id"]),
        "competitionId": str(canonical.competition_id) if canonical else row.get("competition_key"),
        "competitionKey": row.get("competition_key"),
        "competitionName": row.get("competition_name"),
        "competitionType": None,
        "seasonId": _public_season_label(row.get("season_label")),
        "seasonLabel": _public_season_label(row.get("season_label")),
        "roundId": row.get("round_key"),
        "roundName": row.get("round_name"),
        "stageId": row.get("stage_key"),
        "stageName": row.get("stage_name"),
        "stageFormat": None,
        "groupId": row.get("group_key"),
        "groupName": None,
        "tieId": row.get("tie_key"),
        "tieOrder": row.get("tie_order"),
        "tieMatchCount": row.get("tie_match_count"),
        "legNumber": row.get("leg_number"),
        "isKnockout": bool(row.get("tie_key")),
        "kickoffAt": row.get("match_date"),
        "status": row.get("status"),
        "venueName": row.get("venue_name"),
        "homeTeamId": str(row["home_team_id"]) if row.get("home_team_id") is not None else None,
        "homeTeamName": row.get("home_team_name"),
        "awayTeamId": str(row["away_team_id"]) if row.get("away_team_id") is not None else None,
        "awayTeamName": row.get("away_team_name"),
        "homeScore": row.get("home_goals"),
        "awayScore": row.get("away_goals"),
    }


@router.get("/api/v1/teams/{team_id}")
def get_team_profile(team_id: str, request: Request) -> dict[str, Any]:
    try:
        team_id_int = int(team_id)
    except ValueError as exc:
        raise AppError("Invalid team id.", "INVALID_QUERY_PARAM", 400, {"teamId": team_id}) from exc
    team = db_client.fetch_one("select * from serving_v2.team_profile where team_id = %s and match_count > 0;", [team_id_int])
    if team is None:
        raise AppError("Team not found.", "TEAM_NOT_FOUND", 404, {"teamId": team_id})
    where, params = _match_catalog_where(request)
    summary = db_client.fetch_one(
        f"""
        select count(*)::int as matches_played,
               count(*) filter (where (home_team_id = %s and home_goals > away_goals) or (away_team_id = %s and away_goals > home_goals))::int as wins,
               count(*) filter (where home_goals = away_goals)::int as draws,
               count(*) filter (where (home_team_id = %s and home_goals < away_goals) or (away_team_id = %s and away_goals < home_goals))::int as losses,
               coalesce(sum(case when home_team_id = %s then home_goals else away_goals end), 0)::int as goals_for,
               coalesce(sum(case when home_team_id = %s then away_goals else home_goals end), 0)::int as goals_against
        from serving_v2.match_catalog m
        where {where} and (m.home_team_id = %s or m.away_team_id = %s);
        """,
        [team_id_int, team_id_int, team_id_int, team_id_int, team_id_int, team_id_int, *params, team_id_int, team_id_int],
    ) or {}
    recent = db_client.fetch_all(
        f"""
        select m.* from serving_v2.match_catalog m
        where {where} and (m.home_team_id = %s or m.away_team_id = %s)
        order by m.match_date desc, m.match_id desc limit %s;
        """,
        [*params, team_id_int, team_id_int, min(_int_q(request, "recentMatchesLimit", 10) or 10, 50)],
    )
    matches_played = int(summary.get("matches_played") or 0)
    wins = int(summary.get("wins") or 0)
    draws = int(summary.get("draws") or 0)
    losses = int(summary.get("losses") or 0)
    goals_for = int(summary.get("goals_for") or 0)
    goals_against = int(summary.get("goals_against") or 0)
    recent_items = []
    for row in recent:
        item = _match_item(row)
        home = int(row["home_team_id"]) == team_id_int
        gf = int(row.get("home_goals") or 0) if home else int(row.get("away_goals") or 0)
        ga = int(row.get("away_goals") or 0) if home else int(row.get("home_goals") or 0)
        recent_items.append({"matchId": item["matchId"], "playedAt": item["kickoffAt"], "opponentTeamId": item["awayTeamId"] if home else item["homeTeamId"], "opponentName": item["awayTeamName"] if home else item["homeTeamName"], "venue": "home" if home else "away", "goalsFor": gf, "goalsAgainst": ga, "result": "win" if gf > ga else "loss" if gf < ga else "draw"})
    team_context = _canonical_context(_competition_key(request), _q(request, "seasonLabel") or _q(request, "seasonId"))
    data: dict[str, Any] = {
        "team": {"teamId": team_id, "teamName": team["team_name"], "visualAssetId": team_id if team.get("asset_url") else None, "visualAssetUrl": team.get("asset_url"), **(team_context or {})},
        "identity": {"teamType": team.get("team_type") or "unknown", "officialName": team["team_name"], "countryOrTerritory": team.get("country_or_territory"), "city": None, "foundedYear": None, "stadiumName": None, "stadiumCapacity": None, "assetUrl": team.get("asset_url"), "assetType": team.get("asset_type")},
        "archive": {"competitionCount": int(team.get("competition_count") or 0), "seasonCount": int(team.get("edition_count") or 0), "matchesPlayed": int(team.get("match_count") or 0), "firstMatchAt": team.get("first_match_date"), "lastMatchAt": team.get("last_match_date")},
        "summary": {"matchesPlayed": matches_played, "wins": wins, "draws": draws, "losses": losses, "goalsFor": goals_for, "goalsAgainst": goals_against, "goalDiff": goals_for - goals_against, "points": wins * 3 + draws},
        "standing": None,
        "form": [item["result"] for item in recent_items[:5]],
        "recentMatches": recent_items if _bool_q(request, "includeRecentMatches", True) else None,
        "squad": [],
        "stats": {"pointsPerMatch": round((wins * 3 + draws) / matches_played, 2) if matches_played else None, "winRatePct": round(wins / matches_played * 100, 2) if matches_played else None, "goalsForPerMatch": round(goals_for / matches_played, 2) if matches_played else None, "goalsAgainstPerMatch": round(goals_against / matches_played, 2) if matches_played else None, "cleanSheets": None, "failedToScore": None, "trend": []},
        "honors": None,
        "sectionCoverage": {"overview": build_coverage_from_counts(matches_played, matches_played, "Team overview coverage"), "identity": {"status": "complete", "percentage": 100, "label": "Team identity coverage"}, "archive": {"status": "complete", "percentage": 100, "label": "Team archive coverage"}},
    }
    if not _bool_q(request, "includeStats", True):
        data.pop("stats", None)
    if not _bool_q(request, "includeSquad", True):
        data.pop("squad", None)
    return _response(data, request)


def _player_scope_clauses(request: Request, alias: str, params: list[Any]) -> list[str]:
    clauses = [f"{alias}.publication_state = 'published'"]
    competition_key = _competition_key(request)
    if competition_key:
        clauses.append(f"{alias}.competition_key = %s")
        params.append(competition_key)
    season = _q(request, "seasonLabel") or _q(request, "seasonId")
    candidates = _internal_season_candidates(season)
    if candidates:
        clauses.append(f"{alias}.edition_key in (select edition_key from mart_v2.dim_edition where season_label = any(%s))")
        params.append(list(candidates))
    team_id = _int_q(request, "teamId")
    if team_id is not None:
        clauses.append("ps.team_id = %s")
        params.append(team_id)
    date_start = _q(request, "dateStart") or _q(request, "dateRangeStart")
    date_end = _q(request, "dateEnd") or _q(request, "dateRangeEnd")
    if date_start:
        clauses.append(f"{alias}.match_date >= %s")
        params.append(date_start)
    if date_end:
        clauses.append(f"{alias}.match_date <= %s")
        params.append(date_end)
    return clauses


def _player_aggregate_query(request: Request, *, player_filter: str | None = None, player_params: list[Any] | None = None) -> tuple[str, list[Any]]:
    params: list[Any] = []
    clauses = _player_scope_clauses(request, "f", params)
    if player_filter:
        clauses.append(player_filter)
        params.extend(player_params or [])
    return (
        f"""
        with scoped as (
          select ps.*, f.match_date, f.competition_key, f.edition_key,
                 f.home_team_id, f.away_team_id, f.home_goals, f.away_goals
          from mart_v2.fact_match_player_stats ps
          join mart_v2.fact_match f on f.match_id = ps.match_id
          where {' and '.join(clauses)}
        )
        select s.player_key, max(p.display_name) as player_name, max(p.nationality) as nationality,
               max(s.match_date) as career_end_at, min(s.match_date) as career_start_at,
               count(distinct s.match_id)::int as matches_played,
               count(distinct s.team_id)::int as team_count,
               count(distinct s.competition_key)::int as competition_count,
               count(distinct s.edition_key)::int as season_count,
               coalesce(sum(s.minutes_played), 0)::numeric as minutes_played,
               coalesce(sum(s.goals), 0)::numeric as goals,
               coalesce(sum(s.assists), 0)::numeric as assists,
               coalesce(sum(s.total_shots), 0)::numeric as shots_total,
               coalesce(sum(s.shots_on_target), 0)::numeric as shots_on_target,
               coalesce(sum(s.total_passes), 0)::numeric as passes_attempted,
               coalesce(sum(s.accurate_passes), 0)::numeric as passes_completed,
               coalesce(sum(s.yellow_cards), 0)::numeric as yellow_cards,
               coalesce(sum(s.red_cards), 0)::numeric as red_cards,
               avg(s.rating)::numeric as rating
        from scoped s
        join mart_v2.dim_player p on p.player_key = s.player_key
        group by s.player_key
        """,
        params,
    )


@router.get("/api/v1/players")
def get_players(request: Request) -> dict[str, Any]:
    page = max(_int_q(request, "page", 1) or 1, 1)
    page_size = min(max(_int_q(request, "pageSize", 20) or 20, 1), 100)
    offset = (page - 1) * page_size
    params: list[Any] = []
    clauses = ["p.match_count > 0"]
    search = _q(request, "search")
    if search:
        clauses.append("p.display_name ilike %s")
        params.append(f"%{search}%")
    position = _q(request, "position")
    if position:
        clauses.append("coalesce(p.position_name, '') ilike %s")
        params.append(f"%{position}%")
    team_id = _int_q(request, "teamId")
    if team_id is not None:
        clauses.append("exists (select 1 from mart_v2.fact_match_player_stats ps where ps.player_key = p.player_key and ps.team_id = %s)")
        params.append(team_id)
    rows = db_client.fetch_all(
        f"""
        select p.player_key, p.display_name, p.nationality, p.position_name,
               p.match_count, p.team_count, p.first_match_date, p.last_match_date,
               count(*) over()::int as _total_count
        from serving_v2.player_profile p
        where {' and '.join(clauses)}
        order by p.match_count desc, p.display_name asc nulls last, p.player_key
        limit %s offset %s;
        """,
        [*params, page_size, offset],
    )
    total = int(rows[0].get("_total_count") or 0) if rows else 0
    items = [
        {
            "playerId": row["player_key"],
            "playerName": row.get("display_name") or "Nome indisponível",
            "teamId": None,
            "teamName": None,
            "position": row.get("position_name"),
            "nationality": row.get("nationality"),
            "teamCount": int(row.get("team_count") or 0),
            "teamContextLabel": f"{int(row.get('team_count') or 0)} clubes" if int(row.get("team_count") or 0) > 1 else None,
            "recentTeams": [],
            "competitionCount": None,
            "seasonCount": None,
            "careerStartAt": row.get("first_match_date"),
            "careerEndAt": row.get("last_match_date"),
            "matchesPlayed": int(row.get("match_count") or 0),
            "minutesPlayed": None,
            "goals": None,
            "assists": None,
            "shotsTotal": None,
            "passAccuracyPct": None,
            "yellowCards": None,
            "redCards": None,
            "rating": None,
        }
        for row in rows
    ]
    return _response(
        {"items": items, "scope": {"kind": "filtered" if any(_q(request, name) for name in ("competitionId", "seasonId", "search", "teamId", "position")) else "archive", "label": "Recorte atual", "isExhaustive": False}},
        request,
        page=page,
        page_size=page_size,
        total=total,
        coverage=build_coverage_from_counts(len(items), min(total, page_size), "Players list coverage"),
    )


def _player_contexts(player_key: str, request: Request) -> list[dict[str, str]]:
    rows = db_client.fetch_all(
        """
        select f.competition_key, c.competition_name, f.edition_key, e.season_label,
               max(f.match_date) as last_match_date, count(distinct f.match_id)::int as matches_played
        from mart_v2.fact_match_player_stats ps
        join mart_v2.fact_match f on f.match_id = ps.match_id and f.publication_state = 'published'
        join mart_v2.dim_competition c on c.competition_key = f.competition_key
        join mart_v2.dim_edition e on e.edition_key = f.edition_key
        where ps.player_key = %s
        group by f.competition_key, c.competition_name, f.edition_key, e.season_label
        order by last_match_date desc nulls last, matches_played desc;
        """,
        [player_key],
    )
    contexts: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        context = _canonical_context(row.get("competition_key"), row.get("season_label"))
        if context is None or (context["competitionId"], context["seasonId"]) in seen:
            continue
        seen.add((context["competitionId"], context["seasonId"]))
        contexts.append(context)
    return contexts


@router.get("/api/v1/players/{player_id}/contexts")
def get_player_contexts(player_id: str, request: Request) -> dict[str, Any]:
    exists = db_client.fetch_one("select player_key from mart_v2.dim_player where player_key = %s;", [player_id])
    if exists is None:
        raise AppError("Player not found.", "PLAYER_NOT_FOUND", 404, {"playerId": player_id})
    contexts = _player_contexts(player_id, request)
    return _response(
        {"defaultContext": select_default_context(contexts, preferred_competition_id=_int_q(request, "competitionId"), preferred_season_id=_int_q(request, "seasonId")), "availableContexts": contexts},
        request,
    )


@router.get("/api/v1/players/{player_id}")
def get_player_profile(player_id: str, request: Request) -> dict[str, Any]:
    player = db_client.fetch_one("select * from serving_v2.player_profile where player_key = %s;", [player_id])
    if player is None:
        raise AppError("Player not found.", "PLAYER_NOT_FOUND", 404, {"playerId": player_id})
    aggregate_sql, aggregate_params = _player_aggregate_query(request, player_filter="s.player_key = %s", player_params=[player_id])
    aggregate = db_client.fetch_one(f"{aggregate_sql};", aggregate_params) or {}
    recent = db_client.fetch_all(
        """
        select f.*, ps.team_id as player_team_id, ps.minutes_played, ps.goals, ps.assists,
               ps.total_shots, ps.shots_on_target, ps.total_passes, ps.rating,
               t.team_name as player_team_name
        from mart_v2.fact_match_player_stats ps
        join mart_v2.fact_match f on f.match_id = ps.match_id and f.publication_state = 'published'
        left join mart_v2.dim_team t on t.team_id = ps.team_id
        where ps.player_key = %s
        order by f.match_date desc, f.match_id desc
        limit %s;
        """,
        [player_id, min(_int_q(request, "recentMatchesLimit", 10) or 10, 50)],
    )
    career_rows = db_client.fetch_all(
        """
        select ps.team_id, max(t.team_name) as team_name,
               count(distinct ps.match_id)::int as matches_played,
               count(distinct f.competition_key)::int as competition_count,
               count(distinct f.edition_key)::int as season_count,
               coalesce(sum(ps.minutes_played), 0)::int as minutes_played,
               coalesce(sum(ps.goals), 0)::int as goals,
               coalesce(sum(ps.assists), 0)::int as assists,
               min(f.match_date) as first_match_at, max(f.match_date) as last_match_at
        from mart_v2.fact_match_player_stats ps
        join mart_v2.fact_match f on f.match_id = ps.match_id and f.publication_state = 'published'
        left join mart_v2.dim_team t on t.team_id = ps.team_id
        where ps.player_key = %s
        group by ps.team_id order by last_match_at desc nulls last;
        """,
        [player_id],
    )
    matches_played = int(aggregate.get("matches_played") or player.get("match_count") or 0)
    minutes = float(aggregate.get("minutes_played") or 0)
    goals = float(aggregate.get("goals") or 0)
    assists = float(aggregate.get("assists") or 0)
    summary = {
        "matchesPlayed": int(matches_played), "minutesPlayed": int(minutes), "goals": int(goals), "assists": int(assists),
        "shotsTotal": int(float(aggregate.get("shots_total") or 0)), "shotsOnTarget": int(float(aggregate.get("shots_on_target") or 0)),
        "passesCompleted": int(float(aggregate.get("passes_completed") or 0)), "passesAttempted": int(float(aggregate.get("passes_attempted") or 0)),
        "passAccuracyPct": round(float(aggregate["passes_completed"]) / float(aggregate["passes_attempted"]) * 100, 2) if aggregate.get("passes_attempted") else None,
        "yellowCards": int(float(aggregate.get("yellow_cards") or 0)), "redCards": int(float(aggregate.get("red_cards") or 0)),
        "rating": float(aggregate["rating"]) if aggregate.get("rating") is not None else None,
    }
    contexts = _player_contexts(player_id, request)
    profile = {
        "player": {"playerId": player_id, "playerName": player.get("display_name") or "Nome indisponível", "teamId": None, "teamName": None, "position": player.get("position_name"), "nationality": player.get("nationality"), "lastMatchAt": player.get("last_match_date")},
        "summary": summary,
        "career": {"teamCount": len(career_rows), "clubCount": len(career_rows), "nationalTeamCount": 0, "competitionCount": int(aggregate.get("competition_count") or 0), "seasonCount": int(aggregate.get("season_count") or 0), "firstMatchAt": aggregate.get("career_start_at"), "lastMatchAt": aggregate.get("career_end_at"), "teams": [{"teamId": str(row["team_id"]), "teamName": row.get("team_name") or "Time indisponível", "teamType": "unknown", "competitionCount": int(row.get("competition_count") or 0), "seasonCount": int(row.get("season_count") or 0), "matchesPlayed": int(row.get("matches_played") or 0), "minutesPlayed": int(row.get("minutes_played") or 0), "goals": int(row.get("goals") or 0), "assists": int(row.get("assists") or 0), "firstMatchAt": row.get("first_match_at"), "lastMatchAt": row.get("last_match_at")} for row in career_rows]},
        "profileMeta": {"profileType": "sportmonks_with_history", "dataSource": "sportmonks", "hasHistoricalStats": bool(matches_played), "historyAvailability": "available" if matches_played else "unavailable", "isWorldCupLinked": False, "worldCup": None},
        "recentMatches": [{"fixtureId": str(row["match_id"]), "matchId": str(row["match_id"]), "playedAt": row.get("match_date"), "competitionId": str(get_canonical_competition_by_key(row.get("competition_key")).competition_id) if get_canonical_competition_by_key(row.get("competition_key")) else row.get("competition_key"), "competitionName": row.get("competition_key"), "seasonId": _public_season_label(row.get("edition_key")), "teamId": str(row["player_team_id"]) if row.get("player_team_id") is not None else None, "teamName": row.get("player_team_name"), "minutesPlayed": int(row.get("minutes_played") or 0), "goals": int(row.get("goals") or 0), "assists": int(row.get("assists") or 0), "shotsTotal": int(row.get("total_shots") or 0), "shotsOnTarget": int(row.get("shots_on_target") or 0), "passesAttempted": int(row.get("total_passes") or 0), "rating": float(row["rating"]) if row.get("rating") is not None else None} for row in recent] if _bool_q(request, "includeRecentMatches", True) else None,
        "history": [{"competitionKey": row.get("competition_key"), "seasonLabel": _public_season_label(row.get("season_label")), "teamId": None, "teamName": None, "matchesPlayed": int(row.get("matches_played") or 0), "minutesPlayed": int(row.get("minutes_played") or 0), "goals": int(row.get("goals") or 0), "assists": int(row.get("assists") or 0), "rating": float(row["rating"]) if row.get("rating") is not None else None, "lastMatchAt": row.get("last_match_date")} for row in db_client.fetch_all("select f.competition_key, e.season_label, count(distinct ps.match_id)::int as matches_played, coalesce(sum(ps.minutes_played), 0)::int as minutes_played, coalesce(sum(ps.goals), 0)::int as goals, coalesce(sum(ps.assists), 0)::int as assists, avg(ps.rating)::numeric as rating, max(f.match_date) as last_match_date from mart_v2.fact_match_player_stats ps join mart_v2.fact_match f on f.match_id = ps.match_id and f.publication_state = 'published' join mart_v2.dim_edition e on e.edition_key = f.edition_key where ps.player_key = %s group by f.competition_key, e.season_label order by last_match_date desc;", [player_id])] if _bool_q(request, "includeHistory", True) else None,
        "stats": {"minutesPerMatch": round(minutes / matches_played, 2) if matches_played else None, "goalsPer90": round(goals / minutes * 90, 2) if minutes else None, "assistsPer90": round(assists / minutes * 90, 2) if minutes else None, "goalContributionsPer90": round((goals + assists) / minutes * 90, 2) if minutes else None, "shotsPer90": None, "shotsOnTargetPer90": None, "shotsOnTargetPct": None, "passesAttemptedPer90": None, "yellowCardsPer90": None, "redCardsPer90": None, "trend": []} if _bool_q(request, "includeStats", True) else None,
        "sectionCoverage": {"overview": build_coverage_from_counts(matches_played, matches_played, "Player overview coverage"), "history": build_coverage_from_counts(len(contexts), len(contexts), "Player history coverage"), "matches": build_coverage_from_counts(len(recent), len(recent), "Player match coverage"), "stats": build_coverage_from_counts(matches_played, matches_played, "Player stats coverage")},
    }
    return _response(profile, request)


def _match_query(request: Request, *, detail: bool = False) -> tuple[str, list[Any]]:
    where, params = _match_catalog_where(request)
    content = _q(request, "contentSection")
    if _bool_q(request, "hasContent") or content:
        content_checks = {
            "events": "exists (select 1 from mart_v2.fact_match_event x where x.match_id = m.match_id)",
            "lineups": "exists (select 1 from mart_v2.fact_lineup x where x.match_id = m.match_id)",
            "teamStats": "exists (select 1 from mart_v2.fact_match_team_stats x where x.match_id = m.match_id)",
            "playerStats": "exists (select 1 from mart_v2.fact_match_player_stats x where x.match_id = m.match_id)",
        }
        checks = [content_checks[content]] if content in content_checks else list(content_checks.values())
        clauses = [where, "(" + " or ".join(checks) + ")"]
        where = " and ".join(clauses)
    return where, params


def _match_select() -> str:
    return """
      select m.*
      from serving_v2.match_catalog m
    """


def _match_flags(match_ids: list[int]) -> dict[int, dict[str, bool]]:
    if not match_ids:
        return {}
    rows = db_client.fetch_all(
        """
        select match_id,
               bool_or(kind = 'events') as has_events,
               bool_or(kind = 'lineups') as has_lineups,
               bool_or(kind = 'team_stats') as has_team_stats,
               bool_or(kind = 'player_stats') as has_player_stats,
               bool_or(kind = 'odds') as has_odds
        from (
          select match_id, 'events'::text as kind from mart_v2.fact_match_event where match_id = any(%s)
          union all
          select match_id, 'lineups' from mart_v2.fact_lineup where match_id = any(%s)
          union all
          select match_id, 'team_stats' from mart_v2.fact_match_team_stats where match_id = any(%s)
          union all
          select match_id, 'player_stats' from mart_v2.fact_match_player_stats where match_id = any(%s)
          union all
          select match_id, 'odds' from mart_v2.fact_match_odds where match_id = any(%s)
        ) content
        group by match_id;
        """,
        [match_ids] * 5,
    )
    return {int(row["match_id"]): {key: bool(row.get(key)) for key in ("has_events", "has_lineups", "has_team_stats", "has_player_stats", "has_odds")} for row in rows}


@router.get("/api/v1/matches")
def get_matches(request: Request) -> dict[str, Any]:
    page = max(_int_q(request, "page", 1) or 1, 1)
    page_size = min(max(_int_q(request, "pageSize", 20) or 20, 1), 100)
    offset = (page - 1) * page_size
    where, params = _match_query(request)
    sort_by = _q(request, "sortBy", "kickoffAt")
    sort_dir = "asc" if _q(request, "sortDirection") == "asc" else "desc"
    sort_column = {"status": "m.status", "homeTeamName": "home_team_name", "awayTeamName": "away_team_name"}.get(sort_by or "", "m.match_date")
    total_row = (
        db_client.fetch_one(
            "select metric_value as total_count from serving_v2.publication_metrics where metric_key = 'published_matches';"
        )
        if where == "m.match_id is not null"
        else db_client.fetch_one(
            f"select count(*)::int as total_count from serving_v2.match_catalog m where {where};",
            params,
        )
    ) or {}
    rows = db_client.fetch_all(
        f"""
        with enriched as ({_match_select()})
        select m.*
        from enriched m
        where {where}
        order by {sort_column} {sort_dir} nulls last, match_id desc
        limit %s offset %s;
        """,
        [*params, page_size, offset],
    )
    flags = _match_flags([int(row["match_id"]) for row in rows])
    for row in rows:
        row.update(flags.get(int(row["match_id"]), {}))
    total = int(total_row.get("total_count") or 0)
    items = []
    for row in rows:
        item = _match_item(row)
        item["depthProfile"] = {
            "hasMatchContext": True,
            "hasScore": row.get("home_goals") is not None and row.get("away_goals") is not None,
            "hasOdds": bool(row.get("has_odds")),
            "hasTeamStats": bool(row.get("has_team_stats")),
            "hasEvents": bool(row.get("has_events")),
            "hasLineups": bool(row.get("has_lineups")),
            "hasPlayerStats": bool(row.get("has_player_stats")),
            "hasPlayerLayer": bool(row.get("has_player_stats") or row.get("has_lineups")),
            "hasMinimumRichDepth": bool(row.get("has_events") or row.get("has_lineups") or row.get("has_team_stats")),
            "safeSections": [key for key, value in (("events", row.get("has_events")), ("lineups", row.get("has_lineups")), ("teamStats", row.get("has_team_stats")), ("playerStats", row.get("has_player_stats"))) if value],
            "depthScore": sum(bool(row.get(key)) for key in ("has_events", "has_lineups", "has_team_stats", "has_player_stats", "has_odds")),
            "counts": {"validEventRows": 0, "validLineupRows": 0, "validPlayerStatRows": 0, "validTeamStatRows": 0, "valid1x2Rows": 0},
        }
        items.append(item)
    return _response(
        {"items": items, "contentSummary": {"totalMatches": total, "withAnyContent": sum(bool(row.get("has_events") or row.get("has_lineups") or row.get("has_team_stats") or row.get("has_player_stats")) for row in rows), "sections": {"events": 0, "lineups": 0, "teamStats": 0, "playerStats": 0}}},
        request,
        page=page,
        page_size=page_size,
        total=total,
    )


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


@router.get("/api/v1/matches/{match_id}")
def get_match(match_id: str, request: Request) -> dict[str, Any]:
    try:
        match_id_int = int(match_id)
    except ValueError as exc:
        raise AppError("Invalid match id.", "INVALID_QUERY_PARAM", 400, {"matchId": match_id}) from exc
    rows = db_client.fetch_all(
        f"{_match_select()} where m.match_id = %s limit 1;",
        [match_id_int],
    )
    if not rows:
        raise AppError("Match not found.", "MATCH_NOT_FOUND", 404, {"matchId": match_id})
    row = rows[0]
    timeline = db_client.fetch_all(
        """
        select e.event_key, e.minute, e.extra_minute, e.period, e.event_type,
               e.event_detail, e.team_id, t.team_name, e.player_key, p.display_name
        from mart_v2.fact_match_event e
        left join mart_v2.dim_team t on t.team_id = e.team_id
        left join mart_v2.dim_player p on p.player_key = e.player_key
        where e.match_id = %s order by e.period nulls last, e.minute nulls last, e.event_key;
        """,
        [match_id_int],
    )
    lineups = db_client.fetch_all(
        """
        select l.player_key, p.display_name as player_name, l.team_id, t.team_name,
               l.position_name, l.formation_position, l.jersey_number, l.lineup_type
        from mart_v2.fact_lineup l
        left join mart_v2.dim_player p on p.player_key = l.player_key
        left join mart_v2.dim_team t on t.team_id = l.team_id
        where l.match_id = %s order by l.team_id, l.lineup_type, l.jersey_number nulls last, l.player_key;
        """,
        [match_id_int],
    )
    team_stats = db_client.fetch_all(
        """
        select s.*, t.team_name
        from mart_v2.fact_match_team_stats s
        left join mart_v2.dim_team t on t.team_id = s.team_id
        where s.match_id = %s order by s.team_id;
        """,
        [match_id_int],
    )
    player_stats = db_client.fetch_all(
        """
        select s.*, p.display_name as player_name, t.team_name
        from mart_v2.fact_match_player_stats s
        left join mart_v2.dim_player p on p.player_key = s.player_key
        left join mart_v2.dim_team t on t.team_id = s.team_id
        where s.match_id = %s order by s.team_id, s.player_key;
        """,
        [match_id_int],
    )
    payload: dict[str, Any] = {"match": _match_item(row), "timeline": [{"eventId": item.get("event_key"), "minute": item.get("minute"), "period": str(item.get("period")) if item.get("period") is not None else None, "type": item.get("event_type"), "detail": item.get("event_detail"), "teamId": str(item["team_id"]) if item.get("team_id") is not None else None, "teamName": item.get("team_name"), "playerId": item.get("player_key"), "playerName": item.get("display_name")} for item in timeline], "lineups": [{"playerId": item.get("player_key"), "playerName": item.get("player_name"), "teamId": str(item["team_id"]) if item.get("team_id") is not None else None, "teamName": item.get("team_name"), "position": item.get("position_name"), "formationPosition": item.get("formation_position"), "shirtNumber": item.get("jersey_number"), "isStarter": str(item.get("lineup_type") or "").lower() in {"11", "starter", "starting"}} for item in lineups], "teamStats": [{"teamId": str(item["team_id"]) if item.get("team_id") is not None else None, "teamName": item.get("team_name"), "totalShots": item.get("total_shots"), "shotsOnGoal": item.get("shots_on_goal"), "possessionPct": float(item["ball_possession"]) if item.get("ball_possession") is not None else None, "totalPasses": item.get("total_passes"), "passesAccurate": item.get("passes_accurate"), "passAccuracyPct": float(item["passes_pct"]) if item.get("passes_pct") is not None else None, "corners": item.get("corner_kicks"), "fouls": item.get("fouls"), "yellowCards": item.get("yellow_cards"), "redCards": item.get("red_cards"), "goalkeeperSaves": item.get("goalkeeper_saves")} for item in team_stats], "playerStats": [{"playerId": item.get("player_key"), "playerName": item.get("player_name"), "teamId": str(item["team_id"]) if item.get("team_id") is not None else None, "teamName": item.get("team_name"), "minutesPlayed": float(item["minutes_played"]) if item.get("minutes_played") is not None else None, "goals": float(item["goals"]) if item.get("goals") is not None else None, "assists": float(item["assists"]) if item.get("assists") is not None else None, "shotsTotal": float(item["total_shots"]) if item.get("total_shots") is not None else None, "shotsOnGoal": float(item["shots_on_target"]) if item.get("shots_on_target") is not None else None, "passesTotal": float(item["total_passes"]) if item.get("total_passes") is not None else None, "rating": float(item["rating"]) if item.get("rating") is not None else None} for item in player_stats]}
    payload["sectionCoverage"] = {"timeline": build_coverage_from_counts(len(timeline), 1, "Timeline coverage"), "lineups": build_coverage_from_counts(len(lineups), 1, "Lineup coverage"), "teamStats": build_coverage_from_counts(len(team_stats), 1, "Team stats coverage"), "playerStats": build_coverage_from_counts(len(player_stats), 1, "Player stats coverage")}
    payload["depthProfile"] = {"hasMatchContext": True, "hasScore": row.get("home_goals") is not None and row.get("away_goals") is not None, "hasOdds": False, "hasTeamStats": bool(team_stats), "hasEvents": bool(timeline), "hasLineups": bool(lineups), "hasPlayerStats": bool(player_stats), "hasPlayerLayer": bool(lineups or player_stats), "hasMinimumRichDepth": bool(timeline or lineups or team_stats), "safeSections": [key for key, value in (("events", timeline), ("lineups", lineups), ("teamStats", team_stats), ("playerStats", player_stats)) if value], "depthScore": sum(bool(value) for value in (timeline, lineups, team_stats, player_stats))}
    return _response(payload, request)


def _search_types(request: Request) -> tuple[str, ...]:
    raw = _q(request, "types") or _q(request, "type")
    allowed = ("competition", "edition", "team", "player", "match")
    if not raw:
        return allowed
    values = tuple(item for item in (part.strip() for part in raw.split(",")) if item in allowed)
    return values or allowed


@router.get("/api/v1/search")
def get_search(request: Request) -> dict[str, Any]:
    query = _q(request, "q") or _q(request, "query")
    if not query or len(query) < 2:
        raise AppError("Search query must contain at least two characters.", "INVALID_QUERY_PARAM", 400, {"field": "q"})
    limit = min(max(_int_q(request, "limit", 5) or 5, 1), 20)
    types = _search_types(request)
    rows = db_client.fetch_all(
        """
        with candidates as (
          select d.*, similarity(d.search_text, lower(unaccent(%s))) as score
          from serving_v2.search_document d
          where d.publication_state = 'published'
            and d.entity_type = any(%s)
            and d.search_text ilike '%%' || lower(unaccent(%s)) || '%%'
          order by score desc, d.label asc, d.entity_id
          limit %s
        )
        select * from candidates;
        """,
        [query, list(types), query, limit * len(types)],
    )
    grouped: dict[str, list[dict[str, Any]]] = {entity_type: [] for entity_type in types}
    for row in rows:
        entity_type = row.get("entity_type")
        if entity_type not in grouped:
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        context = _canonical_context(row.get("competition_key"), row.get("edition_key", "").split(":", 1)[-1] if row.get("edition_key") else None)
        if entity_type == "competition":
            item = {"competitionId": str(get_canonical_competition_by_key(row.get("entity_id")).competition_id) if get_canonical_competition_by_key(row.get("entity_id")) else row.get("entity_id"), "competitionKey": row.get("entity_id"), "competitionName": row.get("label")}
        elif entity_type == "edition":
            item = {"competitionKey": row.get("competition_key"), "seasonLabel": _public_season_label(row.get("edition_key", "").split(":", 1)[-1]), "label": row.get("label"), "href": row.get("href"), "defaultContext": context}
        elif entity_type == "team":
            item = {"teamId": row.get("entity_id"), "teamName": row.get("label"), "teamType": metadata.get("team_type") or "unknown", "defaultContext": context}
        elif entity_type == "player":
            item = {"playerId": row.get("entity_id"), "playerName": row.get("label"), "teamId": None, "teamName": None, "position": metadata.get("position_name"), "defaultContext": context}
        else:
            item = {"matchId": row.get("entity_id"), "competitionId": str(get_canonical_competition_by_key(row.get("competition_key")).competition_id) if get_canonical_competition_by_key(row.get("competition_key")) else row.get("competition_key"), "competitionName": row.get("subtitle"), "seasonId": _public_season_label(row.get("edition_key", "").split(":", 1)[-1]) if row.get("edition_key") else None, "kickoffAt": metadata.get("match_date"), "homeTeamId": str(metadata["home_team_id"]) if metadata.get("home_team_id") is not None else None, "homeTeamName": metadata.get("home_team_name"), "awayTeamId": str(metadata["away_team_id"]) if metadata.get("away_team_id") is not None else None, "awayTeamName": metadata.get("away_team_name"), "homeScore": metadata.get("home_goals"), "awayScore": metadata.get("away_goals"), "defaultContext": context}
        item["href"] = row.get("href")
        grouped[entity_type].append(item)
    result = [{"type": entity_type, "items": grouped[entity_type][:limit], "total": len(grouped[entity_type][:limit])} for entity_type in types]
    return _response(result, request)


@router.get("/api/v1/insights", deprecated=True)
def get_insights_v2(request: Request) -> dict[str, Any]:
    entity_type = _q(request, "entityType", "global") or "global"
    entity_id = _q(request, "entityId")
    if entity_type != "global" and not entity_id:
        raise AppError(
            "Invalid insight context. 'entityId' is required when entityType is not 'global'.",
            "INVALID_INSIGHT_CONTEXT",
            400,
            {"entityType": entity_type},
        )

    where, params = _match_catalog_where(request)
    if entity_type == "team" and entity_id:
        try:
            team_id = int(entity_id)
        except ValueError as exc:
            raise AppError("Invalid team id.", "INVALID_QUERY_PARAM", 400, {"entityId": entity_id}) from exc
        where = f"({where}) and (m.home_team_id = %s or m.away_team_id = %s)"
        params.extend([team_id, team_id])

    summary = db_client.fetch_one(
        f"""
        select count(*)::int as match_count,
               coalesce(sum(coalesce(home_goals, 0) + coalesce(away_goals, 0)), 0)::int as goal_count,
               min(match_date) as first_match_date,
               max(match_date) as last_match_date
        from serving_v2.match_catalog m
        where {where};
        """,
        params,
    ) or {}
    match_count = int(summary.get("match_count") or 0)
    if match_count == 0:
        return _response([], request)

    season = _public_season_label(_q(request, "seasonLabel") or _q(request, "seasonId"))
    reference_period = season or f"{summary.get('first_match_date')} — {summary.get('last_match_date')}"
    return _response(
        [
            {
                "insight_id": f"v2-{entity_type}-match-volume",
                "severity": "info",
                "explanation": f"O recorte publicado contém {match_count} partidas e {int(summary.get('goal_count') or 0)} gols registrados.",
                "evidences": {
                    "matches": match_count,
                    "goals": int(summary.get("goal_count") or 0),
                },
                "reference_period": reference_period,
                "data_source": ["serving_v2.match_catalog"],
            }
        ],
        request,
    )


@router.get("/api/v1/coaches")
def get_coaches(request: Request) -> dict[str, Any]:
    page = max(_int_q(request, "page", 1) or 1, 1)
    page_size = min(max(_int_q(request, "pageSize", 24) or 24, 1), 100)
    offset = (page - 1) * page_size
    search = _q(request, "search")
    params: list[Any] = []
    clauses = ["1 = 1"]
    if search:
        clauses.append("coalesce(c.display_name, '') ilike %s")
        params.append(f"%{search}%")
    rows = db_client.fetch_all(
        f"""
        select c.*, count(*) over()::int as _total_count
        from mart_v2.dim_coach c
        where {' and '.join(clauses)}
        order by c.source_count desc, c.display_name asc nulls last, c.coach_key
        limit %s offset %s;
        """,
        [*params, page_size, offset],
    )
    total = int(rows[0].get("_total_count") or 0) if rows else 0
    items = [
        {
            "coachId": row["coach_key"],
            "coachName": row.get("display_name") or "Nome indisponível",
            "photoUrl": row.get("image_url"),
            "hasRealPhoto": bool(row.get("image_url")),
            "mediaStatus": "real" if row.get("image_url") else "unavailable",
            "teamId": None,
            "teamName": "Time indisponível",
            "dataStatus": "partial",
            "active": False,
            "temporary": False,
            "tenureCount": 0,
            "activeTenures": 0,
            "matches": 0,
            "wins": 0,
            "draws": 0,
            "losses": 0,
            "points": 0,
            "goalsFor": 0,
            "goalsAgainst": 0,
            "goalDiff": 0,
            "adjustedPpm": None,
            "pointsPerMatch": None,
            "lastMatchDate": None,
            "startDate": None,
            "endDate": None,
            "context": None,
        }
        for row in rows
    ]
    return _response(
        {"items": items},
        request,
        page=page,
        page_size=page_size,
        total=total,
        coverage={"status": "partial" if items else "empty", "percentage": None, "label": "Coach directory coverage"},
    )


@router.get("/api/v1/coaches/{coach_id}")
def get_coach(coach_id: str, request: Request) -> dict[str, Any]:
    row = db_client.fetch_one("select * from mart_v2.dim_coach where coach_key = %s;", [coach_id])
    if row is None:
        raise AppError("Coach not found.", "COACH_NOT_FOUND", 404, {"coachId": coach_id})
    return _response({"coach": {"coachId": coach_id, "coachName": row.get("display_name") or "Nome indisponível", "imageUrl": row.get("image_url")}, "summary": {"matches": 0, "wins": 0, "draws": 0, "losses": 0, "points": 0}, "coverage": {"status": "partial", "percentage": 0, "label": "Coach profile coverage"}}, request)


def _standings_scope(request: Request) -> dict[str, Any] | None:
    scope = _scope_row(request)
    if scope is not None:
        return scope
    return None


@router.get("/api/v1/standings")
@router.get("/api/v1/group-standings", deprecated=True)
def get_standings_v2(request: Request) -> dict[str, Any]:
    scope = _standings_scope(request)
    if scope is None:
        return _response({"competition": None, "stage": None, "group": None, "selectedRound": None, "currentRound": None, "rounds": [], "rows": []}, request, coverage={"status": "empty", "percentage": 0, "label": "Standings coverage"})
    stage_id = _int_q(request, "stageId")
    group_id = _q(request, "groupId")
    stage = db_client.fetch_one("select stage_key, stage_id, stage_name from mart_v2.dim_stage where edition_key = %s and (%s::bigint is null or stage_id = %s) order by sort_order nulls last, stage_id limit 1;", [scope["edition_key"], stage_id, stage_id])
    round_id = _int_q(request, "roundId")
    round_row = db_client.fetch_one("select round_key, round_id, round_name, starting_at, ending_at from mart_v2.dim_round where edition_key = %s and (%s::bigint is null or round_id = %s) order by starting_at nulls last, round_id limit 1;", [scope["edition_key"], round_id, round_id])
    params = [scope["edition_key"], round_row["round_key"] if round_row else None, group_id]
    standing_rows = db_client.fetch_all(
        """
        select fs.*, t.team_name
        from mart_v2.fact_standing fs
        left join mart_v2.dim_team t on t.team_id = fs.team_id
        where fs.edition_key = %s
          and (%s::text is null or fs.round_key = %s)
          and (%s::text is null or exists (select 1 from mart_v2.dim_group g where g.group_key = %s and g.edition_key = fs.edition_key))
        order by fs.position, t.team_name, fs.team_id;
        """,
        [scope["edition_key"], params[1], params[1], params[2], params[2]],
    )
    rows = [{"position": int(row.get("position") or index), "teamId": str(row["team_id"]), "teamName": row.get("team_name"), "matchesPlayed": int(row.get("games_played") or 0), "wins": int(row.get("wins") or 0), "draws": int(row.get("draws") or 0), "losses": int(row.get("losses") or 0), "goalsFor": int(row.get("goals_for") or 0), "goalsAgainst": int(row.get("goals_against") or 0), "goalDiff": int(row.get("goal_difference") or 0), "points": int(row.get("points") or 0)} for index, row in enumerate(standing_rows, 1)]
    round_payload = {"roundId": str(round_row["round_id"]), "providerRoundId": str(round_row["round_id"]), "roundName": round_row.get("round_name"), "label": round_row.get("round_name") or str(round_row["round_id"]), "startingAt": round_row.get("starting_at"), "endingAt": round_row.get("ending_at"), "isCurrent": True} if round_row else None
    return _response({"competition": {"competitionId": str(get_canonical_competition_by_key(scope["competition_key"]).competition_id) if get_canonical_competition_by_key(scope["competition_key"]) else scope["competition_key"], "competitionKey": scope["competition_key"], "competitionName": scope["competition_name"], "seasonId": _public_season_label(scope["season_label"]), "seasonLabel": _public_season_label(scope["season_label"])}, "stage": {"stageId": str(stage["stage_id"]), "stageName": stage.get("stage_name"), "stageFormat": "group_table" if group_id else "league_table", "expectedTeams": None} if stage else None, "group": {"groupId": group_id, "groupName": None, "groupOrder": None, "expectedTeams": None} if group_id else None, "selectedRound": round_payload, "currentRound": round_payload, "rounds": [round_payload] if round_payload else [], "rows": rows}, request, coverage=build_coverage_from_counts(len(rows), len(rows), "Standings coverage"))


def _ranking_metric(ranking_type: str) -> tuple[str, str, str] | None:
    metrics = {
        "player-goals": ("sum(coalesce(ps.goals, 0))", "player", "goals"),
        "player-assists": ("sum(coalesce(ps.assists, 0))", "player", "assists"),
        "player-shots-total": ("sum(coalesce(ps.total_shots, 0))", "player", "shots"),
        "player-shots-on-target": ("sum(coalesce(ps.shots_on_target, 0))", "player", "shots_on_target"),
        "player-rating": ("avg(ps.rating)", "player", "rating"),
        "player-cards": ("sum(coalesce(ps.yellow_cards, 0) + coalesce(ps.red_cards, 0))", "player", "cards"),
        "team-possession": ("avg(ts.ball_possession)", "team", "possession"),
        "team-pass-accuracy": ("avg(ts.passes_pct)", "team", "pass_accuracy"),
    }
    return metrics.get(ranking_type)


@router.get("/api/v1/rankings/{rankingType}")
def get_ranking_v2(rankingType: str, request: Request) -> dict[str, Any]:
    metric = _ranking_metric(rankingType)
    if metric is None:
        raise AppError("Ranking metric is not available in the canonical v2 layer.", "RANKING_NOT_IMPLEMENTED", 501, {"rankingType": rankingType})
    expression, domain, metric_key = metric
    page = max(_int_q(request, "page", 1) or 1, 1)
    page_size = min(max(_int_q(request, "pageSize", 20) or 20, 1), 100)
    offset = (page - 1) * page_size
    params: list[Any] = []
    clauses = ["f.publication_state = 'published'"]
    competition_key = _competition_key(request)
    if competition_key:
        clauses.append("f.competition_key = %s")
        params.append(competition_key)
    season = _q(request, "seasonLabel") or _q(request, "seasonId")
    candidates = _internal_season_candidates(season)
    if candidates:
        clauses.append("f.edition_key in (select edition_key from mart_v2.dim_edition where season_label = any(%s))")
        params.append(list(candidates))
    if domain == "player":
        query = f"""
        with aggregate as (
          select ps.player_key, max(p.display_name) as entity_name, max(ps.team_id) as team_id,
                 max(t.team_name) as team_name, count(distinct ps.match_id)::int as matches_played,
                 sum(coalesce(ps.minutes_played, 0))::numeric as minutes_played,
                 {expression}::numeric as metric_value
          from mart_v2.fact_match_player_stats ps
          join mart_v2.fact_match f on f.match_id = ps.match_id
          join mart_v2.dim_player p on p.player_key = ps.player_key
          left join mart_v2.dim_team t on t.team_id = ps.team_id
          where {' and '.join(clauses)}
          group by ps.player_key
        ), ranked as (
          select aggregate.*, dense_rank() over(order by metric_value desc nulls last, entity_name, player_key)::int as rank
          from aggregate
        )
        select *, count(*) over()::int as _total_count from ranked
        order by rank, player_key limit %s offset %s;
        """
        rows = db_client.fetch_all(query, [*params, page_size, offset])
    else:
        query = f"""
        with aggregate as (
          select ts.team_id, max(t.team_name) as entity_name, count(distinct ts.match_id)::int as matches_played,
                 {expression}::numeric as metric_value
          from mart_v2.fact_match_team_stats ts
          join mart_v2.fact_match f on f.match_id = ts.match_id
          left join mart_v2.dim_team t on t.team_id = ts.team_id
          where {' and '.join(clauses)}
          group by ts.team_id
        ), ranked as (
          select aggregate.*, dense_rank() over(order by metric_value desc nulls last, entity_name, team_id)::int as rank
          from aggregate
        )
        select *, count(*) over()::int as _total_count from ranked
        order by rank, team_id limit %s offset %s;
        """
        rows = db_client.fetch_all(query, [*params, page_size, offset])
    total = int(rows[0].get("_total_count") or 0) if rows else 0
    normalized = [{"entityId": str(row["player_key"] if domain == "player" else row["team_id"]), "entityName": row.get("entity_name"), "rank": int(row.get("rank") or 0), "metricValue": float(row["metric_value"]) if row.get("metric_value") is not None else None, "matchesPlayed": int(row.get("matches_played") or 0), "minutesPlayed": float(row["minutes_played"]) if row.get("minutes_played") is not None else None, "metricPer90": None, "teamId": str(row["team_id"]) if row.get("team_id") is not None else None, "teamName": row.get("team_name")} for row in rows]
    return _response({"rankingId": rankingType, "metricKey": metric_key, "entity": domain, "scope": {"kind": "filtered" if competition_key or season else "archive", "competitionKey": competition_key, "seasonLabel": _public_season_label(season)}, "rows": normalized, "updatedAt": None, "freshnessClass": _q(request, "freshnessClass", "season"), "sort": {"direction": "desc", "label": "Maior para menor", "serverSide": True}}, request, page=page, page_size=page_size, total=total, coverage=build_coverage_from_counts(len(normalized), min(total, page_size), "Ranking coverage"))


def _analytics_overview(request: Request) -> dict[str, Any]:
    where, params = _match_catalog_where(request)
    row = db_client.fetch_one(
        f"""
        select count(*)::int as total_matches,
               coalesce(sum(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0)), 0)::int as total_goals,
               avg(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0))::numeric as avg_goals,
               count(*) filter (where m.home_goals > m.away_goals)::int as home_wins,
               count(*) filter (where m.home_goals = m.away_goals)::int as draws,
               count(*) filter (where m.away_goals > m.home_goals)::int as away_wins,
               count(distinct m.home_team_id)::int + count(distinct m.away_team_id)::int as team_mentions
        from serving_v2.match_catalog m where {where};
        """,
        params,
    ) or {}
    total = int(row.get("total_matches") or 0)
    return _response({"scope": {"competitionKey": _competition_key(request), "seasonLabel": _public_season_label(_q(request, "seasonLabel") or _q(request, "seasonId"))}, "summary": {"totalMatches": total, "totalGoals": int(row.get("total_goals") or 0), "avgGoalsPerMatch": float(row["avg_goals"]) if row.get("avg_goals") is not None else None, "totalTeams": int(row.get("team_mentions") or 0), "totalCoaches": 0, "totalPlayers": 0, "homeWins": int(row.get("home_wins") or 0), "awayWins": int(row.get("away_wins") or 0), "draws": int(row.get("draws") or 0), "homeWinRate": round(int(row.get("home_wins") or 0) / total * 100, 2) if total else None, "awayWinRate": round(int(row.get("away_wins") or 0) / total * 100, 2) if total else None, "drawRate": round(int(row.get("draws") or 0) / total * 100, 2) if total else None}}, request, coverage=build_coverage_from_counts(total, total, "Analytics overview coverage"))


@router.get("/api/v1/analytics/overview")
def get_analytics_overview(request: Request) -> dict[str, Any]:
    return _analytics_overview(request)


_ANALYTICS_MATCH_METRICS = {
    "matches": "count(distinct m.match_id)::int",
    "goals": "sum(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0))::int",
    "avg_goals": "round(avg(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0)), 4)",
    "home_wins": "count(*) filter (where m.home_goals > m.away_goals)::int",
    "away_wins": "count(*) filter (where m.away_goals > m.home_goals)::int",
    "draws": "count(*) filter (where m.home_goals = m.away_goals)::int",
}
_ANALYTICS_TEAM_METRICS = {
    "matches": "count(distinct tr.match_id)::int",
    "goals": "sum(tr.goals_for)::int",
    "avg_goals": "round(avg(tr.goals_for), 4)",
    "home_wins": "sum((tr.is_home and tr.points = 3)::int)::int",
    "away_wins": "sum((not tr.is_home and tr.points = 3)::int)::int",
    "draws": "sum((tr.points = 1)::int)::int",
    "points": "sum(tr.points)::int",
    "goals_for": "sum(tr.goals_for)::int",
    "goals_against": "sum(tr.goals_against)::int",
    "goal_diff": "(sum(tr.goals_for) - sum(tr.goals_against))::int",
}
_ANALYTICS_METRICS = set(_ANALYTICS_MATCH_METRICS) | {"points", "goals_for", "goals_against", "goal_diff"}
_ANALYTICS_DIMENSIONS = {"round", "team", "venue", "period"}
_ANALYTICS_COMPARISON_TYPES = {"team_vs_team", "season_vs_season", "home_vs_away", "period_vs_period"}
_ANALYTICS_SUPERLATIVE_LABELS = {
    "most_goals_match": "Partida com mais gols",
    "biggest_win": "Maior goleada",
    "best_attack": "Melhor ataque",
    "best_defense": "Melhor defesa",
    "best_goal_diff": "Melhor saldo de gols",
    "most_goals_round": "Rodada com mais gols",
    "highest_avg_goals_round": "Rodada com maior média de gols",
    "best_team_ppg": "Melhor aproveitamento (PPG)",
    "coach_best_ppm": "Técnico melhor PPM",
    "coach_most_matches": "Técnico com mais partidas",
}


def _analytics_period_expr(alias: str, period_type: str) -> str:
    if period_type == "round":
        return f"coalesce({alias}.round_name, 'Sem rodada')"
    return f"to_char({alias}.match_date, 'YYYY-MM')"


def _analytics_team_rows_cte(where: str) -> str:
    return f"""
        with scoped_matches as (
            select m.*
            from serving_v2.match_catalog m
            where {where}
        ), team_rows as (
            select match_id, match_date, competition_key, season_label, round_name,
                   home_team_id as team_id, home_team_name as team_name,
                   coalesce(home_goals, 0) as goals_for,
                   coalesce(away_goals, 0) as goals_against,
                   case when home_goals > away_goals then 3
                        when home_goals = away_goals then 1 else 0 end as points,
                   true as is_home
            from scoped_matches
            union all
            select match_id, match_date, competition_key, season_label, round_name,
                   away_team_id, away_team_name,
                   coalesce(away_goals, 0), coalesce(home_goals, 0),
                   case when away_goals > home_goals then 3
                        when away_goals = home_goals then 1 else 0 end,
                   false
            from scoped_matches
        )
    """


def _analytics_number(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


def _analytics_direction(values: list[float]) -> str | None:
    if len(values) < 3:
        return None
    midpoint = len(values) // 2
    first = sum(values[:midpoint]) / midpoint
    second = sum(values[midpoint:]) / (len(values) - midpoint)
    threshold = max(0.01, abs(first) * 0.02)
    if second - first > threshold:
        return "up"
    if second - first < -threshold:
        return "down"
    return "stable"


def _analytics_error(message: str, code: str, details: dict[str, Any]) -> AppError:
    return AppError(message, code, 400, details)


@router.get("/api/v1/analytics/trends")
def get_analytics_trends(request: Request) -> dict[str, Any]:
    metric = _q(request, "metric")
    period_type = _q(request, "periodType")
    if metric not in _ANALYTICS_METRICS:
        raise _analytics_error("Invalid analytics metric.", "INVALID_METRIC", {"metric": metric})
    if period_type not in {"round", "month"}:
        raise _analytics_error("Invalid analytics period type.", "INVALID_PERIOD_TYPE", {"periodType": period_type})

    where, params = _match_catalog_where(request)
    entity_id = _q(request, "entityId")
    if metric in {"points", "goals_for", "goals_against", "goal_diff"}:
        try:
            entity_value = int(entity_id) if entity_id else None
        except ValueError as exc:
            raise _analytics_error("Invalid entity id.", "INVALID_QUERY_PARAM", {"entityId": entity_id}) from exc
        entity_clause = " and tr.team_id = %s" if entity_value is not None else ""
        query = f"""
            {_analytics_team_rows_cte(where)}
            select {_analytics_period_expr('tr', period_type)} as period,
                   {_analytics_period_expr('tr', period_type)} as period_label,
                   {_ANALYTICS_TEAM_METRICS[metric]} as value,
                   count(distinct tr.match_id)::int as sample_size
            from team_rows tr
            where tr.team_id is not null{entity_clause}
            group by {_analytics_period_expr('tr', period_type)}
            order by {_analytics_period_expr('tr', period_type)};
        """
        rows = db_client.fetch_all(query, [*params, *([entity_value] if entity_value is not None else [])])
    else:
        entity_clause = ""
        if entity_id:
            try:
                entity_value = int(entity_id)
            except ValueError as exc:
                raise _analytics_error("Invalid entity id.", "INVALID_QUERY_PARAM", {"entityId": entity_id}) from exc
            entity_clause = " and (m.home_team_id = %s or m.away_team_id = %s)"
            params.extend([entity_value, entity_value])
        query = f"""
            select {_analytics_period_expr('m', period_type)} as period,
                   {_analytics_period_expr('m', period_type)} as period_label,
                   {_ANALYTICS_MATCH_METRICS[metric]} as value,
                   count(distinct m.match_id)::int as sample_size
            from serving_v2.match_catalog m
            where {where}{entity_clause}
            group by {_analytics_period_expr('m', period_type)}
            order by {_analytics_period_expr('m', period_type)};
        """
        rows = db_client.fetch_all(query, params)

    series = [
        {
            "period": row.get("period"),
            "periodLabel": row.get("period_label"),
            "value": _analytics_number(row.get("value")),
            "sampleSize": int(row.get("sample_size") or 0),
        }
        for row in rows
    ]
    values = [float(item["value"]) for item in series if item["value"] is not None]
    return _response(
        {
            "metric": metric,
            "periodType": period_type,
            "series": series,
            "trendDirection": _analytics_direction(values),
            "minPeriodsRequired": 3,
            "totalPeriods": len(series),
        },
        request,
        coverage=build_coverage_from_counts(len(series), max(len(series), 1), "Trend series coverage"),
    )


@router.get("/api/v1/analytics/olap")
def get_analytics_olap(request: Request) -> dict[str, Any]:
    metric = _q(request, "metric", "matches") or "matches"
    dimension = _q(request, "dimension", "period") or "period"
    if metric not in _ANALYTICS_METRICS:
        raise _analytics_error("Invalid analytics metric.", "INVALID_METRIC", {"metric": metric})
    if dimension not in _ANALYTICS_DIMENSIONS:
        raise _analytics_error("This OLAP dimension is not available in serving_v2.", "INVALID_DIMENSION", {"dimension": dimension})

    where, params = _match_catalog_where(request)
    use_team_rows = dimension in {"team", "venue"} or metric in {"points", "goals_for", "goals_against", "goal_diff"}
    if use_team_rows:
        if dimension == "team":
            key_expr, label_expr, group_expr = "tr.team_id::text", "coalesce(max(tr.team_name), 'Time indisponível')", "tr.team_id"
        elif dimension == "venue":
            key_expr, label_expr, group_expr = "case when tr.is_home then 'home' else 'away' end", "case when tr.is_home then 'Casa' else 'Fora' end", "tr.is_home"
        else:
            key_expr = label_expr = _analytics_period_expr("tr", "month")
            group_expr = key_expr
        metric_expr = _ANALYTICS_TEAM_METRICS.get(metric, "count(distinct tr.match_id)::int")
        query = f"""
            {_analytics_team_rows_cte(where)}
            select {key_expr} as dimension_key, {label_expr} as dimension_label,
                   {metric_expr} as value, count(distinct tr.match_id)::int as sample_size
            from team_rows tr
            where tr.team_id is not null
            group by {group_expr}
            order by dimension_label;
        """
    else:
        key_expr = _analytics_period_expr("m", "round" if dimension == "round" else "month")
        metric_expr = _ANALYTICS_MATCH_METRICS[metric]
        query = f"""
            select {key_expr} as dimension_key, {key_expr} as dimension_label,
                   {metric_expr} as value, count(distinct m.match_id)::int as sample_size
            from serving_v2.match_catalog m
            where {where}
            group by {key_expr}
            order by dimension_label;
        """
    rows = db_client.fetch_all(query, params)
    normalized = [
        {
            "dimensionKey": row.get("dimension_key"),
            "dimensionLabel": row.get("dimension_label"),
            "value": _analytics_number(row.get("value")),
            "sampleSize": int(row.get("sample_size") or 0),
        }
        for row in rows
    ]
    return _response(
        {
            "metric": metric,
            "dimension": dimension,
            "grain": _q(request, "grain", "competition_season") or "competition_season",
            "operation": _q(request, "operation", "slice") or "slice",
            "rows": normalized,
            "total": len(normalized),
            "drillThroughAvailable": False,
        },
        request,
        coverage=build_coverage_from_counts(len(normalized), max(len(normalized), 1), "OLAP coverage"),
    )


def _analytics_team_aggregate(
    request: Request,
    team_id: int,
    *,
    extra_clause: str = "",
    extra_params: list[Any] | None = None,
    home_only: bool | None = None,
) -> dict[str, Any]:
    where, params = _match_catalog_where(request)
    clauses = ["tr.team_id = %s"]
    params.append(team_id)
    if extra_clause:
        clauses.append(extra_clause)
        params.extend(extra_params or [])
    if home_only is not None:
        clauses.append("tr.is_home = %s")
        params.append(home_only)
    row = db_client.fetch_one(
        f"""
            {_analytics_team_rows_cte(where)}
            select max(tr.team_name) as entity_label,
                   count(distinct tr.match_id)::int as matches,
                   sum((tr.points = 3)::int)::int as wins,
                   sum((tr.points = 1)::int)::int as draws,
                   sum((tr.points = 0)::int)::int as losses,
                   sum(tr.points)::int as points,
                   sum(tr.goals_for)::int as goals_for,
                   sum(tr.goals_against)::int as goals_against
            from team_rows tr
            where {' and '.join(clauses)};
        """,
        params,
    ) or {}
    matches = int(row.get("matches") or 0)
    return {
        "id": str(team_id),
        "label": row.get("entity_label") or str(team_id),
        "matches": matches,
        "wins": int(row.get("wins") or 0),
        "draws": int(row.get("draws") or 0),
        "losses": int(row.get("losses") or 0),
        "points": int(row.get("points") or 0),
        "goalsFor": int(row.get("goals_for") or 0),
        "goalsAgainst": int(row.get("goals_against") or 0),
        "goalDiff": int(row.get("goals_for") or 0) - int(row.get("goals_against") or 0),
        "avgGoalsPerMatch": round((int(row.get("goals_for") or 0) + int(row.get("goals_against") or 0)) / matches, 4) if matches else None,
        "pointsPerMatch": round(int(row.get("points") or 0) / matches, 4) if matches else None,
    }


def _analytics_match_aggregate(request: Request, extra_clause: str, extra_params: list[Any]) -> dict[str, Any]:
    where, params = _match_catalog_where(request)
    params.extend(extra_params)
    row = db_client.fetch_one(
        f"""
            select count(*)::int as matches,
                   count(*) filter (where m.home_goals > m.away_goals)::int as wins_home,
                   count(*) filter (where m.away_goals > m.home_goals)::int as wins_away,
                   count(*) filter (where m.home_goals = m.away_goals)::int as draws,
                   coalesce(sum(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0)), 0)::int as goals
            from serving_v2.match_catalog m
            where {where} and {extra_clause};
        """,
        params,
    ) or {}
    matches = int(row.get("matches") or 0)
    home_wins = int(row.get("wins_home") or 0)
    away_wins = int(row.get("wins_away") or 0)
    draws = int(row.get("draws") or 0)
    return {
        "matches": matches,
        "wins": home_wins + away_wins,
        "draws": draws,
        "losses": 0,
        "points": home_wins * 3 + away_wins * 3 + draws * 2,
        "goalsFor": int(row.get("goals") or 0),
        "goalsAgainst": int(row.get("goals") or 0),
        "goalDiff": 0,
        "avgGoalsPerMatch": round(int(row.get("goals") or 0) / matches, 4) if matches else None,
        "pointsPerMatch": round((home_wins * 3 + away_wins * 3 + draws * 2) / matches, 4) if matches else None,
    }


def _analytics_difference(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (a.get(key) - b.get(key)) if a.get(key) is not None and b.get(key) is not None else None
        for key in ("points", "goalDiff", "wins", "draws", "losses")
    }


@router.get("/api/v1/analytics/comparisons")
def get_analytics_comparisons(request: Request) -> dict[str, Any]:
    comparison_type = _q(request, "type")
    entity_a = _q(request, "entityA")
    entity_b = _q(request, "entityB")
    if comparison_type not in _ANALYTICS_COMPARISON_TYPES:
        raise _analytics_error("Invalid comparison type.", "INVALID_COMPARISON_TYPE", {"type": comparison_type})
    if not entity_a or not entity_b:
        raise _analytics_error("Both comparison entities are required.", "INVALID_QUERY_PARAM", {"missing": [name for name, value in (("entityA", entity_a), ("entityB", entity_b)) if not value]})

    if comparison_type == "team_vs_team":
        try:
            a_id, b_id = int(entity_a), int(entity_b)
        except ValueError as exc:
            raise _analytics_error("Team comparison ids must be integers.", "INVALID_QUERY_PARAM", {}) from exc
        a = _analytics_team_aggregate(request, a_id)
        b = _analytics_team_aggregate(request, b_id)
    elif comparison_type == "home_vs_away":
        try:
            team_id = int(entity_a)
        except ValueError as exc:
            raise _analytics_error("Team comparison id must be an integer.", "INVALID_QUERY_PARAM", {}) from exc
        a = _analytics_team_aggregate(request, team_id, home_only=True)
        b = _analytics_team_aggregate(request, team_id, home_only=False)
        a["label"], b["label"] = "Casa", "Fora"
    elif comparison_type == "season_vs_season":
        a = _analytics_match_aggregate(request, "m.season_label = any(%s)", [list(_internal_season_candidates(entity_a))])
        b = _analytics_match_aggregate(request, "m.season_label = any(%s)", [list(_internal_season_candidates(entity_b))])
        a.update({"id": entity_a, "label": f"Temporada {_public_season_label(entity_a)}"})
        b.update({"id": entity_b, "label": f"Temporada {_public_season_label(entity_b)}"})
    else:
        try:
            team_id = int(entity_a)
        except ValueError as exc:
            raise _analytics_error("Team comparison id must be an integer.", "INVALID_QUERY_PARAM", {}) from exc
        where, params = _match_catalog_where(request)
        midpoint = db_client.fetch_one(
            f"select percentile_disc(0.5) within group (order by m.match_date) as midpoint from serving_v2.match_catalog m where {where};",
            params,
        ) or {}
        cutoff = midpoint.get("midpoint")
        a = _analytics_team_aggregate(request, team_id, extra_clause="tr.match_date <= %s", extra_params=[cutoff])
        b = _analytics_team_aggregate(request, team_id, extra_clause="tr.match_date > %s", extra_params=[cutoff])
        a["label"], b["label"] = "1º período", "2º período"

    for item, fallback in ((a, entity_a), (b, entity_b)):
        item.setdefault("id", fallback)
        item.setdefault("label", fallback)
    combined = int(a.get("matches") or 0) + int(b.get("matches") or 0)
    return _response(
        {
            "type": comparison_type,
            "entityA": a,
            "entityB": b,
            "difference": _analytics_difference(a, b),
            "coverage": {
                "entityA": build_coverage_from_counts(int(a.get("matches") or 0), max(int(a.get("matches") or 0), 1), "Entity A match coverage"),
                "entityB": build_coverage_from_counts(int(b.get("matches") or 0), max(int(b.get("matches") or 0), 1), "Entity B match coverage"),
            },
        },
        request,
        coverage=build_coverage_from_counts(combined, max(combined, 1), "Combined match coverage"),
    )


@router.get("/api/v1/analytics/superlatives")
def get_analytics_superlatives(request: Request) -> dict[str, Any]:
    category = _q(request, "category", "most_goals_match") or "most_goals_match"
    limit = min(max(_int_q(request, "limit", 10) or 10, 1), 50)
    if category not in _ANALYTICS_SUPERLATIVE_LABELS:
        raise _analytics_error("Invalid superlative category.", "INVALID_SUPERLATIVE_CATEGORY", {"category": category})
    if category in {"coach_best_ppm", "coach_most_matches"}:
        return _response(
            {"category": category, "categoryLabel": _ANALYTICS_SUPERLATIVE_LABELS[category], "limit": limit, "records": []},
            request,
            coverage={"status": "not_available", "percentage": None, "sampleSize": 0, "expectedSize": 1, "label": "Coach analytics coverage", "details": "Coach assignment is not part of the serving_v2 contract."},
        )

    where, params = _match_catalog_where(request)
    if category in {"most_goals_match", "biggest_win"}:
        value = "(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0))" if category == "most_goals_match" else "abs(coalesce(m.home_goals, 0) - coalesce(m.away_goals, 0))"
        rows = db_client.fetch_all(
            f"""
                select m.match_id::text as entity_id,
                       concat(m.home_team_name, ' ', coalesce(m.home_goals, 0), 'x', coalesce(m.away_goals, 0), ' ', m.away_team_name) as entity_label,
                       {value} as value, concat(m.competition_key, '/', m.season_label) as scope,
                       count(*) over()::int as sample_size
                from serving_v2.match_catalog m
                where {where}
                order by value desc, m.match_id
                limit %s;
            """,
            [*params, limit],
        )
    elif category in {"most_goals_round", "highest_avg_goals_round"}:
        aggregate = "sum(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0))" if category == "most_goals_round" else "round(avg(coalesce(m.home_goals, 0) + coalesce(m.away_goals, 0)), 4)"
        rows = db_client.fetch_all(
            f"""
                select coalesce(m.round_name, 'Sem rodada') as entity_id,
                       coalesce(m.round_name, 'Sem rodada') as entity_label,
                       {aggregate} as value, concat(m.competition_key, '/', m.season_label) as scope,
                       count(distinct m.match_id)::int as sample_size
                from serving_v2.match_catalog m
                where {where}
                group by m.round_name, m.competition_key, m.season_label
                order by value desc, entity_id
                limit %s;
            """,
            [*params, limit],
        )
    else:
        metric_expr = {"best_attack": "sum(tr.goals_for)", "best_defense": "sum(tr.goals_against)", "best_goal_diff": "sum(tr.goals_for) - sum(tr.goals_against)", "best_team_ppg": "round(sum(tr.points)::numeric / nullif(count(distinct tr.match_id), 0), 4)"}[category]
        order = "asc" if category == "best_defense" else "desc"
        rows = db_client.fetch_all(
            f"""
                {_analytics_team_rows_cte(where)}
                select tr.team_id::text as entity_id, max(tr.team_name) as entity_label,
                       {metric_expr} as value, 'published archive' as scope,
                       count(distinct tr.match_id)::int as sample_size
                from team_rows tr
                where tr.team_id is not null
                group by tr.team_id
                order by value {order} nulls last, entity_id
                limit %s;
            """,
            [*params, limit],
        )
    records = [
        {"position": index, "entityId": row.get("entity_id"), "entityLabel": row.get("entity_label"), "value": _analytics_number(row.get("value")), "scope": row.get("scope"), "sampleSize": int(row.get("sample_size") or 0), "tiebreaker": None}
        for index, row in enumerate(rows, 1)
    ]
    return _response(
        {"category": category, "categoryLabel": _ANALYTICS_SUPERLATIVE_LABELS[category], "limit": limit, "records": records},
        request,
        coverage=build_coverage_from_counts(len(records), max(len(records), 1), "Superlative category coverage"),
    )


@router.get("/api/v1/analytics/coverage")
def get_analytics_coverage(request: Request) -> dict[str, Any]:
    where, params = _match_catalog_where(request)
    row = db_client.fetch_one(
        f"""
            with scope as (
                select m.match_id
                from serving_v2.match_catalog m
                where {where}
            )
            select count(*)::int as total_matches,
                   count(*) filter (where exists (select 1 from mart_v2.fact_match_event e where e.match_id = s.match_id))::int as matches_with_events,
                   count(*) filter (where exists (select 1 from mart_v2.fact_lineup l where l.match_id = s.match_id))::int as matches_with_lineups,
                   count(*) filter (where exists (select 1 from mart_v2.fact_match_player_stats p where p.match_id = s.match_id))::int as matches_with_player_stats,
                   count(*) filter (where exists (select 1 from mart_v2.fact_match_team_stats t where t.match_id = s.match_id))::int as matches_with_team_stats
            from scope s;
        """,
        params,
    ) or {}
    total = int(row.get("total_matches") or 0)

    def metric(count: int) -> dict[str, Any]:
        percentage = round(count / total * 100, 2) if total else None
        status = "complete" if percentage is not None and percentage >= 95 else "partial" if percentage else "not_available"
        return {"count": count, "percentage": percentage, "status": status}

    metrics = {
        "scores": metric(total),
        "events": metric(int(row.get("matches_with_events") or 0)),
        "lineups": metric(int(row.get("matches_with_lineups") or 0)),
        "playerStats": metric(int(row.get("matches_with_player_stats") or 0)),
        "teamStats": metric(int(row.get("matches_with_team_stats") or 0)),
        "coachAssignment": {"count": 0, "percentage": None, "status": "not_available"},
    }
    return _response(
        {
            "scope": {"competitionKey": _competition_key(request), "seasonLabel": _public_season_label(_q(request, "seasonLabel") or _q(request, "seasonId"))},
            "totalMatches": total,
            "metrics": metrics,
            "hiddenMetrics": [
                {"metric": "xg", "reason": "Advanced statistics are only available where the source provides them."},
                {"metric": "passes", "reason": "Advanced statistics are only available where the source provides them."},
                {"metric": "rating", "reason": "Player rating coverage is source-dependent."},
            ],
            "enabledMetrics": ["matches", "goals", "avg_goals", "home_wins", "away_wins", "draws"],
        },
        request,
        coverage=build_coverage_from_counts(total, total, "Overall coverage report"),
    )


@router.get("/api/v1/market/transfers")
def get_market_transfers_v2(request: Request) -> dict[str, Any]:
    page = max(_int_q(request, "page", 1) or 1, 1)
    page_size = min(max(_int_q(request, "pageSize", 24) or 24, 1), 100)
    offset = (page - 1) * page_size
    params: list[Any] = []
    clauses = ["1 = 1"]
    search = _q(request, "search")
    if search:
        clauses.append("(p.display_name ilike %s or ft.team_name ilike %s or tt.team_name ilike %s)")
        pattern = f"%{search}%"
        params.extend([pattern, pattern, pattern])
    date_start = _q(request, "dateStart") or _q(request, "dateRangeStart")
    date_end = _q(request, "dateEnd") or _q(request, "dateRangeEnd")
    if date_start:
        clauses.append("tr.transfer_date >= %s")
        params.append(date_start)
    if date_end:
        clauses.append("tr.transfer_date <= %s")
        params.append(date_end)
    rows = db_client.fetch_all(
        f"""
        select tr.transfer_key, tr.player_key, p.display_name as player_name,
               tr.from_team_id, ft.team_name as from_team_name,
               tr.to_team_id, tt.team_name as to_team_name, tr.transfer_date,
               tr.transfer_type, tr.fee_text, tr.source_system,
               count(*) over()::int as _total_count
        from mart_v2.fact_transfer tr
        left join mart_v2.dim_player p on p.player_key = tr.player_key
        left join mart_v2.dim_team ft on ft.team_id = tr.from_team_id
        left join mart_v2.dim_team tt on tt.team_id = tr.to_team_id
        where {' and '.join(clauses)}
        order by tr.transfer_date desc nulls last, tr.transfer_key
        limit %s offset %s;
        """,
        [*params, page_size, offset],
    )
    total = int(rows[0].get("_total_count") or 0) if rows else 0
    items = [{"transferId": row["transfer_key"], "source": row.get("source_system"), "playerId": row.get("player_key"), "playerName": row.get("player_name") or "Nome indisponível", "fromTeamId": str(row["from_team_id"]) if row.get("from_team_id") is not None else None, "fromTeamName": row.get("from_team_name"), "toTeamId": str(row["to_team_id"]) if row.get("to_team_id") is not None else None, "toTeamName": row.get("to_team_name"), "transferDate": row.get("transfer_date"), "completed": True, "careerEnded": False, "typeId": None, "typeName": row.get("transfer_type"), "movementKind": "transfer", "amount": row.get("fee_text"), "amountValue": None, "currency": None} for row in rows]
    return _response({"items": items}, request, page=page, page_size=page_size, total=total, coverage=build_coverage_from_counts(len(items), min(total, page_size), "Market transfers coverage"))


def _competition_scope_payload(scope: dict[str, Any]) -> dict[str, Any]:
    canonical = get_canonical_competition_by_key(scope.get("competition_key"))
    return {"competitionId": str(canonical.competition_id) if canonical else scope.get("competition_key"), "competitionKey": scope.get("competition_key"), "competitionName": scope.get("competition_name"), "seasonId": _public_season_label(scope.get("season_label")), "seasonLabel": _public_season_label(scope.get("season_label")), "formatFamily": "canonical_v2", "seasonFormatCode": "canonical_v2", "participantScope": "published_matches", "groupRankingRuleCode": None, "tieRuleCode": None}


@router.get("/api/v1/competition-historical-stats")
def get_competition_historical_stats_v2(request: Request) -> dict[str, Any]:
    key = _competition_key(request)
    as_of_year = _int_q(request, "asOfYear", 2025) or 2025
    if key is None:
        raise AppError("'competitionKey' is required.", "INVALID_QUERY_PARAM", 400, {"missing": ["competitionKey"]})
    return _response({"champions": {"items": [], "source": "mart_v2_not_curated", "asOfYear": as_of_year}, "scorers": {"items": [], "source": "mart_v2_not_curated", "asOfYear": as_of_year}}, request)


@router.get("/api/v1/ties")
def get_ties_v2(request: Request) -> dict[str, Any]:
    scope = _scope_row(request)
    if scope is None:
        return _response({"competition": None, "stage": None, "ties": []}, request)
    params: list[Any] = [scope["edition_key"]]
    clauses = ["t.edition_key = %s"]
    stage_id = _int_q(request, "stageId")
    if stage_id is not None:
        clauses.append("exists (select 1 from mart_v2.dim_stage s where s.stage_key = t.stage_key and s.stage_id = %s)")
        params.append(stage_id)
    rows = db_client.fetch_all(
        f"""
        select t.*, ht.team_name as home_team_name, at.team_name as away_team_name,
               wt.team_name as winner_team_name
        from mart_v2.dim_tie t
        left join mart_v2.dim_team ht on ht.team_id = t.home_team_id
        left join mart_v2.dim_team at on at.team_id = t.away_team_id
        left join mart_v2.dim_team wt on wt.team_id = t.winner_team_id
        where {' and '.join(clauses)} order by t.tie_order, t.tie_key;
        """,
        params,
    )
    stage = db_client.fetch_one("select stage_id, stage_name, stage_key, sort_order from mart_v2.dim_stage where edition_key = %s order by sort_order nulls last, stage_id limit 1;", [scope["edition_key"]])
    return _response({"competition": _competition_scope_payload(scope), "stage": {"stageId": str(stage["stage_id"]), "stageName": stage.get("stage_name"), "stageCode": stage.get("stage_key"), "stageFormat": "knockout", "stageOrder": stage.get("sort_order"), "isCurrent": False} if stage else None, "ties": [{"tieId": row["tie_key"], "tieOrder": int(row.get("tie_order") or 0), "homeTeamId": str(row["home_team_id"]) if row.get("home_team_id") is not None else None, "homeTeamName": row.get("home_team_name"), "awayTeamId": str(row["away_team_id"]) if row.get("away_team_id") is not None else None, "awayTeamName": row.get("away_team_name"), "matchCount": int(row.get("match_count") or 0), "firstLegAt": row.get("first_leg_at"), "lastLegAt": row.get("last_leg_at"), "homeGoals": int(row.get("home_side_goals") or 0), "awayGoals": int(row.get("away_side_goals") or 0), "winnerTeamId": str(row["winner_team_id"]) if row.get("winner_team_id") is not None else None, "winnerTeamName": row.get("winner_team_name"), "resolutionType": row.get("resolution_type"), "hasExtraTimeMatch": bool(row.get("has_extra_time_match")), "hasPenaltiesMatch": bool(row.get("has_penalties_match")), "nextStageName": row.get("next_stage_name")} for row in rows]}, request)


def _team_journey(request: Request) -> dict[str, Any]:
    key = _competition_key(request)
    team_id = _int_q(request, "teamId")
    if key is None or team_id is None:
        raise AppError("'competitionKey' and 'teamId' are required.", "INVALID_QUERY_PARAM", 400, {"missing": [name for name, value in (("competitionKey", key), ("teamId", team_id)) if value is None]})
    rows = db_client.fetch_all(
        """
        select e.season_label, f.match_date, f.home_team_id, f.away_team_id,
               f.home_goals, f.away_goals, f.stage_key, s.stage_id, s.stage_name,
               s.sort_order
        from mart_v2.fact_match f
        join mart_v2.dim_edition e on e.edition_key = f.edition_key
        left join mart_v2.dim_stage s on s.stage_key = f.stage_key
        where f.publication_state = 'published' and f.competition_key = %s
          and (f.home_team_id = %s or f.away_team_id = %s)
        order by e.season_start_date desc nulls last, f.match_date;
        """,
        [key, team_id, team_id],
    )
    by_season: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_season[str(row["season_label"])].append(row)
    seasons = []
    team = db_client.fetch_one("select team_name from mart_v2.dim_team where team_id = %s;", [team_id]) or {}
    competition = db_client.fetch_one("select competition_name from mart_v2.dim_competition where competition_key = %s;", [key]) or {}
    for season, season_rows in by_season.items():
        wins = draws = losses = goals_for = goals_against = 0
        stages: dict[str, dict[str, Any]] = {}
        for row in season_rows:
            home = int(row["home_team_id"]) == team_id
            gf = int(row.get("home_goals") or 0) if home else int(row.get("away_goals") or 0)
            ga = int(row.get("away_goals") or 0) if home else int(row.get("home_goals") or 0)
            goals_for += gf
            goals_against += ga
            if gf > ga: wins += 1
            elif gf == ga: draws += 1
            else: losses += 1
            stage_key = str(row.get("stage_key") or "unknown")
            item = stages.setdefault(stage_key, {"stageId": str(row.get("stage_id") or stage_key), "stageName": row.get("stage_name"), "stageFormat": "knockout", "stageOrder": row.get("sort_order"), "matchesPlayed": 0, "wins": 0, "draws": 0, "losses": 0, "goalsFor": 0, "goalsAgainst": 0, "progressionType": None, "tieOutcome": None, "sourcePosition": None, "groupId": None, "groupName": None, "tieCount": 0, "tiesWon": 0, "tiesLost": 0, "stageResult": "available"})
            item["matchesPlayed"] += 1
            item["goalsFor"] += gf
            item["goalsAgainst"] += ga
            if gf > ga: item["wins"] += 1
            elif gf == ga: item["draws"] += 1
            else: item["losses"] += 1
        seasons.append({"seasonLabel": _public_season_label(season), "formatFamily": "canonical_v2", "seasonFormatCode": "canonical_v2", "summary": {"matchesPlayed": len(season_rows), "wins": wins, "draws": draws, "losses": losses, "goalsFor": goals_for, "goalsAgainst": goals_against, "finalOutcome": "available"}, "stages": list(stages.values())})
    return _response({"competition": {"competitionKey": key, "competitionName": competition.get("competition_name")}, "team": {"teamId": str(team_id), "teamName": team.get("team_name")}, "seasons": seasons, "updatedAt": None}, request)


@router.get("/api/v1/team-journey-history")
def get_team_journey_history(request: Request) -> dict[str, Any]:
    return _team_journey(request)


@router.get("/api/v1/team-progression", deprecated=True)
def get_team_progression(request: Request) -> dict[str, Any]:
    return _team_journey(request)


def _world_cup_editions() -> list[dict[str, Any]]:
    return db_client.fetch_all(
        """
        select edition_key, season_label, published_match_count, first_match_date,
               last_match_date, href
        from serving_v2.edition_catalog
        where competition_key = 'fifa_world_cup_mens' and is_selectable
        order by season_start_date desc nulls last, season_label desc;
        """
    )


def _world_cup_match_rows(season: str | None = None) -> list[dict[str, Any]]:
    params: list[Any] = []
    clauses = ["m.competition_key = 'fifa_world_cup_mens'"]
    candidates = _internal_season_candidates(season)
    if candidates:
        clauses.append("e.season_label = any(%s)")
        params.append(list(candidates))
    return db_client.fetch_all(
        f"""
        select m.*, f.stage_key, f.round_key, f.group_key, f.venue_name,
               e.season_label, c.competition_name,
               ht.team_name as home_team_name, at.team_name as away_team_name
        from serving_v2.match_catalog m
        join mart_v2.fact_match f on f.match_id = m.match_id and f.publication_state = 'published'
        join mart_v2.dim_edition e on e.edition_key = m.edition_key
        join mart_v2.dim_competition c on c.competition_key = m.competition_key
        join mart_v2.dim_team ht on ht.team_id = m.home_team_id
        join mart_v2.dim_team at on at.team_id = m.away_team_id
        where {' and '.join(clauses)}
        order by e.season_start_date desc, m.match_date, m.match_id;
        """,
        params,
    )


def _world_cup_team_ref(team_id: Any, name: Any) -> dict[str, str] | None:
    if team_id is None:
        return None
    team_id_text = str(team_id)
    team_name = name or "Seleção indisponível"
    return {
        "teamId": team_id_text,
        "teamName": team_name,
        "identity": {
            "entityType": "team",
            "competitionKey": "fifa_world_cup_mens",
            "canonicalId": team_id_text,
            "displayName": team_name,
            "sourceId": team_id_text,
            "sourceSystem": "mart_v2",
            "confidence": "confirmed",
            "editorialStatus": "canonical",
        },
    }


def _world_cup_competition_ref() -> dict[str, Any]:
    return {
        "competitionKey": "fifa_world_cup_mens",
        "competitionName": "Copa do Mundo FIFA",
        "identity": {
            "entityType": "competition",
            "competitionKey": "fifa_world_cup_mens",
            "canonicalId": "fifa_world_cup_mens",
            "displayName": "Copa do Mundo FIFA",
            "sourceId": "0",
            "sourceSystem": "mart_v2",
            "confidence": "confirmed",
            "editorialStatus": "canonical",
        },
    }


def _world_cup_stage_rows(edition_key: str) -> list[dict[str, Any]]:
    return db_client.fetch_all(
        """
        select stage_key, stage_name, stage_id, sort_order
        from mart_v2.dim_stage
        where edition_key = %s
        order by case lower(coalesce(stage_name, ''))
                   when 'group stage' then 1
                   when 'round of 16' then 2
                   when 'quarter-finals' then 3
                   when 'semi-finals' then 4
                   when 'third place' then 5
                   when 'final' then 6
                   else 99 end,
                 sort_order nulls last, stage_id;
        """,
        [edition_key],
    )


def _world_cup_group_stages(edition_key: str) -> list[dict[str, Any]]:
    stage_rows = [
        row for row in _world_cup_stage_rows(edition_key)
        if str(row.get("stage_name") or "").lower() == "group stage"
    ]
    if not stage_rows:
        return []

    stage_keys = [row["stage_key"] for row in stage_rows]
    group_rows = db_client.fetch_all(
        """
        select group_key, stage_key, group_name
        from mart_v2.dim_group
        where edition_key = %s and stage_key = any(%s)
        order by group_name, group_key;
        """,
        [edition_key, stage_keys],
    )
    standing_rows = db_client.fetch_all(
        """
        select r.stage_key, r.round_name, g.group_key, g.group_name,
               fs.position, fs.team_id, t.team_name, fs.games_played,
               fs.wins, fs.draws, fs.losses, fs.goals_for, fs.goals_against,
               fs.goal_difference, fs.points
        from mart_v2.fact_standing fs
        join mart_v2.dim_round r on r.round_key = fs.round_key
        left join mart_v2.dim_group g
          on g.edition_key = fs.edition_key
         and g.stage_key = r.stage_key
         and lower(g.group_name) = lower(r.round_name)
        left join mart_v2.dim_team t on t.team_id = fs.team_id
        where fs.edition_key = %s and r.stage_key = any(%s)
        order by r.round_name, fs.position, t.team_name, fs.team_id;
        """,
        [edition_key, stage_keys],
    )
    groups_by_stage: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in group_rows:
        group_key = str(row["group_key"])
        groups_by_stage[str(row["stage_key"])][group_key] = {
            "groupKey": group_key,
            "groupLabel": row.get("group_name") or group_key,
            "rows": [],
        }

    for row in standing_rows:
        stage_key = str(row["stage_key"])
        group_key = str(row.get("group_key") or row.get("group_name") or "group")
        group = groups_by_stage.setdefault(stage_key, {}).setdefault(
            group_key,
            {"groupKey": group_key, "groupLabel": row.get("group_name") or row.get("round_name") or group_key, "rows": []},
        )
        team_ref = _world_cup_team_ref(row.get("team_id"), row.get("team_name"))
        group["rows"].append(
            {
                "position": int(row.get("position") or len(group["rows"]) + 1),
                "teamId": team_ref["teamId"] if team_ref else None,
                "teamName": team_ref["teamName"] if team_ref else None,
                "identity": team_ref.get("identity") if team_ref else None,
                "matchesPlayed": int(row.get("games_played") or 0),
                "wins": int(row.get("wins") or 0),
                "draws": int(row.get("draws") or 0),
                "losses": int(row.get("losses") or 0),
                "goalsFor": int(row.get("goals_for") or 0),
                "goalsAgainst": int(row.get("goals_against") or 0),
                "goalDiff": int(row.get("goal_difference") or 0),
                "points": int(row.get("points") or 0),
                "advanced": int(row.get("position") or 99) <= 2,
            }
        )

    return [
        {
            "stageKey": str(stage["stage_key"]),
            "stageLabel": stage.get("stage_name") or "Group Stage",
            "groups": list(groups_by_stage.get(str(stage["stage_key"]), {}).values()),
        }
        for stage in stage_rows
    ]


def _world_cup_scorers(edition_key: str | None = None) -> list[dict[str, Any]]:
    params: list[Any] = []
    clause = ""
    if edition_key:
        clause = "where g.edition_key = %s"
        params.append(edition_key)
    rows = db_client.fetch_all(
        f"""
        select g.player_key, max(p.display_name) as player_name,
               g.team_id, max(t.team_name) as team_name, count(*)::int as goals
        from mart_v2.fact_world_cup_goal g
        left join mart_v2.dim_player p on p.player_key = g.player_key
        left join mart_v2.dim_team t on t.team_id = g.team_id
        {clause}
        group by g.player_key, g.team_id
        order by goals desc, player_name nulls last, g.player_key
        limit 50;
        """,
        params,
    )
    items = []
    for index, row in enumerate(rows, 1):
        team_ref = _world_cup_team_ref(row.get("team_id"), row.get("team_name"))
        player_id = str(row["player_key"]) if row.get("player_key") is not None else None
        items.append(
            {
                "rank": index,
                "playerId": player_id,
                "identity": {"entityType": "player", "competitionKey": "fifa_world_cup_mens", "canonicalId": player_id, "displayName": row.get("player_name"), "sourceSystem": "mart_v2", "confidence": "confirmed", "editorialStatus": "canonical"} if player_id else None,
                "imageAssetId": None,
                "playerName": row.get("player_name"),
                "profileUrl": f"/players/{player_id}" if player_id else None,
                "teamId": team_ref["teamId"] if team_ref else None,
                "teamName": team_ref["teamName"] if team_ref else None,
                "teamIdentity": team_ref.get("identity") if team_ref else None,
                "goals": int(row.get("goals") or 0),
            }
        )
    return items


def _world_cup_knockout_rounds(edition_key: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stage_rows = [
        row for row in _world_cup_stage_rows(edition_key)
        if str(row.get("stage_name") or "").lower() != "group stage"
    ]
    rows_by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("stage_name") or "").lower() != "group stage":
            rows_by_stage[str(row.get("stage_key") or row.get("stage_name"))].append(row)

    result = []
    for stage in stage_rows:
        stage_key = str(stage["stage_key"])
        ties = []
        for row in rows_by_stage.get(stage_key, []):
            home_ref = _world_cup_team_ref(row.get("home_team_id"), row.get("home_team_name"))
            away_ref = _world_cup_team_ref(row.get("away_team_id"), row.get("away_team_name"))
            home_score = int(row["home_goals"]) if row.get("home_goals") is not None else None
            away_score = int(row["away_goals"]) if row.get("away_goals") is not None else None
            winner = home_ref if home_score is not None and away_score is not None and home_score > away_score else away_ref if home_score is not None and away_score is not None and away_score > home_score else None
            runner_up = away_ref if winner is home_ref else home_ref if winner is away_ref else None
            match_id = str(row["match_id"])
            ties.append(
                {
                    "tieKey": f"match:{match_id}",
                    "roundKey": str(row.get("round_key") or stage_key),
                    "roundLabel": row.get("round_name") or stage.get("stage_name") or "Fase eliminatória",
                    "winner": winner,
                    "runnerUp": runner_up,
                    "resolutionType": None if winner else "unresolved",
                    "resolutionNote": "O placar regulamentar não identifica o vencedor do desempate." if winner is None else None,
                    "matches": [
                        {
                            "fixtureId": match_id,
                            "kickoffAt": str(row.get("match_date")) if row.get("match_date") is not None else None,
                            "venueName": row.get("venue_name"),
                            "homeTeam": home_ref,
                            "awayTeam": away_ref,
                            "homeScore": home_score,
                            "awayScore": away_score,
                            "shootout": None,
                            "isReplay": False,
                        }
                    ],
                }
            )
        result.append({"roundKey": stage_key, "roundLabel": stage.get("stage_name") or "Fase eliminatória", "ties": ties})
    return result


def _world_cup_format_flags(stage_names: set[str]) -> dict[str, bool]:
    normalized = {name.strip().lower() for name in stage_names}
    return {
        "final": "final" in normalized,
        "final_round": False,
        "group_stage": "group stage" in normalized,
        "round_of_16": "round of 16" in normalized,
        "semi_finals": "semi-finals" in normalized,
        "quarter_finals": "quarter-finals" in normalized,
        "third_place_match": "third place" in normalized,
        "second_group_stage": False,
    }


def _world_cup_navigation(season_label: str, editions: list[dict[str, Any]]) -> dict[str, Any]:
    labels = [_public_season_label(row.get("season_label")) for row in editions]
    current = _public_season_label(season_label)
    try:
        index = labels.index(current)
    except ValueError:
        index = -1

    def item(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        label = _public_season_label(row.get("season_label")) or ""
        year = int(label[:4]) if label[:4].isdigit() else 0
        return {"seasonLabel": label, "year": year, "editionName": f"Copa do Mundo FIFA {label}"}

    previous = editions[index + 1] if index >= 0 and index + 1 < len(editions) else None
    next_edition = editions[index - 1] if index > 0 else None
    return {"previousEdition": item(previous), "nextEdition": item(next_edition)}


def _world_cup_edition_payload(
    season_label: str,
    rows: list[dict[str, Any]],
    editions: list[dict[str, Any]],
) -> dict[str, Any]:
    edition_key = str(rows[0]["edition_key"])
    label = _public_season_label(rows[0].get("season_label")) or season_label
    stage_names = {str(row.get("stage_name") or "") for row in rows}
    final_resolution = _world_cup_final_resolution(rows)
    final_row = final_resolution["row"]

    scorers = _world_cup_scorers(edition_key)
    group_stages = _world_cup_group_stages(edition_key)
    matches_count = len(rows)
    team_ids = {int(row[key]) for row in rows for key in ("home_team_id", "away_team_id") if row.get(key) is not None}
    edition = {
        "seasonLabel": label,
        "year": int(label[:4]) if label[:4].isdigit() else None,
        "editionName": f"Copa do Mundo FIFA {label}",
        "hostCountry": None,
        "hostCountryTeam": None,
        "teamsCount": len(team_ids),
        "matchesCount": matches_count,
        "champion": final_resolution["champion"],
        "runnerUp": final_resolution["runnerUp"],
        "finalVenue": final_row.get("venue_name") if final_row else None,
        "resolutionType": final_resolution["resolutionType"],
        "coverage": {"status": "complete", "percentage": 100, "label": "Cobertura completa"},
        "coverageNote": final_resolution["resolutionNote"] if final_resolution["resolutionNote"] else None,
        "formatFlags": _world_cup_format_flags(stage_names),
        "topScorer": scorers[0] if scorers else None,
        "coverageNotes": [],
    }
    return {
        "competition": _world_cup_competition_ref(),
        "edition": edition,
        "navigation": _world_cup_navigation(label, editions),
        "groupStages": group_stages,
        "knockoutRounds": _world_cup_knockout_rounds(edition_key, rows),
        "scorers": scorers,
        # Keep the compact legacy fields available while the frontend consumes
        # the richer stage-aware contract above.
        "matches": [_match_item(row) for row in rows],
        "teams": list(
            {
                team_ref["teamId"]: team_ref
                for row in rows
                for team_ref in (
                    _world_cup_team_ref(row.get("home_team_id"), row.get("home_team_name")),
                    _world_cup_team_ref(row.get("away_team_id"), row.get("away_team_name")),
                )
                if team_ref is not None
            }.values()
        ),
        "groups": [
            group
            for stage in group_stages
            for group in stage["groups"]
        ],
        "updatedAt": None,
    }


@router.get("/api/v1/world-cup/hub")
def get_world_cup_hub_v2(request: Request) -> dict[str, Any]:
    editions = _world_cup_editions()
    matches = _world_cup_match_rows()
    matches_by_edition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for match in matches:
        matches_by_edition[str(match["edition_key"])].append(match)
    payload_editions = []
    champions: set[str] = set()
    for row in editions:
        edition_rows = matches_by_edition.get(str(row["edition_key"]), [])
        final_resolution = _world_cup_final_resolution(edition_rows)
        final_row = final_resolution["row"]
        champion = final_resolution["champion"]
        if champion:
            champions.add(champion["teamId"])
        stage_names = {str(match.get("stage_name") or "") for match in edition_rows}
        label = _public_season_label(row["season_label"])
        coverage_note = final_resolution["resolutionNote"]
        if final_row is None:
            coverage_note = "Nenhuma partida foi classificada como final na mart v2 para esta edição."
        payload_editions.append(
            {
                "seasonLabel": label,
                "year": int(str(row["season_label"])[:4]) if str(row["season_label"])[:4].isdigit() else None,
                "editionName": f"Copa do Mundo FIFA {label}",
                "hostCountry": None,
                "hostCountryTeam": None,
                "teamsCount": len({int(match[key]) for match in edition_rows for key in ("home_team_id", "away_team_id") if match.get(key) is not None}),
                "matchesCount": len(edition_rows),
                "champion": champion,
                "runnerUp": final_resolution["runnerUp"],
                "finalVenue": final_row.get("venue_name") if final_row else None,
                "resolutionType": final_resolution["resolutionType"],
                "coverage": {"status": "complete", "percentage": 100, "label": "Cobertura completa"},
                "coverageNote": coverage_note,
                "formatFlags": _world_cup_format_flags(stage_names),
            }
        )
    scorers = _world_cup_scorers()
    summary = {
        "editionsCount": len(payload_editions),
        "matchesCount": len(matches),
        "distinctChampionsCount": len(champions),
        "topScorer": scorers[0] if scorers else None,
    }
    return _response({"summary": summary, "editions": payload_editions, "competition": _world_cup_competition_ref(), "updatedAt": None}, request, coverage={"status": "complete" if matches else "empty", "percentage": 100 if matches else 0, "label": "World Cup coverage"})


@router.get("/api/v1/world-cup/editions/{season_label}")
def get_world_cup_edition_v2(season_label: str, request: Request) -> dict[str, Any]:
    rows = _world_cup_match_rows(season_label)
    if not rows:
        raise AppError("World Cup edition not found.", "EDITION_NOT_FOUND", 404, {"seasonLabel": season_label})
    payload = _world_cup_edition_payload(season_label, rows, _world_cup_editions())
    return _response(payload, request, coverage={"status": "complete", "percentage": 100, "label": "World Cup edition coverage"})


_WORLD_CUP_RESULT_ORDER = {
    "final": (1, "Final"),
    "third place": (3, "3º lugar"),
    "semi-finals": (4, "Semifinais"),
    "quarter-finals": (5, "Quartas de final"),
    "round of 16": (6, "Oitavas de final"),
    "round of 32": (7, "32 avos de final"),
    "group stage": (8, "Fase de grupos"),
}


def _world_cup_normalize_stage(value: Any) -> str:
    return re.sub(r"[-_]", " ", str(value or "").strip().lower())


def _world_cup_final_resolution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    final = next(
        (row for row in rows if _world_cup_normalize_stage(row.get("stage_name")) == "final"),
        None,
    )
    if final is None:
        return {"row": None, "champion": None, "runnerUp": None, "resolutionType": None, "resolutionNote": None}

    home_ref = _world_cup_team_ref(final.get("home_team_id"), final.get("home_team_name"))
    away_ref = _world_cup_team_ref(final.get("away_team_id"), final.get("away_team_name"))
    home_goals = int(final.get("home_goals") or 0)
    away_goals = int(final.get("away_goals") or 0)
    if home_goals > away_goals:
        return {"row": final, "champion": home_ref, "runnerUp": away_ref, "resolutionType": "single_match", "resolutionNote": None}
    if away_goals > home_goals:
        return {"row": final, "champion": away_ref, "runnerUp": home_ref, "resolutionType": "single_match", "resolutionNote": None}
    return {
        "row": final,
        "champion": None,
        "runnerUp": None,
        "resolutionType": "unresolved",
        "resolutionNote": "O placar regulamentar não identifica o vencedor do desempate.",
    }


def _world_cup_team_result(team_id: int, rows: list[dict[str, Any]]) -> tuple[str, int]:
    final = _world_cup_final_resolution(rows)["row"]
    if final and team_id in {int(final["home_team_id"]), int(final["away_team_id"])}:
        home_goals = int(final.get("home_goals") or 0)
        away_goals = int(final.get("away_goals") or 0)
        team_goals = home_goals if int(final["home_team_id"]) == team_id else away_goals
        opponent_goals = away_goals if int(final["home_team_id"]) == team_id else home_goals
        if team_goals > opponent_goals:
            return "Campeão", 1
        if team_goals < opponent_goals:
            return "Vice-campeão", 2
        return "Final (desempate não publicado)", 2

    stages = [
        _WORLD_CUP_RESULT_ORDER[_world_cup_normalize_stage(row.get("stage_name") or row.get("round_name"))]
        for row in rows
        if _world_cup_normalize_stage(row.get("stage_name") or row.get("round_name")) in _WORLD_CUP_RESULT_ORDER
    ]
    best_stage = min(stages, default=(99, "Participação"))
    return best_stage[1], best_stage[0]


def _world_cup_team_scorer_rows(team_ids: list[int] | None = None) -> list[dict[str, Any]]:
    clauses = ["g.team_id is not null"]
    params: list[Any] = []
    if team_ids:
        clauses.append("g.team_id = any(%s)")
        params.append(team_ids)
    return db_client.fetch_all(
        f"""
        with totals as (
            select g.edition_key, e.season_label, g.team_id, g.player_key,
                   max(p.display_name) as player_name,
                   count(*) filter (where coalesce(g.is_own_goal, false) = false)::int as goals
            from mart_v2.fact_world_cup_goal g
            join mart_v2.dim_edition e on e.edition_key = g.edition_key
            left join mart_v2.dim_player p on p.player_key = g.player_key
            where {' and '.join(clauses)}
            group by g.edition_key, e.season_label, g.team_id, g.player_key
        ), ranked as (
            select totals.*,
                   row_number() over (
                       partition by edition_key, team_id
                       order by goals desc, player_name nulls last, player_key
                   ) as scorer_rank
            from totals
            where goals > 0
        )
        select edition_key, season_label, team_id, player_key, player_name, goals
        from ranked
        where scorer_rank = 1
        order by season_label, team_id, player_key;
        """,
        params,
    )


def _world_cup_scorer_item(
    row: dict[str, Any],
    rank: int,
    team_id: int | None = None,
    team_name: str | None = None,
) -> dict[str, Any]:
    player_id = str(row["player_key"]) if row.get("player_key") is not None else None
    team_ref = _world_cup_team_ref(team_id, team_name) if team_id is not None else None
    return {
        "rank": rank,
        "playerId": player_id,
        "identity": {"entityType": "player", "competitionKey": "fifa_world_cup_mens", "canonicalId": player_id, "displayName": row.get("player_name"), "sourceSystem": "mart_v2", "confidence": "confirmed", "editorialStatus": "canonical"} if player_id else None,
        "imageAssetId": None,
        "playerName": row.get("player_name"),
        "profileUrl": f"/players/{player_id}" if player_id else None,
        "teamId": team_ref["teamId"] if team_ref else None,
        "teamName": team_ref["teamName"] if team_ref else None,
        "teamIdentity": team_ref.get("identity") if team_ref else None,
        "goals": int(row.get("goals") or 0),
    }


def _world_cup_team_catalog_v2(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    by_team: dict[int, dict[str, Any]] = defaultdict(lambda: {"teamName": None, "editions": defaultdict(list)})
    for row in rows:
        for side in ("home", "away"):
            team_id = row.get(f"{side}_team_id")
            if team_id is None:
                continue
            team_id_int = int(team_id)
            edition_key = str(row["edition_key"])
            by_team[team_id_int]["teamName"] = row.get(f"{side}_team_name") or by_team[team_id_int]["teamName"]
            by_team[team_id_int]["editions"][edition_key].append(row)

    top_scorers = {
        (str(row["edition_key"]), int(row["team_id"])): row
        for row in _world_cup_team_scorer_rows(list(by_team))
    }
    teams: list[dict[str, Any]] = []
    for team_id, item in by_team.items():
        team_ref = _world_cup_team_ref(team_id, item["teamName"])
        participations = []
        for edition_rows in item["editions"].values():
            first = edition_rows[0]
            label = _public_season_label(first.get("season_label")) or ""
            result_label, result_rank = _world_cup_team_result(team_id, edition_rows)
            scorer = top_scorers.get((str(first["edition_key"]), team_id))
            scorer_payload = None
            if scorer:
                scorer_payload = _world_cup_scorer_item(scorer, 1, team_id, item["teamName"])
            participations.append(
                {
                    "seasonLabel": label,
                    "year": int(label[:4]) if label[:4].isdigit() else 0,
                    "editionName": f"Copa do Mundo FIFA {label}",
                    "matchesCount": len(edition_rows),
                    "resultLabel": result_label,
                    "resultRank": result_rank,
                    "topScorer": scorer_payload,
                }
            )
        participations.sort(key=lambda item: item["year"])
        best = min(participations, key=lambda item: (item["resultRank"], item["year"]))
        teams.append(
            {
                "teamId": team_ref["teamId"],
                "teamName": team_ref["teamName"],
                "identity": team_ref.get("identity"),
                "participationsCount": len(participations),
                "titlesCount": sum(item["resultLabel"] == "Campeão" for item in participations),
                "bestResultLabel": best["resultLabel"],
                "firstEdition": participations[0]["year"],
                "lastEdition": participations[-1]["year"],
                "participations": participations,
            }
        )
    teams.sort(key=lambda item: (-item["titlesCount"], -item["participationsCount"], item["teamName"] or ""))
    return teams, {item["teamId"]: item for item in teams}


def _world_cup_team_historical_scorers(team_id: int) -> list[dict[str, Any]]:
    rows = db_client.fetch_all(
        """
        select g.player_key, max(p.display_name) as player_name,
               count(*) filter (where coalesce(g.is_own_goal, false) = false)::int as goals
        from mart_v2.fact_world_cup_goal g
        left join mart_v2.dim_player p on p.player_key = g.player_key
        where g.team_id = %s
        group by g.player_key
        having count(*) filter (where coalesce(g.is_own_goal, false) = false) >= 3
        order by goals desc, player_name nulls last, g.player_key
        limit 50;
        """,
        [team_id],
    )
    return [_world_cup_scorer_item(row, index) for index, row in enumerate(rows, 1)]


def _world_cup_team_assets(team_ids: list[int]) -> dict[str, dict[str, Any]]:
    if not team_ids:
        return {}
    return {
        str(row["team_id"]): row
        for row in db_client.fetch_all(
            """
            select team_id, country_or_territory, asset_url, asset_type
            from serving_v2.team_profile
            where team_id = any(%s);
            """,
            [team_ids],
        )
    }


def _world_cup_scorer_editions() -> dict[str, list[dict[str, Any]]]:
    rows = db_client.fetch_all(
        """
        select g.player_key, e.season_label, e.season_start_date, g.team_id,
               max(t.team_name) as team_name,
               count(*) filter (where coalesce(g.is_own_goal, false) = false)::int as goals
        from mart_v2.fact_world_cup_goal g
        join mart_v2.dim_edition e on e.edition_key = g.edition_key
        left join mart_v2.dim_team t on t.team_id = g.team_id
        where g.player_key is not null
        group by g.player_key, e.season_label, e.season_start_date, g.team_id
        having count(*) filter (where coalesce(g.is_own_goal, false) = false) > 0
        order by e.season_start_date, e.season_label, g.player_key;
        """
    )
    editions_by_player: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        player_id = str(row["player_key"])
        team_ref = _world_cup_team_ref(row.get("team_id"), row.get("team_name"))
        label = _public_season_label(row.get("season_label")) or ""
        editions_by_player[player_id].append(
            {
                "seasonLabel": label,
                "year": int(label[:4]) if label[:4].isdigit() else 0,
                "teamId": team_ref["teamId"] if team_ref else None,
                "teamName": team_ref["teamName"] if team_ref else None,
                "teamIdentity": team_ref.get("identity") if team_ref else None,
                "goals": int(row.get("goals") or 0),
            }
        )
    return editions_by_player


def _world_cup_squad_appearances() -> list[dict[str, Any]]:
    rows = db_client.fetch_all(
        """
        select s.player_key, e.season_label, e.season_start_date, s.team_id,
               max(p.display_name) as player_name, max(t.team_name) as team_name
        from mart_v2.fact_world_cup_squad s
        join mart_v2.dim_edition e on e.edition_key = s.edition_key
        left join mart_v2.dim_player p on p.player_key = s.player_key
        left join mart_v2.dim_team t on t.team_id = s.team_id
        where s.player_key is not null
        group by s.player_key, e.season_label, e.season_start_date, s.team_id
        order by e.season_start_date desc, e.season_label desc, s.player_key;
        """
    )
    players: dict[str, dict[str, Any]] = {}
    for row in rows:
        player_id = str(row["player_key"])
        item = players.setdefault(
            player_id,
            {
                "playerId": player_id,
                "playerName": row.get("player_name"),
                "teamId": None,
                "teamName": None,
                "teamIdentity": None,
                "editions": [],
            },
        )
        team_ref = _world_cup_team_ref(row.get("team_id"), row.get("team_name"))
        if item["teamId"] is None and team_ref:
            item["teamId"] = team_ref["teamId"]
            item["teamName"] = team_ref["teamName"]
            item["teamIdentity"] = team_ref.get("identity")
        label = _public_season_label(row.get("season_label")) or ""
        item["editions"].append({"seasonLabel": label, "year": int(label[:4]) if label[:4].isdigit() else 0})
    result = []
    for item in players.values():
        if len(item["editions"]) < 3:
            continue
        item["editions"].sort(key=lambda edition: edition["year"])
        item.update(
            {
                "rank": 0,
                "identity": {"entityType": "player", "competitionKey": "fifa_world_cup_mens", "canonicalId": item["playerId"], "displayName": item["playerName"], "sourceSystem": "mart_v2", "confidence": "confirmed", "editorialStatus": "canonical"},
                "imageAssetId": None,
                "profileUrl": f"/players/{item['playerId']}",
                "appearancesCount": len(item["editions"]),
            }
        )
        result.append(item)
    result.sort(key=lambda item: (-item["appearancesCount"], item["playerName"] or "", item["playerId"]))
    for index, item in enumerate(result, 1):
        item["rank"] = index
    return result


@router.get("/api/v1/world-cup/teams")
def get_world_cup_teams_v2(request: Request) -> dict[str, Any]:
    teams, _ = _world_cup_team_catalog_v2(_world_cup_match_rows())
    assets = _world_cup_team_assets([int(team["teamId"]) for team in teams])
    items = [
        {
            **{
                key: team[key]
                for key in (
                    "teamId",
                    "teamName",
                    "identity",
                    "participationsCount",
                    "titlesCount",
                    "bestResultLabel",
                    "firstEdition",
                    "lastEdition",
                )
            },
            "countryOrTerritory": assets.get(team["teamId"], {}).get("country_or_territory"),
            "matchesCount": sum(participation["matchesCount"] for participation in team["participations"]),
            "imageAssetId": team["teamId"] if assets.get(team["teamId"], {}).get("asset_url") else None,
            "imageAssetUrl": assets.get(team["teamId"], {}).get("asset_url"),
            "assetType": assets.get(team["teamId"], {}).get("asset_type"),
            "participations": team["participations"],
        }
        for team in teams
    ]
    return _response(
        {"competition": _world_cup_competition_ref(), "teams": items, "updatedAt": None},
        request,
        coverage={"status": "complete" if items else "empty", "percentage": 100 if items else 0, "label": "World Cup teams coverage"},
    )


@router.get("/api/v1/world-cup/teams/{team_id}")
def get_world_cup_team_v2(team_id: str, request: Request) -> dict[str, Any]:
    try:
        team_id_int = int(team_id)
    except ValueError as exc:
        raise AppError("Invalid team id.", "INVALID_QUERY_PARAM", 400, {"teamId": team_id}) from exc
    rows = _world_cup_match_rows()
    _, team_index = _world_cup_team_catalog_v2(rows)
    team = team_index.get(str(team_id_int))
    if team is None:
        raise AppError("World Cup team not found.", "TEAM_NOT_FOUND", 404, {"teamId": team_id})
    team_rows = [row for row in rows if int(row["home_team_id"]) == team_id_int or int(row["away_team_id"]) == team_id_int]
    asset = _world_cup_team_assets([team_id_int]).get(str(team_id_int), {})
    team_summary = {
        key: team[key]
        for key in (
            "teamId",
            "teamName",
            "identity",
            "participationsCount",
            "titlesCount",
            "bestResultLabel",
            "firstEdition",
            "lastEdition",
        )
    }
    team_summary.update(
        {
            "countryOrTerritory": asset.get("country_or_territory"),
            "imageAssetId": team["teamId"] if asset.get("asset_url") else None,
            "imageAssetUrl": asset.get("asset_url"),
            "assetType": asset.get("asset_type"),
        }
    )
    return _response(
        {
            "competition": _world_cup_competition_ref(),
            "team": team_summary,
            "participations": team["participations"],
            "historicalScorers": _world_cup_team_historical_scorers(team_id_int),
            "summary": {
                "matches": len(team_rows),
                "wins": sum(
                    1
                    for row in team_rows
                    if (int(row["home_team_id"]) == team_id_int and int(row.get("home_goals") or 0) > int(row.get("away_goals") or 0))
                    or (int(row["away_team_id"]) == team_id_int and int(row.get("away_goals") or 0) > int(row.get("home_goals") or 0))
                ),
                "goals": sum(
                    int(row.get("home_goals") or 0) if int(row["home_team_id"]) == team_id_int else int(row.get("away_goals") or 0)
                    for row in team_rows
                ),
            },
            "matches": [_match_item(row) for row in team_rows],
            "updatedAt": None,
        },
        request,
        coverage={"status": "complete", "percentage": 100, "label": "World Cup team coverage"},
    )


@router.get("/api/v1/world-cup/rankings")
def get_world_cup_rankings_v2(request: Request) -> dict[str, Any]:
    rows = _world_cup_match_rows()
    team_catalog, _ = _world_cup_team_catalog_v2(rows)
    aggregate: dict[int, dict[str, Any]] = defaultdict(lambda: {"teamName": None, "matches": 0, "wins": 0, "goals": 0, "editions": set(), "finals": 0})
    editions: dict[str, dict[str, Any]] = defaultdict(lambda: {"matches": 0, "goals": 0})
    for row in rows:
        season = _public_season_label(row.get("season_label")) or ""
        editions[season]["matches"] += 1
        editions[season]["goals"] += int(row.get("home_goals") or 0) + int(row.get("away_goals") or 0)
        is_final = str(row.get("stage_name") or "").lower() == "final"
        for side in ("home", "away"):
            team_id = int(row[f"{side}_team_id"])
            goals = int(row.get(f"{side}_goals") or 0)
            opponent_goals = int(row.get("away_goals" if side == "home" else "home_goals") or 0)
            aggregate[team_id]["teamName"] = row.get(f"{side}_team_name")
            aggregate[team_id]["matches"] += 1
            aggregate[team_id]["goals"] += goals
            aggregate[team_id]["wins"] += int(goals > opponent_goals)
            aggregate[team_id]["editions"].add(season)
            aggregate[team_id]["finals"] += int(is_final)

    def team_ref(team_id: int, item: dict[str, Any]) -> dict[str, Any]:
        return _world_cup_team_ref(team_id, item.get("teamName")) or {"teamId": str(team_id), "teamName": item.get("teamName")}

    def ranked(field: str, output_field: str | None = None) -> list[dict[str, Any]]:
        key = output_field or field
        return [
            {
                "rank": index,
                "teamId": str(team_id),
                "teamName": item["teamName"],
                "identity": team_ref(team_id, item).get("identity"),
                key: item[field],
                "matches": item["matches"],
                "wins": item["wins"],
            }
            for index, (team_id, item) in enumerate(
                sorted(aggregate.items(), key=lambda pair: (-pair[1][field], pair[1]["teamName"] or "")),
                1,
            )
        ]

    team_items = [
        {
            "rank": index,
            "teamId": item["teamId"],
            "teamName": item["teamName"],
            "identity": item.get("identity"),
            "titlesCount": item["titlesCount"],
            "participationsCount": item["participationsCount"],
            "finalsCount": sum(participation["resultRank"] <= 2 for participation in item["participations"]),
        }
        for index, item in enumerate(
            sorted(team_catalog, key=lambda team: (-team["participationsCount"], team["teamName"] or "")),
            1,
        )
    ]
    edition_records = [
        {
            "seasonLabel": season,
            "year": int(season[:4]) if season[:4].isdigit() else 0,
            "editionName": f"Copa do Mundo FIFA {season}",
            "matchesCount": int(item["matches"]),
            "goalsCount": int(item["goals"]),
            "goalsPerMatch": round(item["goals"] / item["matches"], 4) if item["matches"] else 0,
        }
        for season, item in editions.items()
    ]
    edition_goals = [dict(item, rank=index) for index, item in enumerate(sorted(edition_records, key=lambda item: (-item["goalsCount"], -item["matchesCount"], -item["year"])), 1)]
    edition_goal_average = [dict(item, rank=index) for index, item in enumerate(sorted(edition_records, key=lambda item: (-item["goalsPerMatch"], -item["goalsCount"], -item["year"])), 1)]
    scorer_editions = _world_cup_scorer_editions()
    scorer_items = [
        {**item, "editions": scorer_editions.get(str(item["playerId"]), [])}
        for item in _world_cup_scorers()
    ]
    squad_items = _world_cup_squad_appearances()
    finals = []
    biggest_wins = []
    for row in rows:
        home_ref = _world_cup_team_ref(row.get("home_team_id"), row.get("home_team_name"))
        away_ref = _world_cup_team_ref(row.get("away_team_id"), row.get("away_team_name"))
        home_score = int(row.get("home_goals") or 0)
        away_score = int(row.get("away_goals") or 0)
        if str(row.get("stage_name") or "").lower() == "final":
            finals.append({"seasonLabel": _public_season_label(row.get("season_label")), "year": int(str(row.get("season_label"))[:4]), "homeTeam": home_ref, "awayTeam": away_ref, "homeScore": home_score, "awayScore": away_score, "shootout": None, "venueName": row.get("venue_name"), "champion": home_ref if home_score > away_score else away_ref if away_score > home_score else None, "runnerUp": away_ref if home_score > away_score else home_ref if away_score > home_score else None, "resolutionType": None if home_score != away_score else "unresolved", "resolutionNote": None if home_score != away_score else "O placar regulamentar não identifica o vencedor do desempate."})
        if home_score != away_score:
            biggest_wins.append({"fixtureId": str(row["match_id"]), "seasonLabel": _public_season_label(row.get("season_label")), "year": int(str(row.get("season_label"))[:4]), "homeTeam": home_ref, "awayTeam": away_ref, "homeScore": home_score, "awayScore": away_score, "goalDiff": abs(home_score - away_score), "totalGoals": home_score + away_score, "venueName": row.get("venue_name")})
    finals.sort(key=lambda item: item["year"], reverse=True)
    biggest_wins = sorted(biggest_wins, key=lambda item: (-item["goalDiff"], -item["totalGoals"], item["year"]))[:50]
    for index, item in enumerate(finals, 1):
        item["rank"] = index
    for index, item in enumerate(biggest_wins, 1):
        item["rank"] = index

    title_items = [
        dict(item, rank=index)
        for index, item in enumerate(
            sorted(team_items, key=lambda item: (-item["titlesCount"], -item["participationsCount"], item["teamName"] or "")),
            1,
        )
        if item["titlesCount"] > 0
    ]
    top_four_items = []
    for index, item in enumerate(
        sorted(
            (
                {
                    "teamId": team["teamId"],
                    "teamName": team["teamName"],
                    "identity": team.get("identity"),
                    "topFourCount": sum(participation["resultRank"] <= 4 for participation in team["participations"]),
                    "titlesCount": team["titlesCount"],
                }
                for team in team_catalog
            ),
            key=lambda item: (-item["topFourCount"], -item["titlesCount"], item["teamName"] or ""),
        ),
        1,
    ):
        if item["topFourCount"] > 0:
            top_four_items.append(dict(item, rank=index))
    edition_labels = {_public_season_label(row.get("season_label")) for row in _world_cup_editions()}
    final_labels = {item["seasonLabel"] for item in finals}
    omitted_editions = [
        {"seasonLabel": label, "year": int(label[:4]) if label and label[:4].isdigit() else 0, "reason": "Nenhuma partida classificada como final na mart_v2."}
        for label in sorted(edition_labels - final_labels, reverse=True)
        if label
    ]
    payload = {
        "competition": _world_cup_competition_ref(),
        "scorers": scorer_items,
        "teams": team_items,
        "teamRankings": {
            "titles": {"label": "Títulos", "metricLabel": "títulos", "items": title_items},
            "wins": {"label": "Vitórias", "metricLabel": "vitórias", "items": ranked("wins")},
            "matches": {"label": "Partidas", "metricLabel": "partidas", "items": ranked("matches")},
            "goalsScored": {"label": "Gols", "metricLabel": "gols", "items": ranked("goals", "goalsScored")},
            "topFourAppearances": {"label": "Top 4", "metricLabel": "aparições no top 4", "items": top_four_items},
        },
        "editionRankings": {
            "goalsPerMatch": {"label": "Média de gols", "metricLabel": "gols por partida", "items": edition_goal_average},
            "goals": {"label": "Gols por edição", "metricLabel": "gols", "items": edition_goals},
        },
        "playerRankings": {
            "scorers": {"label": "Artilheiros", "metricLabel": "gols", "items": scorer_items},
            "squadAppearances": {"label": "Presenças", "metricLabel": "convocações", "items": squad_items, "minimumAppearancesCount": 3},
        },
        "matchRankings": {
            "highestScoringFinals": {"label": "Finais com mais gols", "metricLabel": "gols", "items": sorted(finals, key=lambda item: (-(item["homeScore"] + item["awayScore"]), -item["year"]))[:50]},
            "biggestWins": {"label": "Maiores goleadas", "metricLabel": "saldo", "items": biggest_wins},
        },
        "finals": {"items": finals, "omittedEditions": omitted_editions},
        "updatedAt": None,
    }
    return _response(payload, request, coverage={"status": "complete" if rows else "empty", "percentage": 100 if rows else 0, "label": "World Cup rankings coverage"})
