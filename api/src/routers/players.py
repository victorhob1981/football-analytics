from __future__ import annotations

from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Query, Request

from ..core.context_registry import build_canonical_context, select_default_context
from ..core.contracts import build_api_response, build_coverage_from_counts, build_pagination
from ..core.errors import AppError
from ..core.filters import GlobalFilters, VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/players", tags=["players"])

PlayersSortBy = Literal["playerName", "minutesPlayed", "goals", "assists", "rating"]
SortDirection = Literal["asc", "desc"]


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _to_player_id(player_id: str) -> int:
    try:
        return int(player_id)
    except ValueError as exc:
        raise AppError(
            message="Invalid player id. Expected integer-compatible identifier.",
            code="INVALID_QUERY_PARAM",
            status=400,
            details={"playerId": player_id},
        ) from exc


def _player_scope_filters_sql(filters: GlobalFilters) -> tuple[str, list[Any]]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)

    if filters.venue == VenueFilter.home:
        where_clauses.append("pms.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        where_clauses.append("pms.team_id = fm.away_team_id")

    return " and ".join(where_clauses), params


def _player_coverage(filters: GlobalFilters) -> dict[str, Any]:
    match_where: list[str] = ["1=1"]
    match_params: list[Any] = []
    append_fact_match_filters(match_where, match_params, alias="fm", filters=filters)

    player_where, player_params = _player_scope_filters_sql(filters)
    query = f"""
        with scoped_matches as (
            select distinct fm.match_id
            from mart.fact_matches fm
            where {" and ".join(match_where)}
        ),
        scoped_stats as (
            select distinct pms.match_id
            from mart.player_match_summary pms
            inner join mart.fact_matches fm
              on fm.match_id = pms.match_id
            where {player_where}
        )
        select
            (select count(*) from scoped_stats) as available_count,
            (select count(*) from scoped_matches) as total_count;
    """
    row = db_client.fetch_one(query, [*match_params, *player_params]) or {}
    available = int(row.get("available_count") or 0)
    total = int(row.get("total_count") or 0)
    return build_coverage_from_counts(available, total, "Player stats coverage")


@router.get("")
def get_players(
    request: Request,
    competitionId: str | None = None,
    seasonId: str | None = None,
    roundId: str | None = None,
    stageId: str | None = None,
    stageFormat: str | None = None,
    venue: VenueFilter = VenueFilter.all,
    lastN: int | None = Query(default=None, gt=0),
    dateStart: date | None = None,
    dateEnd: date | None = None,
    dateRangeStart: date | None = None,
    dateRangeEnd: date | None = None,
    search: str | None = None,
    teamId: str | None = None,
    position: str | None = None,
    minMinutes: int | None = Query(default=None, ge=0),
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=20, ge=1, le=100),
    sortBy: PlayersSortBy = "goals",
    sortDirection: SortDirection = "desc",
) -> dict[str, Any]:
    global_filters = validate_and_build_global_filters(
        competition_id=competitionId,
        season_id=seasonId,
        round_id=roundId,
        stage_id=stageId,
        stage_format=stageFormat,
        venue=venue,
        last_n=lastN,
        date_start=dateStart,
        date_end=dateEnd,
        date_range_start=dateRangeStart,
        date_range_end=dateRangeEnd,
    )

    where_sql, where_params = _player_scope_filters_sql(global_filters)
    sort_column = {
        "playerName": "f.player_name",
        "minutesPlayed": "f.minutes_played",
        "goals": "f.goals",
        "assists": "f.assists",
        "rating": "f.rating",
    }[sortBy]
    sort_dir = "asc" if sortDirection == "asc" else "desc"
    offset = (page - 1) * pageSize

    team_id_int: int | None = None
    if teamId is not None and teamId.strip() != "":
        if not teamId.strip().isdigit():
            raise AppError(
                message="Invalid value for 'teamId'. Expected integer.",
                code="INVALID_QUERY_PARAM",
                status=400,
                details={"teamId": teamId},
            )
        team_id_int = int(teamId)
    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    position_pattern = f"%{position.strip()}%" if position and position.strip() else None

    query = f"""
        with scoped as (
            select
                pms.player_id,
                pms.player_name,
                pms.team_id,
                pms.team_name,
                pms.position_name,
                pms.match_id,
                pms.match_date,
                coalesce(pms.minutes_played, 0) as minutes_played,
                coalesce(pms.goals, 0) as goals,
                coalesce(pms.assists, 0) as assists,
                coalesce(pms.shots_total, 0) as shots_total,
                coalesce(pms.yellow_cards, 0) as yellow_cards,
                coalesce(pms.red_cards, 0) as red_cards,
                pms.rating,
                row_number() over (
                    partition by pms.player_id
                    order by pms.match_date desc, pms.match_id desc
                ) as rn_recent
            from mart.player_match_summary pms
            inner join mart.fact_matches fm
              on fm.match_id = pms.match_id
            where {where_sql}
        ),
        filtered_scoped as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        aggregated as (
            select
                fs.player_id,
                max(fs.player_name) as player_name,
                count(distinct fs.match_id) as matches_played,
                sum(fs.minutes_played)::numeric as minutes_played,
                sum(fs.goals)::numeric as goals,
                sum(fs.assists)::numeric as assists,
                sum(fs.shots_total)::numeric as shots_total,
                sum(fs.yellow_cards)::numeric as yellow_cards,
                sum(fs.red_cards)::numeric as red_cards,
                avg(fs.rating)::numeric as rating
            from filtered_scoped fs
            group by fs.player_id
        ),
        latest_context as (
            select distinct on (fs.player_id)
                fs.player_id,
                fs.team_id,
                fs.team_name,
                fs.position_name
            from filtered_scoped fs
            order by fs.player_id, fs.match_date desc, fs.match_id desc
        ),
        final_rows as (
            select
                a.player_id,
                a.player_name,
                lc.team_id,
                lc.team_name,
                lc.position_name,
                dp.nationality,
                a.matches_played,
                a.minutes_played,
                a.goals,
                a.assists,
                a.shots_total,
                a.yellow_cards,
                a.red_cards,
                a.rating
            from aggregated a
            left join latest_context lc
              on lc.player_id = a.player_id
            left join mart.dim_player dp
              on dp.player_id = a.player_id
            where (%s::text is null or a.player_name ilike %s)
              and (%s::bigint is null or lc.team_id = %s)
              and (%s::text is null or coalesce(lc.position_name, '') ilike %s)
              and (%s::numeric is null or a.minutes_played >= %s)
        )
        select
            f.*,
            count(*) over() as _total_count
        from final_rows f
        order by {sort_column} {sort_dir}, f.player_id asc
        limit %s offset %s;
    """

    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
            search_pattern,
            search_pattern,
            team_id_int,
            team_id_int,
            position_pattern,
            position_pattern,
            minMinutes,
            minMinutes,
            pageSize,
            offset,
        ],
    )

    total_count = int(rows[0]["_total_count"]) if rows else 0
    pagination = build_pagination(page, pageSize, total_count)
    coverage = _player_coverage(global_filters)

    items = [
        {
            "playerId": str(row["player_id"]),
            "playerName": row["player_name"],
            "teamId": str(row["team_id"]) if row.get("team_id") is not None else None,
            "teamName": row.get("team_name"),
            "position": row.get("position_name"),
            "nationality": row.get("nationality"),
            "matchesPlayed": int(row.get("matches_played") or 0),
            "minutesPlayed": float(row.get("minutes_played") or 0),
            "goals": float(row.get("goals") or 0),
            "assists": float(row.get("assists") or 0),
            "shotsTotal": float(row.get("shots_total") or 0),
            "passAccuracyPct": None,
            "yellowCards": float(row.get("yellow_cards") or 0),
            "redCards": float(row.get("red_cards") or 0),
            "rating": float(row["rating"]) if row.get("rating") is not None else None,
        }
        for row in rows
    ]

    return build_api_response(
        {"items": items},
        request_id=_request_id(request),
        pagination=pagination,
        coverage=coverage,
    )


def _profile_coverage(player_id: int, filters: GlobalFilters) -> dict[str, Any]:
    where_mart, params_mart = _player_scope_filters_sql(filters)
    mart_query = f"""
        select count(distinct pms.match_id) as count_matches
        from mart.player_match_summary pms
        inner join mart.fact_matches fm
          on fm.match_id = pms.match_id
        where pms.player_id = %s
          and {where_mart};
    """
    mart_row = db_client.fetch_one(mart_query, [player_id, *params_mart]) or {}
    available = int(mart_row.get("count_matches") or 0)

    raw_clauses: list[str] = ["1=1", "fps.player_id = %s"]
    raw_params: list[Any] = [player_id]
    append_fact_match_filters(raw_clauses, raw_params, alias="fm", filters=filters)
    if filters.venue == VenueFilter.home:
        raw_clauses.append("fps.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        raw_clauses.append("fps.team_id = fm.away_team_id")

    raw_query = f"""
        select count(distinct fps.fixture_id) as count_matches
        from raw.fixture_player_statistics fps
        inner join mart.fact_matches fm
          on fm.match_id = fps.fixture_id
        where {" and ".join(raw_clauses)};
    """
    raw_row = db_client.fetch_one(raw_query, raw_params) or {}
    total = int(raw_row.get("count_matches") or 0)

    return build_coverage_from_counts(available, total, "Player profile coverage")


def _section_coverage_from_match_count(match_count: int, label: str) -> dict[str, Any]:
    if match_count <= 0:
        return {"status": "empty", "percentage": 0, "label": label}

    return {"status": "complete", "percentage": 100, "label": label}


def _coverage_score(coverage: dict[str, Any]) -> float | None:
    if isinstance(coverage.get("percentage"), (int, float)):
        return float(coverage["percentage"])

    status = coverage.get("status")
    if status == "complete":
        return 100.0
    if status == "empty":
        return 0.0
    if status == "partial":
        return 50.0
    return None


def _merge_coverages(label: str, coverages: list[dict[str, Any]]) -> dict[str, Any]:
    if not coverages:
        return {"status": "unknown", "label": label}

    statuses = {str(coverage.get("status") or "unknown") for coverage in coverages}
    if statuses == {"complete"}:
        status = "complete"
    elif statuses == {"empty"}:
        status = "empty"
    elif statuses == {"unknown"}:
        status = "unknown"
    else:
        status = "partial"

    payload: dict[str, Any] = {"status": status, "label": label}
    scores = [score for score in (_coverage_score(coverage) for coverage in coverages) if score is not None]
    if scores:
        payload["percentage"] = round(sum(scores) / len(scores), 2)
    return payload


def _with_coverage_label(coverage: dict[str, Any], label: str) -> dict[str, Any]:
    payload = dict(coverage)
    payload["label"] = label
    return payload


def _resolve_result(goals_for: int, goals_against: int) -> str:
    if goals_for > goals_against:
        return "win"
    if goals_for < goals_against:
        return "loss"
    return "draw"


def _safe_divide(
    numerator: float | int | None,
    denominator: float | int | None,
    digits: int = 2,
) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return round(float(numerator) / float(denominator), digits)


@router.get("/{playerId}/contexts")
def get_player_contexts(
    playerId: str,
    request: Request,
    competitionId: str | None = None,
    seasonId: str | None = None,
) -> dict[str, Any]:
    player_id = _to_player_id(playerId)
    preference_filters = validate_and_build_global_filters(
        competition_id=competitionId,
        season_id=seasonId,
        round_id=None,
        venue=VenueFilter.all,
        last_n=None,
        date_start=None,
        date_end=None,
        date_range_start=None,
        date_range_end=None,
    )

    player_ref = db_client.fetch_one(
        "select player_id, player_name, nationality from mart.dim_player where player_id = %s;",
        [player_id],
    )
    if player_ref is None:
        raise AppError(
            message="Player not found.",
            code="PLAYER_NOT_FOUND",
            status=404,
            details={"playerId": playerId},
        )

    context_rows = db_client.fetch_all(
        """
        with player_contexts as (
            select
                dc.league_id,
                dc.league_name,
                pms.season,
                max(pms.match_date) as last_match_date,
                count(distinct pms.match_id) as matches_played
            from mart.player_match_summary pms
            inner join mart.dim_competition dc
              on dc.competition_sk = pms.competition_sk
            where pms.player_id = %s
            group by dc.league_id, dc.league_name, pms.season
        )
        select
            league_id,
            league_name,
            season,
            last_match_date,
            matches_played
        from player_contexts
        order by
            last_match_date desc nulls last,
            matches_played desc,
            season desc,
            league_id asc;
        """,
        [player_id],
    )

    available_contexts: list[dict[str, str]] = []
    seen_contexts: set[tuple[str, str]] = set()

    for row in context_rows:
        context = build_canonical_context(
            competition_id=row.get("league_id"),
            competition_name=row.get("league_name"),
            season_id=row.get("season"),
        )
        if context is None:
            continue

        identity = (context["competitionId"], context["seasonId"])
        if identity in seen_contexts:
            continue

        seen_contexts.add(identity)
        available_contexts.append(context)

    default_context = select_default_context(
        available_contexts,
        preferred_competition_id=preference_filters.competition_id,
        preferred_season_id=preference_filters.season_id,
    )

    return build_api_response(
        {
            "defaultContext": default_context,
            "availableContexts": available_contexts,
        },
        request_id=_request_id(request),
    )


@router.get("/{playerId}")
def get_player_profile(
    playerId: str,
    request: Request,
    competitionId: str | None = None,
    seasonId: str | None = None,
    roundId: str | None = None,
    stageId: str | None = None,
    stageFormat: str | None = None,
    venue: VenueFilter = VenueFilter.all,
    lastN: int | None = Query(default=None, gt=0),
    dateStart: date | None = None,
    dateEnd: date | None = None,
    dateRangeStart: date | None = None,
    dateRangeEnd: date | None = None,
    includeRecentMatches: bool = True,
    includeHistory: bool = True,
    includeStats: bool = True,
    recentMatchesLimit: int = Query(default=10, ge=1, le=50),
) -> dict[str, Any]:
    player_id = _to_player_id(playerId)
    global_filters = validate_and_build_global_filters(
        competition_id=competitionId,
        season_id=seasonId,
        round_id=roundId,
        stage_id=stageId,
        stage_format=stageFormat,
        venue=venue,
        last_n=lastN,
        date_start=dateStart,
        date_end=dateEnd,
        date_range_start=dateRangeStart,
        date_range_end=dateRangeEnd,
    )

    player_ref = db_client.fetch_one(
        "select player_id, player_name, nationality from mart.dim_player where player_id = %s;",
        [player_id],
    )
    if player_ref is None:
        raise AppError(
            message="Player not found.",
            code="PLAYER_NOT_FOUND",
            status=404,
            details={"playerId": playerId},
        )

    where_sql, where_params = _player_scope_filters_sql(global_filters)
    summary_query = f"""
        with scoped as (
            select
                pms.player_id,
                pms.player_name,
                pms.team_id,
                pms.team_name,
                pms.position_name,
                pms.match_id,
                pms.match_date,
                coalesce(pms.minutes_played, 0) as minutes_played,
                coalesce(pms.goals, 0) as goals,
                coalesce(pms.assists, 0) as assists,
                coalesce(pms.shots_total, 0) as shots_total,
                coalesce(pms.shots_on_goal, 0) as shots_on_goal,
                coalesce(pms.passes_total, 0) as passes_attempted,
                coalesce(pms.yellow_cards, 0) as yellow_cards,
                coalesce(pms.red_cards, 0) as red_cards,
                pms.rating,
                row_number() over (
                    partition by pms.player_id
                    order by pms.match_date desc, pms.match_id desc
                ) as rn_recent
            from mart.player_match_summary pms
            inner join mart.fact_matches fm
              on fm.match_id = pms.match_id
            where pms.player_id = %s
              and {where_sql}
        ),
        filtered_scoped as (
            select *
            from scoped
            where (%s::int is null or rn_recent <= %s)
        ),
        latest_context as (
            select
                fs.team_id,
                fs.team_name,
                fs.position_name
            from filtered_scoped fs
            order by fs.match_date desc, fs.match_id desc
            limit 1
        )
        select
            (select team_id from latest_context) as team_id,
            (select team_name from latest_context) as team_name,
            (select position_name from latest_context) as position_name,
            count(distinct fs.match_id) as matches_played,
            max(fs.match_date) as last_match_date,
            sum(fs.minutes_played)::numeric as minutes_played,
            sum(fs.goals)::numeric as goals,
            sum(fs.assists)::numeric as assists,
            sum(fs.shots_total)::numeric as shots_total,
            sum(fs.shots_on_goal)::numeric as shots_on_target,
            sum(fs.passes_attempted)::numeric as passes_attempted,
            sum(fs.yellow_cards)::numeric as yellow_cards,
            sum(fs.red_cards)::numeric as red_cards,
            avg(fs.rating)::numeric as rating
        from filtered_scoped fs;
    """

    summary_row = db_client.fetch_one(
        summary_query,
        [
            player_id,
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
        ],
    ) or {}
    coverage = _profile_coverage(player_id, global_filters)

    recent_matches: list[dict[str, Any]] | None = None
    recent_match_items: list[dict[str, Any]] = []
    if includeRecentMatches:
        recent_query = f"""
            with scoped as (
                select
                    pms.player_id,
                    pms.team_id,
                    pms.team_name,
                    pms.match_id,
                    pms.match_date,
                    coalesce(pms.minutes_played, 0) as minutes_played,
                    coalesce(pms.goals, 0) as goals,
                    coalesce(pms.assists, 0) as assists,
                    coalesce(pms.shots_total, 0) as shots_total,
                    coalesce(pms.shots_on_goal, 0) as shots_on_goal,
                    coalesce(pms.passes_total, 0) as passes_total,
                    pms.rating,
                    row_number() over (
                        partition by pms.player_id
                        order by pms.match_date desc, pms.match_id desc
                    ) as rn_recent
                from mart.player_match_summary pms
                inner join mart.fact_matches fm
                  on fm.match_id = pms.match_id
                where pms.player_id = %s
                  and {where_sql}
            ),
            filtered_scoped as (
                select *
                from scoped
                where (%s::int is null or rn_recent <= %s)
            )
            select
                fs.match_id::text as fixture_id,
                fs.match_id::text as match_id,
                fs.match_date as played_at,
                dc.league_id::text as competition_id,
                dc.league_name as competition_name,
                fm.season::text as season_id,
                fm.round_number::text as round_id,
                fs.team_id::text as team_id,
                coalesce(fs.team_name, player_team.team_name) as team_name,
                case
                    when fs.team_id = fm.home_team_id then away_team.team_id
                    else home_team.team_id
                end::text as opponent_team_id,
                case
                    when fs.team_id = fm.home_team_id then away_team.team_name
                    else home_team.team_name
                end as opponent_name,
                case
                    when fs.team_id = fm.home_team_id then 'home'
                    else 'away'
                end as venue_role,
                case
                    when fs.team_id = fm.home_team_id then coalesce(fm.home_goals, 0)
                    else coalesce(fm.away_goals, 0)
                end as goals_for,
                case
                    when fs.team_id = fm.home_team_id then coalesce(fm.away_goals, 0)
                    else coalesce(fm.home_goals, 0)
                end as goals_against,
                fs.minutes_played,
                fs.goals,
                fs.assists,
                fs.shots_total,
                fs.shots_on_goal,
                fs.passes_total,
                fs.rating
            from filtered_scoped fs
            inner join mart.fact_matches fm
              on fm.match_id = fs.match_id
            inner join mart.dim_competition dc
              on dc.competition_sk = fm.competition_sk
            left join mart.dim_team home_team
              on home_team.team_id = fm.home_team_id
            left join mart.dim_team away_team
              on away_team.team_id = fm.away_team_id
            left join mart.dim_team player_team
              on player_team.team_id = fs.team_id
            order by fs.match_date desc, fs.match_id desc
            limit %s;
        """
        recent_rows = db_client.fetch_all(
            recent_query,
            [
                player_id,
                *where_params,
                global_filters.last_n,
                global_filters.last_n,
                recentMatchesLimit,
            ],
        )
        recent_match_items = [
            {
                "fixtureId": row["fixture_id"],
                "matchId": row["match_id"],
                "playedAt": row.get("played_at"),
                "competitionId": row.get("competition_id"),
                "competitionName": row.get("competition_name"),
                "seasonId": row.get("season_id"),
                "roundId": row.get("round_id"),
                "teamId": row.get("team_id"),
                "teamName": row.get("team_name"),
                "opponentTeamId": row.get("opponent_team_id"),
                "opponentName": row.get("opponent_name"),
                "venue": row.get("venue_role"),
                "goalsFor": int(row.get("goals_for") or 0),
                "goalsAgainst": int(row.get("goals_against") or 0),
                "result": _resolve_result(
                    int(row.get("goals_for") or 0),
                    int(row.get("goals_against") or 0),
                ),
                "minutesPlayed": float(row.get("minutes_played") or 0),
                "goals": float(row.get("goals") or 0),
                "assists": float(row.get("assists") or 0),
                "shotsTotal": float(row.get("shots_total") or 0),
                "shotsOnTarget": float(row.get("shots_on_goal") or 0),
                "passesAttempted": float(row.get("passes_total") or 0),
                "rating": float(row["rating"]) if row.get("rating") is not None else None,
            }
            for row in recent_rows
        ]
        recent_matches = recent_match_items[:recentMatchesLimit]

    history_payload: list[dict[str, Any]] | None = None
    history_coverage: dict[str, Any] | None = None
    if includeHistory:
        history_rows = db_client.fetch_all(
            """
            with player_history as (
                select
                    dc.league_id,
                    dc.league_name,
                    pms.season,
                    pms.team_id,
                    coalesce(max(pms.team_name), dt.team_name) as team_name,
                    count(distinct pms.match_id)::int as matches_played,
                    sum(coalesce(pms.minutes_played, 0))::numeric as minutes_played,
                    sum(coalesce(pms.goals, 0))::numeric as goals,
                    sum(coalesce(pms.assists, 0))::numeric as assists,
                    avg(pms.rating)::numeric as rating,
                    max(pms.match_date) as last_match_date
                from mart.player_match_summary pms
                inner join mart.dim_competition dc
                  on dc.competition_sk = pms.competition_sk
                left join mart.dim_team dt
                  on dt.team_id = pms.team_id
                where pms.player_id = %s
                group by
                    dc.league_id,
                    dc.league_name,
                    pms.season,
                    pms.team_id,
                    dt.team_name
            )
            select
                league_id,
                league_name,
                season,
                team_id,
                team_name,
                matches_played,
                minutes_played,
                goals,
                assists,
                rating,
                last_match_date
            from player_history
            order by
                last_match_date desc nulls last,
                matches_played desc,
                season desc,
                league_id asc,
                team_name asc;
            """,
            [player_id],
        )
        history_payload = []
        for row in history_rows:
            canonical_context = build_canonical_context(
                competition_id=row.get("league_id"),
                competition_name=row.get("league_name"),
                season_id=row.get("season"),
            )
            history_payload.append(
                {
                    "competitionId": canonical_context["competitionId"]
                    if canonical_context
                    else str(row["league_id"])
                    if row.get("league_id") is not None
                    else None,
                    "competitionKey": canonical_context["competitionKey"] if canonical_context else None,
                    "competitionName": canonical_context["competitionName"]
                    if canonical_context
                    else row.get("league_name"),
                    "seasonId": canonical_context["seasonId"]
                    if canonical_context
                    else str(row["season"])
                    if row.get("season") is not None
                    else None,
                    "seasonLabel": canonical_context["seasonLabel"]
                    if canonical_context
                    else str(row["season"])
                    if row.get("season") is not None
                    else None,
                    "teamId": str(row["team_id"]) if row.get("team_id") is not None else None,
                    "teamName": row.get("team_name"),
                    "matchesPlayed": int(row.get("matches_played") or 0),
                    "minutesPlayed": float(row.get("minutes_played") or 0),
                    "goals": float(row.get("goals") or 0),
                    "assists": float(row.get("assists") or 0),
                    "rating": float(row["rating"]) if row.get("rating") is not None else None,
                    "lastMatchAt": row.get("last_match_date"),
                }
            )
        history_coverage = _section_coverage_from_match_count(
            len(history_payload),
            "Player history coverage",
        )

    stats_payload: dict[str, Any] | None = None
    stats_coverage: dict[str, Any] | None = None
    if includeStats:
        stats_rows = db_client.fetch_all(
            f"""
            with scoped as (
                select
                    pms.match_id,
                    pms.match_date,
                    coalesce(pms.minutes_played, 0) as minutes_played,
                    coalesce(pms.goals, 0) as goals,
                    coalesce(pms.assists, 0) as assists,
                    coalesce(pms.shots_total, 0) as shots_total,
                    coalesce(pms.shots_on_goal, 0) as shots_on_goal,
                    coalesce(pms.passes_total, 0) as passes_total,
                    pms.rating,
                    row_number() over (
                        partition by pms.player_id
                        order by pms.match_date desc, pms.match_id desc
                    ) as rn_recent
                from mart.player_match_summary pms
                inner join mart.fact_matches fm
                  on fm.match_id = pms.match_id
                where pms.player_id = %s
                  and {where_sql}
            ),
            filtered_scoped as (
                select *
                from scoped
                where (%s::int is null or rn_recent <= %s)
            )
            select
                to_char(date_trunc('month', fs.match_date), 'YYYY-MM') as period_key,
                extract(year from fs.match_date)::int as period_year,
                extract(month from fs.match_date)::int as period_month,
                count(distinct fs.match_id)::int as matches_played,
                sum(fs.minutes_played)::numeric as minutes_played,
                sum(fs.goals)::numeric as goals,
                sum(fs.assists)::numeric as assists,
                sum(fs.shots_total)::numeric as shots_total,
                sum(fs.shots_on_goal)::numeric as shots_on_target,
                sum(fs.passes_total)::numeric as passes_attempted,
                avg(fs.rating)::numeric as rating
            from filtered_scoped fs
            group by
                date_trunc('month', fs.match_date),
                extract(year from fs.match_date),
                extract(month from fs.match_date)
            order by date_trunc('month', fs.match_date) desc;
            """,
            [
                player_id,
                *where_params,
                global_filters.last_n,
                global_filters.last_n,
            ],
        )
        minutes_played = float(summary_row.get("minutes_played") or 0)
        goals = float(summary_row.get("goals") or 0)
        assists = float(summary_row.get("assists") or 0)
        shots_total = float(summary_row.get("shots_total") or 0)
        shots_on_target = float(summary_row.get("shots_on_target") or 0)
        passes_attempted = float(summary_row.get("passes_attempted") or 0)
        yellow_cards = float(summary_row.get("yellow_cards") or 0)
        red_cards = float(summary_row.get("red_cards") or 0)
        stats_payload = {
            "minutesPerMatch": _safe_divide(minutes_played, int(summary_row.get("matches_played") or 0)),
            "goalsPer90": _safe_divide(goals * 90, minutes_played),
            "assistsPer90": _safe_divide(assists * 90, minutes_played),
            "goalContributionsPer90": _safe_divide((goals + assists) * 90, minutes_played),
            "shotsPer90": _safe_divide(shots_total * 90, minutes_played),
            "shotsOnTargetPer90": _safe_divide(shots_on_target * 90, minutes_played),
            "shotsOnTargetPct": _safe_divide(shots_on_target * 100, shots_total),
            "passesAttemptedPer90": _safe_divide(passes_attempted * 90, minutes_played),
            "yellowCardsPer90": _safe_divide(yellow_cards * 90, minutes_played),
            "redCardsPer90": _safe_divide(red_cards * 90, minutes_played),
            "trend": [
                {
                    "periodKey": row.get("period_key"),
                    "label": f"{int(row.get('period_month') or 0):02d}/{int(row.get('period_year') or 0)}",
                    "matchesPlayed": int(row.get("matches_played") or 0),
                    "minutesPlayed": float(row.get("minutes_played") or 0),
                    "goals": float(row.get("goals") or 0),
                    "assists": float(row.get("assists") or 0),
                    "shotsTotal": float(row.get("shots_total") or 0),
                    "shotsOnTarget": float(row.get("shots_on_target") or 0),
                    "passesAttempted": float(row.get("passes_attempted") or 0),
                    "rating": float(row["rating"]) if row.get("rating") is not None else None,
                }
                for row in stats_rows
            ],
        }
        stats_coverage = _with_coverage_label(coverage, "Player stats coverage")

    overview_coverage = _with_coverage_label(coverage, "Player overview coverage")
    matches_coverage = _with_coverage_label(coverage, "Player matches coverage")

    player_payload = {
        "playerId": str(player_ref["player_id"]),
        "playerName": player_ref["player_name"],
        "teamId": str(summary_row["team_id"]) if summary_row.get("team_id") is not None else None,
        "teamName": summary_row.get("team_name"),
        "position": summary_row.get("position_name"),
        "nationality": player_ref.get("nationality"),
        "lastMatchAt": summary_row.get("last_match_date"),
    }
    summary_payload = {
        "matchesPlayed": int(summary_row.get("matches_played") or 0),
        "minutesPlayed": float(summary_row.get("minutes_played") or 0),
        "goals": float(summary_row.get("goals") or 0),
        "assists": float(summary_row.get("assists") or 0),
        "shotsTotal": float(summary_row.get("shots_total") or 0),
        "shotsOnTarget": float(summary_row.get("shots_on_target") or 0),
        "passesCompleted": None,
        "passesAttempted": float(summary_row.get("passes_attempted") or 0),
        "passAccuracyPct": None,
        "yellowCards": float(summary_row.get("yellow_cards") or 0),
        "redCards": float(summary_row.get("red_cards") or 0),
        "rating": float(summary_row["rating"]) if summary_row.get("rating") is not None else None,
    }

    data: dict[str, Any] = {
        "player": player_payload,
        "summary": summary_payload,
        "sectionCoverage": {
            "overview": overview_coverage,
        },
    }
    if includeRecentMatches:
        data["recentMatches"] = recent_matches or []
        data["sectionCoverage"]["matches"] = matches_coverage
    if includeHistory:
        data["history"] = history_payload or []
        data["sectionCoverage"]["history"] = history_coverage or {
            "status": "unknown",
            "label": "Player history coverage",
        }
    if includeStats:
        data["stats"] = stats_payload or {
            "minutesPerMatch": None,
            "goalsPer90": None,
            "assistsPer90": None,
            "goalContributionsPer90": None,
            "shotsPer90": None,
            "shotsOnTargetPer90": None,
            "shotsOnTargetPct": None,
            "passesAttemptedPer90": None,
            "yellowCardsPer90": None,
            "redCardsPer90": None,
            "trend": [],
        }
        data["sectionCoverage"]["stats"] = stats_coverage or {
            "status": "unknown",
            "label": "Player stats coverage",
        }

    aggregate_coverage = _merge_coverages(
        "Player profile coverage",
        [
            section_coverage
            for section_coverage in [
                overview_coverage,
                matches_coverage if includeRecentMatches else None,
                history_coverage if includeHistory else None,
                stats_coverage if includeStats else None,
            ]
            if section_coverage is not None
        ],
    )

    return build_api_response(
        data,
        request_id=_request_id(request),
        coverage=aggregate_coverage,
    )
