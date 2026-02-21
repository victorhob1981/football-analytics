from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, Literal

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response, build_coverage_from_counts, build_pagination
from ..core.errors import AppError
from ..core.filters import GlobalFilters, VenueFilter, append_fact_match_filters, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/rankings", tags=["rankings"])

SortDirection = Literal["asc", "desc"]
FreshnessClass = Literal["season", "fast"]

RANKING_CONFIG: dict[str, dict[str, Any]] = {
    "player-goals": {"metricKey": "goals", "domain": "player", "valueColumn": "goals", "defaultSort": "desc"},
    "player-assists": {"metricKey": "assists", "domain": "player", "valueColumn": "assists", "defaultSort": "desc"},
    "player-shots-total": {
        "metricKey": "shots_total",
        "domain": "player",
        "valueColumn": "shots_total",
        "defaultSort": "desc",
    },
    "player-shots-on-target": {
        "metricKey": "shots_on_target",
        "domain": "player",
        "valueColumn": "shots_on_goal",
        "defaultSort": "desc",
    },
    "player-pass-accuracy": {
        "metricKey": "pass_accuracy_pct",
        "domain": "player",
        "unsupported": True,
        "defaultSort": "desc",
    },
    "player-rating": {"metricKey": "player_rating", "domain": "player", "valueColumn": "rating", "defaultSort": "desc"},
    "player-yellow-cards": {
        "metricKey": "yellow_cards",
        "domain": "player",
        "valueColumn": "yellow_cards",
        "defaultSort": "desc",
    },
    "team-possession": {
        "metricKey": "team_possession_pct",
        "domain": "team_possession",
        "defaultSort": "desc",
    },
    "team-pass-accuracy": {
        "metricKey": "team_pass_accuracy_pct",
        "domain": "team_pass_accuracy",
        "defaultSort": "desc",
    },
}


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _get_ranking_config(ranking_type: str) -> dict[str, Any]:
    config = RANKING_CONFIG.get(ranking_type)
    if config is None:
        raise AppError(
            message="Invalid ranking type.",
            code="INVALID_RANKING_TYPE",
            status=400,
            details={"rankingType": ranking_type},
        )
    return config


def _player_scope_filters_sql(filters: GlobalFilters) -> tuple[str, list[Any]]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)
    if filters.venue == VenueFilter.home:
        where_clauses.append("pms.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        where_clauses.append("pms.team_id = fm.away_team_id")
    return " and ".join(where_clauses), params


def _player_ranking_coverage(filters: GlobalFilters) -> dict[str, Any]:
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
    return build_coverage_from_counts(
        int(row.get("available_count") or 0),
        int(row.get("total_count") or 0),
        "Player ranking coverage",
    )


def _team_possession_coverage(filters: GlobalFilters) -> dict[str, Any]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)

    venue_selects: list[str] = []
    if filters.venue in (VenueFilter.all, VenueFilter.home):
        venue_selects.append(
            "select fm.match_id, fm.home_team_id as team_id, fm.home_possession::numeric as possession from scoped_matches fm"
        )
    if filters.venue in (VenueFilter.all, VenueFilter.away):
        venue_selects.append(
            "select fm.match_id, fm.away_team_id as team_id, fm.away_possession::numeric as possession from scoped_matches fm"
        )

    query = f"""
        with scoped_matches as (
            select fm.match_id, fm.home_team_id, fm.away_team_id, fm.home_possession, fm.away_possession
            from mart.fact_matches fm
            where {" and ".join(where_clauses)}
        ),
        team_rows as (
            {" union all ".join(venue_selects)}
        )
        select
            count(*) filter (where possession is not null) as available_count,
            count(*) as total_count
        from team_rows;
    """
    row = db_client.fetch_one(query, params) or {}
    return build_coverage_from_counts(
        int(row.get("available_count") or 0),
        int(row.get("total_count") or 0),
        "Team possession coverage",
    )


def _team_pass_accuracy_coverage(filters: GlobalFilters) -> dict[str, Any]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)
    if filters.venue == VenueFilter.home:
        where_clauses.append("ms.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        where_clauses.append("ms.team_id = fm.away_team_id")

    query = f"""
        with scoped as (
            select ms.fixture_id, ms.team_id, ms.passes_pct
            from raw.match_statistics ms
            inner join mart.fact_matches fm
              on fm.match_id = ms.fixture_id
            where {" and ".join(where_clauses)}
        ),
        expected as (
            select
                case
                    when %s = 'all' then count(*) * 2
                    else count(*)
                end as total_expected
            from mart.fact_matches fm
            where {" and ".join([c for c in where_clauses if not c.startswith("ms.")])}
        )
        select
            (select count(*) filter (where passes_pct is not null) from scoped) as available_count,
            (select total_expected from expected) as total_count;
    """
    params_with_venue = [*params, filters.venue.value, *params]
    row = db_client.fetch_one(query, params_with_venue) or {}
    return build_coverage_from_counts(
        int(row.get("available_count") or 0),
        int(row.get("total_count") or 0),
        "Team pass accuracy coverage",
    )


def _fetch_player_ranking_rows(
    *,
    ranking_config: dict[str, Any],
    filters: GlobalFilters,
    search: str | None,
    min_sample_value: int | None,
    page: int,
    page_size: int,
    sort_direction: str,
) -> tuple[list[dict[str, Any]], int]:
    where_sql, where_params = _player_scope_filters_sql(filters)
    value_column = ranking_config["valueColumn"]
    metric_expression = {
        "rating": "avg(fs.rating)::numeric",
        "goals": "sum(fs.goals)::numeric",
        "assists": "sum(fs.assists)::numeric",
        "shots_total": "sum(fs.shots_total)::numeric",
        "shots_on_goal": "sum(fs.shots_on_goal)::numeric",
        "yellow_cards": "sum(fs.yellow_cards)::numeric",
    }[value_column]

    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    offset = (page - 1) * page_size
    order_dir = "asc" if sort_direction == "asc" else "desc"

    query = f"""
        with scoped as (
            select
                pms.player_id,
                pms.player_name,
                pms.team_id,
                pms.team_name,
                pms.match_id,
                pms.match_date,
                coalesce(pms.minutes_played, 0) as minutes_played,
                coalesce(pms.goals, 0) as goals,
                coalesce(pms.assists, 0) as assists,
                coalesce(pms.shots_total, 0) as shots_total,
                coalesce(pms.shots_on_goal, 0) as shots_on_goal,
                coalesce(pms.yellow_cards, 0) as yellow_cards,
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
                max(fs.team_id) as team_id,
                max(fs.team_name) as team_name,
                count(distinct fs.match_id) as matches_played,
                sum(fs.minutes_played)::numeric as minutes_played,
                {metric_expression} as metric_value
            from filtered_scoped fs
            group by fs.player_id
        ),
        constrained as (
            select *
            from aggregated
            where (%s::text is null or player_name ilike %s)
              and (%s::numeric is null or minutes_played >= %s)
        ),
        ranked as (
            select
                c.player_id,
                c.player_name,
                c.team_id,
                c.team_name,
                c.matches_played,
                c.minutes_played,
                c.metric_value,
                dense_rank() over (order by c.metric_value {order_dir} nulls last, c.player_id asc) as rank
            from constrained c
        )
        select
            r.*,
            count(*) over() as _total_count
        from ranked r
        order by r.rank asc, r.player_id asc
        limit %s offset %s;
    """

    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            filters.last_n,
            filters.last_n,
            search_pattern,
            search_pattern,
            min_sample_value,
            min_sample_value,
            page_size,
            offset,
        ],
    )
    total_count = int(rows[0]["_total_count"]) if rows else 0
    return rows, total_count


def _fetch_team_possession_rows(
    *,
    filters: GlobalFilters,
    search: str | None,
    min_sample_value: int | None,
    page: int,
    page_size: int,
    sort_direction: str,
) -> tuple[list[dict[str, Any]], int]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)
    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    offset = (page - 1) * page_size
    order_dir = "asc" if sort_direction == "asc" else "desc"

    venue_selects: list[str] = []
    if filters.venue in (VenueFilter.all, VenueFilter.home):
        venue_selects.append(
            """
            select
                fm.match_id,
                fm.date_day as match_date,
                fm.home_team_id as team_id,
                dt.team_name,
                fm.home_possession::numeric as metric_value
            from scoped_matches fm
            left join mart.dim_team dt
              on dt.team_id = fm.home_team_id
            """
        )
    if filters.venue in (VenueFilter.all, VenueFilter.away):
        venue_selects.append(
            """
            select
                fm.match_id,
                fm.date_day as match_date,
                fm.away_team_id as team_id,
                dt.team_name,
                fm.away_possession::numeric as metric_value
            from scoped_matches fm
            left join mart.dim_team dt
              on dt.team_id = fm.away_team_id
            """
        )

    query = f"""
        with scoped_matches as (
            select fm.match_id, fm.date_day, fm.home_team_id, fm.away_team_id, fm.home_possession, fm.away_possession
            from mart.fact_matches fm
            where {" and ".join(where_clauses)}
        ),
        team_rows as (
            {" union all ".join(venue_selects)}
        ),
        ranked_rows as (
            select
                tr.*,
                row_number() over (partition by tr.team_id order by tr.match_date desc, tr.match_id desc) as rn_recent
            from team_rows tr
        ),
        filtered_rows as (
            select *
            from ranked_rows
            where (%s::int is null or rn_recent <= %s)
        ),
        aggregated as (
            select
                fr.team_id,
                max(fr.team_name) as team_name,
                count(*) as matches_played,
                avg(fr.metric_value)::numeric as metric_value
            from filtered_rows fr
            group by fr.team_id
        ),
        constrained as (
            select *
            from aggregated
            where (%s::text is null or team_name ilike %s)
              and (%s::int is null or matches_played >= %s)
        ),
        ranked as (
            select
                c.*,
                dense_rank() over (order by c.metric_value {order_dir} nulls last, c.team_id asc) as rank
            from constrained c
        )
        select
            r.*,
            count(*) over() as _total_count
        from ranked r
        order by r.rank asc, r.team_id asc
        limit %s offset %s;
    """
    rows = db_client.fetch_all(
        query,
        [
            *params,
            filters.last_n,
            filters.last_n,
            search_pattern,
            search_pattern,
            min_sample_value,
            min_sample_value,
            page_size,
            offset,
        ],
    )
    total_count = int(rows[0]["_total_count"]) if rows else 0
    return rows, total_count


def _fetch_team_pass_accuracy_rows(
    *,
    filters: GlobalFilters,
    search: str | None,
    min_sample_value: int | None,
    page: int,
    page_size: int,
    sort_direction: str,
) -> tuple[list[dict[str, Any]], int]:
    where_clauses = ["1=1"]
    params: list[Any] = []
    append_fact_match_filters(where_clauses, params, alias="fm", filters=filters)
    if filters.venue == VenueFilter.home:
        where_clauses.append("ms.team_id = fm.home_team_id")
    elif filters.venue == VenueFilter.away:
        where_clauses.append("ms.team_id = fm.away_team_id")

    search_pattern = f"%{search.strip()}%" if search and search.strip() else None
    offset = (page - 1) * page_size
    order_dir = "asc" if sort_direction == "asc" else "desc"

    query = f"""
        with scoped as (
            select
                ms.fixture_id,
                ms.team_id,
                coalesce(ms.team_name, dt.team_name) as team_name,
                fm.date_day as match_date,
                ms.passes_pct::numeric as metric_value
            from raw.match_statistics ms
            inner join mart.fact_matches fm
              on fm.match_id = ms.fixture_id
            left join mart.dim_team dt
              on dt.team_id = ms.team_id
            where {" and ".join(where_clauses)}
        ),
        ranked_rows as (
            select
                s.*,
                row_number() over (partition by s.team_id order by s.match_date desc, s.fixture_id desc) as rn_recent
            from scoped s
        ),
        filtered_rows as (
            select *
            from ranked_rows
            where (%s::int is null or rn_recent <= %s)
        ),
        aggregated as (
            select
                fr.team_id,
                max(fr.team_name) as team_name,
                count(distinct fr.fixture_id) as matches_played,
                avg(fr.metric_value)::numeric as metric_value
            from filtered_rows fr
            group by fr.team_id
        ),
        constrained as (
            select *
            from aggregated
            where (%s::text is null or team_name ilike %s)
              and (%s::int is null or matches_played >= %s)
        ),
        ranked as (
            select
                c.*,
                dense_rank() over (order by c.metric_value {order_dir} nulls last, c.team_id asc) as rank
            from constrained c
        )
        select
            r.*,
            count(*) over() as _total_count
        from ranked r
        order by r.rank asc, r.team_id asc
        limit %s offset %s;
    """
    rows = db_client.fetch_all(
        query,
        [
            *params,
            filters.last_n,
            filters.last_n,
            search_pattern,
            search_pattern,
            min_sample_value,
            min_sample_value,
            page_size,
            offset,
        ],
    )
    total_count = int(rows[0]["_total_count"]) if rows else 0
    return rows, total_count


@router.get("/{rankingType}")
def get_ranking(
    rankingType: str,
    request: Request,
    competitionId: str | None = None,
    seasonId: str | None = None,
    roundId: str | None = None,
    venue: VenueFilter = VenueFilter.all,
    lastN: int | None = Query(default=None, gt=0),
    dateStart: date | None = None,
    dateEnd: date | None = None,
    dateRangeStart: date | None = None,
    dateRangeEnd: date | None = None,
    search: str | None = None,
    minSampleValue: int | None = Query(default=None, ge=0),
    freshnessClass: FreshnessClass = "season",
    page: int = Query(default=1, ge=1),
    pageSize: int = Query(default=20, ge=1, le=100),
    sortDirection: SortDirection | None = None,
) -> dict[str, Any]:
    ranking_config = _get_ranking_config(rankingType)
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
    )

    effective_sort = sortDirection or ranking_config["defaultSort"]

    if ranking_config.get("unsupported"):
        coverage = {
            "status": "unknown",
            "label": "Metric currently not materialized in DW for ranking calculation.",
        }
        return build_api_response(
            {
                "rankingId": rankingType,
                "metricKey": ranking_config["metricKey"],
                "rows": [],
                "updatedAt": None,
            },
            request_id=_request_id(request),
            pagination=build_pagination(page, pageSize, 0),
            coverage=coverage,
        )

    rows: list[dict[str, Any]]
    total_count: int
    if ranking_config["domain"] == "player":
        rows, total_count = _fetch_player_ranking_rows(
            ranking_config=ranking_config,
            filters=global_filters,
            search=search,
            min_sample_value=minSampleValue,
            page=page,
            page_size=pageSize,
            sort_direction=effective_sort,
        )
        coverage = _player_ranking_coverage(global_filters)
    elif ranking_config["domain"] == "team_possession":
        rows, total_count = _fetch_team_possession_rows(
            filters=global_filters,
            search=search,
            min_sample_value=minSampleValue,
            page=page,
            page_size=pageSize,
            sort_direction=effective_sort,
        )
        coverage = _team_possession_coverage(global_filters)
    elif ranking_config["domain"] == "team_pass_accuracy":
        rows, total_count = _fetch_team_pass_accuracy_rows(
            filters=global_filters,
            search=search,
            min_sample_value=minSampleValue,
            page=page,
            page_size=pageSize,
            sort_direction=effective_sort,
        )
        coverage = _team_pass_accuracy_coverage(global_filters)
    else:
        raise AppError(
            message="Invalid ranking domain configuration.",
            code="INTERNAL_ERROR",
            status=500,
            details={"rankingType": rankingType},
        )

    normalized_rows = [
        {
            "entityId": str(row["player_id"] if "player_id" in row else row["team_id"]),
            "entityName": row.get("player_name") or row.get("team_name"),
            "rank": int(row.get("rank") or 0),
            "metricValue": float(row["metric_value"]) if row.get("metric_value") is not None else None,
            "matchesPlayed": int(row.get("matches_played") or 0),
            "minutesPlayed": float(row["minutes_played"]) if row.get("minutes_played") is not None else None,
            "teamId": str(row["team_id"]) if row.get("team_id") is not None else None,
            "teamName": row.get("team_name"),
        }
        for row in rows
    ]

    pagination = build_pagination(page, pageSize, total_count)
    data = {
        "rankingId": rankingType,
        "metricKey": ranking_config["metricKey"],
        "rows": normalized_rows,
        "updatedAt": datetime.now(UTC).isoformat(),
        "freshnessClass": freshnessClass,
    }
    return build_api_response(
        data,
        request_id=_request_id(request),
        pagination=pagination,
        coverage=coverage,
    )
