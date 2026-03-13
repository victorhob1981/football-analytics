from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Query, Request

from ..core.contracts import build_api_response, build_coverage_from_counts
from ..core.filters import GlobalFilters, VenueFilter, validate_and_build_global_filters
from ..db.client import db_client

router = APIRouter(prefix="/api/v1/standings", tags=["standings"])


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _resolve_scope_defaults(filters: GlobalFilters) -> tuple[int | None, int | None]:
    season_id = filters.season_id
    competition_id = filters.competition_id

    if season_id is None:
        if competition_id is not None:
            resolved_season = db_client.fetch_val(
                """
                    select max(fm.season)
                    from mart.fact_matches fm
                    where fm.league_id = %s;
                """,
                [competition_id],
            )
        else:
            resolved_season = db_client.fetch_val(
                """
                    select max(fm.season)
                    from mart.fact_matches fm;
                """,
            )

        season_id = int(resolved_season) if resolved_season is not None else None

    if competition_id is None:
        resolved_competition = db_client.fetch_val(
            """
                select fm.league_id
                from mart.fact_matches fm
                where (%s::int is null or fm.season = %s)
                group by fm.league_id
                order by count(*) desc, max(fm.date_day) desc, fm.league_id asc
                limit 1;
            """,
            [season_id, season_id],
        )
        competition_id = int(resolved_competition) if resolved_competition is not None else None

    return competition_id, season_id


def _match_scope_filters_sql(
    filters: GlobalFilters,
    *,
    competition_id: int | None,
    season_id: int | None,
) -> tuple[str, list[Any]]:
    clauses = ["1=1"]
    params: list[Any] = []

    if competition_id is not None:
        clauses.append("fm.league_id = %s")
        params.append(competition_id)

    if season_id is not None:
        clauses.append("fm.season = %s")
        params.append(season_id)

    if filters.date_start is not None:
        clauses.append("fm.date_day >= %s")
        params.append(filters.date_start)

    if filters.date_end is not None:
        clauses.append("fm.date_day <= %s")
        params.append(filters.date_end)

    return " and ".join(clauses), params


def _sanitize_recent_form(raw_recent_form: Any) -> list[str]:
    if not isinstance(raw_recent_form, list):
        return []

    output: list[str] = []
    for raw in raw_recent_form:
        normalized = str(raw).strip().upper()
        if normalized in {"W", "D", "L"}:
            output.append(normalized)
    return output[:5]


def _resolve_zone(position: int, total_teams: int) -> str | None:
    if total_teams <= 0:
        return None

    if position == 1:
        return "title"
    if position <= min(6, total_teams):
        return "libertadores"
    if 7 <= position <= min(12, total_teams):
        return "sulamericana"
    if total_teams >= 4 and position >= (total_teams - 3):
        return "relegation"
    return None


@router.get("")
def get_standings(
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
    month: str | None = None,
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

    effective_competition_id, effective_season_id = _resolve_scope_defaults(global_filters)
    where_sql, where_params = _match_scope_filters_sql(
        global_filters,
        competition_id=effective_competition_id,
        season_id=effective_season_id,
    )
    venue_scope = global_filters.venue.value

    query = f"""
        with scoped_matches as (
            select
                fm.match_id,
                fm.date_day,
                fm.round_number,
                fm.home_team_id,
                fm.away_team_id,
                coalesce(fm.home_goals, 0) as home_goals,
                coalesce(fm.away_goals, 0) as away_goals,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered_matches as (
            select *
            from scoped_matches sm
            where (%s::int is null or sm.rn_recent <= %s)
        ),
        effective_round as (
            select coalesce(%s::int, max(sm.round_number)) as round_limit
            from filtered_matches sm
        ),
        team_matches as (
            select
                fm.match_id,
                fm.date_day,
                fm.round_number,
                fm.home_team_id as team_id,
                'home'::text as venue_scope,
                fm.home_goals as goals_for,
                fm.away_goals as goals_against,
                case
                    when fm.home_goals > fm.away_goals then 'W'
                    when fm.home_goals = fm.away_goals then 'D'
                    else 'L'
                end as result
            from filtered_matches fm
            cross join effective_round er
            where er.round_limit is not null
              and fm.round_number is not null
              and fm.round_number <= er.round_limit

            union all

            select
                fm.match_id,
                fm.date_day,
                fm.round_number,
                fm.away_team_id as team_id,
                'away'::text as venue_scope,
                fm.away_goals as goals_for,
                fm.home_goals as goals_against,
                case
                    when fm.away_goals > fm.home_goals then 'W'
                    when fm.away_goals = fm.home_goals then 'D'
                    else 'L'
                end as result
            from filtered_matches fm
            cross join effective_round er
            where er.round_limit is not null
              and fm.round_number is not null
              and fm.round_number <= er.round_limit
        ),
        venue_filtered as (
            select *
            from team_matches tm
            where (%s::text = 'all' or tm.venue_scope = %s)
        ),
        recent_form_rows as (
            select
                tm.*,
                row_number() over (
                    partition by tm.team_id
                    order by tm.date_day desc nulls last, tm.match_id desc
                ) as rn_form
            from venue_filtered tm
        ),
        recent_form as (
            select
                team_id,
                array_agg(result order by date_day desc nulls last, match_id desc)
                    filter (where rn_form <= 5) as recent_form
            from recent_form_rows
            group by team_id
        ),
        aggregated as (
            select
                tm.team_id,
                count(*)::int as matches_played,
                sum(case when tm.result = 'W' then 1 else 0 end)::int as wins,
                sum(case when tm.result = 'D' then 1 else 0 end)::int as draws,
                sum(case when tm.result = 'L' then 1 else 0 end)::int as losses,
                sum(tm.goals_for)::int as goals_for,
                sum(tm.goals_against)::int as goals_against,
                sum(case when tm.result = 'W' then 3 when tm.result = 'D' then 1 else 0 end)::int as points,
                (sum(tm.goals_for) - sum(tm.goals_against))::int as goal_difference
            from venue_filtered tm
            group by tm.team_id
        ),
        ranked as (
            select
                ag.team_id::text as team_id,
                coalesce(dt.team_name, ag.team_id::text) as team_name,
                dt.logo_url as logo_url,
                dense_rank() over (
                    order by ag.points desc, ag.goal_difference desc, ag.goals_for desc, ag.team_id asc
                )::int as position,
                ag.points,
                ag.matches_played,
                ag.wins,
                ag.draws,
                ag.losses,
                ag.goals_for,
                ag.goals_against,
                ag.goal_difference,
                rf.recent_form
            from aggregated ag
            left join mart.dim_team dt
              on dt.team_id = ag.team_id
            left join recent_form rf
              on rf.team_id = ag.team_id
        )
        select *
        from ranked
        order by position asc, team_id asc;
    """

    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
            global_filters.round_id,
            venue_scope,
            venue_scope,
        ],
    )

    total_teams = len(rows)
    items = [
        {
            "teamId": row["team_id"],
            "teamName": row.get("team_name"),
            "logoUrl": row.get("logo_url"),
            "position": int(row["position"]) if row.get("position") is not None else None,
            "points": int(row["points"]) if row.get("points") is not None else None,
            "matchesPlayed": int(row["matches_played"]) if row.get("matches_played") is not None else None,
            "wins": int(row["wins"]) if row.get("wins") is not None else None,
            "draws": int(row["draws"]) if row.get("draws") is not None else None,
            "losses": int(row["losses"]) if row.get("losses") is not None else None,
            "goalsFor": int(row["goals_for"]) if row.get("goals_for") is not None else None,
            "goalsAgainst": int(row["goals_against"]) if row.get("goals_against") is not None else None,
            "goalDifference": int(row["goal_difference"]) if row.get("goal_difference") is not None else None,
            "recentForm": _sanitize_recent_form(row.get("recent_form")),
            "zone": _resolve_zone(int(row["position"]), total_teams) if row.get("position") is not None else None,
        }
        for row in rows
    ]

    coverage = build_coverage_from_counts(
        available_count=len(items),
        total_count=len(items),
        label="Standings snapshot coverage",
    )

    return build_api_response(
        {"items": items},
        request_id=_request_id(request),
        coverage=coverage,
    )


@router.get("/evolution")
def get_standings_evolution(
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
    month: str | None = None,
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

    effective_competition_id, effective_season_id = _resolve_scope_defaults(global_filters)
    where_sql, where_params = _match_scope_filters_sql(
        global_filters,
        competition_id=effective_competition_id,
        season_id=effective_season_id,
    )
    venue_scope = global_filters.venue.value

    query = f"""
        with scoped_matches as (
            select
                fm.match_id,
                fm.date_day,
                fm.round_number,
                fm.home_team_id,
                fm.away_team_id,
                coalesce(fm.home_goals, 0) as home_goals,
                coalesce(fm.away_goals, 0) as away_goals,
                row_number() over (order by fm.date_day desc, fm.match_id desc) as rn_recent
            from mart.fact_matches fm
            where {where_sql}
        ),
        filtered_matches as (
            select *
            from scoped_matches sm
            where (%s::int is null or sm.rn_recent <= %s)
        ),
        effective_round as (
            select coalesce(%s::int, max(sm.round_number)) as round_limit
            from filtered_matches sm
        ),
        team_match_rows as (
            select
                fm.round_number,
                fm.match_id,
                fm.date_day,
                fm.home_team_id as team_id,
                'home'::text as venue_scope,
                fm.home_goals as goals_for,
                fm.away_goals as goals_against,
                case when fm.home_goals > fm.away_goals then 1 else 0 end as wins_in_match,
                case
                    when fm.home_goals > fm.away_goals then 3
                    when fm.home_goals = fm.away_goals then 1
                    else 0
                end as points_in_match
            from filtered_matches fm
            cross join effective_round er
            where er.round_limit is not null
              and fm.round_number is not null
              and fm.round_number <= er.round_limit

            union all

            select
                fm.round_number,
                fm.match_id,
                fm.date_day,
                fm.away_team_id as team_id,
                'away'::text as venue_scope,
                fm.away_goals as goals_for,
                fm.home_goals as goals_against,
                case when fm.away_goals > fm.home_goals then 1 else 0 end as wins_in_match,
                case
                    when fm.away_goals > fm.home_goals then 3
                    when fm.away_goals = fm.home_goals then 1
                    else 0
                end as points_in_match
            from filtered_matches fm
            cross join effective_round er
            where er.round_limit is not null
              and fm.round_number is not null
              and fm.round_number <= er.round_limit
        ),
        venue_filtered as (
            select *
            from team_match_rows tm
            where (%s::text = 'all' or tm.venue_scope = %s)
        ),
        per_round as (
            select
                tm.round_number,
                tm.team_id,
                sum(tm.points_in_match)::int as points_in_round,
                sum(tm.goals_for)::int as goals_for_in_round,
                sum(tm.goals_for - tm.goals_against)::int as goal_diff_in_round,
                sum(tm.wins_in_match)::int as wins_in_round
            from venue_filtered tm
            group by tm.round_number, tm.team_id
        ),
        accumulated as (
            select
                pr.round_number,
                pr.team_id,
                sum(pr.points_in_round) over (
                    partition by pr.team_id
                    order by pr.round_number
                    rows between unbounded preceding and current row
                )::int as points_accumulated,
                sum(pr.goals_for_in_round) over (
                    partition by pr.team_id
                    order by pr.round_number
                    rows between unbounded preceding and current row
                )::int as goals_for_accumulated,
                sum(pr.goal_diff_in_round) over (
                    partition by pr.team_id
                    order by pr.round_number
                    rows between unbounded preceding and current row
                )::int as goal_diff_accumulated,
                sum(pr.wins_in_round) over (
                    partition by pr.team_id
                    order by pr.round_number
                    rows between unbounded preceding and current row
                )::int as wins_accumulated
            from per_round pr
        ),
        ranked as (
            select
                acc.round_number,
                acc.team_id::text as team_id,
                coalesce(dt.team_name, acc.team_id::text) as team_name,
                acc.points_accumulated,
                dense_rank() over (
                    partition by acc.round_number
                    order by
                        acc.points_accumulated desc,
                        acc.wins_accumulated desc,
                        acc.goal_diff_accumulated desc,
                        acc.goals_for_accumulated desc,
                        acc.team_id asc
                )::int as position
            from accumulated acc
            left join mart.dim_team dt
              on dt.team_id = acc.team_id
        )
        select
            round_number,
            team_id,
            team_name,
            points_accumulated as points,
            position
        from ranked
        order by round_number asc, position asc, team_id asc;
    """

    rows = db_client.fetch_all(
        query,
        [
            *where_params,
            global_filters.last_n,
            global_filters.last_n,
            global_filters.round_id,
            venue_scope,
            venue_scope,
        ],
    )

    items = [
        {
            "teamId": row["team_id"],
            "teamName": row.get("team_name"),
            "roundNumber": int(row["round_number"]) if row.get("round_number") is not None else None,
            "points": int(row["points"]) if row.get("points") is not None else None,
            "position": int(row["position"]) if row.get("position") is not None else None,
        }
        for row in rows
        if row.get("round_number") is not None
    ]

    coverage = build_coverage_from_counts(
        available_count=len(items),
        total_count=len(items),
        label="Standings evolution coverage",
    )

    return build_api_response(
        {"items": items},
        request_id=_request_id(request),
        coverage=coverage,
    )
