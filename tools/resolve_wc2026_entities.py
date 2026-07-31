from __future__ import annotations

import argparse
import csv
import json
import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import psycopg


COMPETITION_KEY = "fifa_world_cup_mens"
SEASON_LABEL = "2026"
EDITION_KEY = f"{COMPETITION_KEY}__{SEASON_LABEL}"
DEFAULT_REPORT = Path("artifacts/wc2026_entity_resolution.json")

TEAM_ALIASES = {
    "usa": "united states",
    "us": "united states",
    "ir iran": "iran",
    "czechia": "czech republic",
    "turkiye": "turkey",
    "turkiye": "turkey",
}


def normalize_name(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = normalized.encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()


def _tokens(value: str | None) -> set[str]:
    return set(normalize_name(value).split())


def _date(value: Any) -> str:
    return str(value or "")[:10]


def _is_token_subset(source_tokens: set[str], candidate_tokens: set[str]) -> bool:
    if len(source_tokens) < 2 or len(candidate_tokens) < 2:
        return False
    return source_tokens <= candidate_tokens or candidate_tokens <= source_tokens


def _player_candidate_score(
    source_name: str,
    source_dob: str | None,
    candidate: dict[str, Any],
) -> dict[str, Any] | None:
    source_normalized = normalize_name(source_name)
    source_tokens = set(source_normalized.split())
    source_dob = _date(source_dob)
    best: dict[str, Any] | None = None

    for candidate_name in candidate.get("names", []):
        candidate_normalized = normalize_name(candidate_name)
        candidate_tokens = set(candidate_normalized.split())
        dob_match = bool(source_dob and source_dob in candidate.get("date_of_births", []))
        exact_name = source_normalized == candidate_normalized
        token_subset = _is_token_subset(source_tokens, candidate_tokens)
        ratio = SequenceMatcher(None, source_normalized, candidate_normalized).ratio()

        if exact_name and dob_match:
            score, method = 120, "exact_name_dob"
        elif token_subset and dob_match:
            score, method = 110, "token_subset_dob"
        elif exact_name:
            score, method = 100, "exact_name"
        elif token_subset and ratio >= 0.72:
            score, method = 85, "token_subset"
        elif len(source_tokens) >= 2 and len(candidate_tokens) >= 2 and ratio >= 0.92 and dob_match:
            score, method = 95, "fuzzy_name_dob"
        else:
            continue

        item = {
            "score": score,
            "method": method,
            "matched_name": candidate_name,
            "name_ratio": round(ratio, 4),
            "date_of_birth_match": dob_match,
            "candidate": candidate,
        }
        if best is None or (item["score"], item["name_ratio"]) > (
            best["score"], best["name_ratio"]
        ):
            best = item

    return best


def match_player(
    source_name: str,
    source_dob: str | None,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    scored: dict[tuple[Any, Any], dict[str, Any]] = {}
    for candidate in candidates:
        item = _player_candidate_score(source_name, source_dob, candidate)
        if item is None:
            continue
        key = (candidate.get("sportmonks_player_id"), candidate.get("wc_player_id"))
        previous = scored.get(key)
        if previous is None or item["score"] > previous["score"]:
            scored[key] = item

    ranked = sorted(
        scored.values(),
        key=lambda item: (
            item["score"],
            item["name_ratio"],
            str(item["candidate"].get("sportmonks_player_id")),
        ),
        reverse=True,
    )
    if not ranked:
        return {"status": "unmatched", "candidates": []}

    top_score = ranked[0]["score"]
    top = [item for item in ranked if item["score"] == top_score]
    candidate_payload = [
        {
            **item["candidate"],
            "score": item["score"],
            "method": item["method"],
            "matched_name": item["matched_name"],
            "name_ratio": item["name_ratio"],
            "date_of_birth_match": item["date_of_birth_match"],
        }
        for item in ranked[:5]
    ]
    if len(top) > 1:
        return {"status": "ambiguous", "candidates": candidate_payload}

    best = top[0]
    if best["method"] in {"exact_name_dob", "token_subset_dob", "exact_name"}:
        return {
            "status": "resolved",
            "candidate": best["candidate"],
            "method": best["method"],
            "score": best["score"],
            "candidates": candidate_payload,
        }
    return {"status": "review", "candidates": candidate_payload}


def _team_key(value: str | None) -> str:
    normalized = normalize_name(value)
    return TEAM_ALIASES.get(normalized, normalized)


def match_team(source_name: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    source_key = _team_key(source_name)
    matches = []
    for candidate in candidates:
        keys = {_team_key(name) for name in candidate.get("names", [])}
        if source_key in keys:
            matches.append(candidate)

    if not matches:
        return {"status": "unmatched", "candidates": []}
    if len(matches) > 1:
        return {"status": "ambiguous", "candidates": matches}
    return {"status": "resolved", "candidate": matches[0], "method": "canonical_name_or_alias", "candidates": matches}


def _candidate_for_player(candidates: dict[int, dict[str, Any]], sportmonks_id: int) -> dict[str, Any]:
    return candidates.setdefault(
        sportmonks_id,
        {
            "sportmonks_player_id": sportmonks_id,
            "wc_player_id": None,
            "names": [],
            "date_of_births": [],
        },
    )


def _append_unique(values: list[Any], value: Any) -> None:
    if value is not None and value not in values:
        values.append(value)


def load_player_candidates(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    candidates: dict[int, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select wc_player_id, sportmonks_player_id
            from raw.wc_player_identity_map
            where match_confidence = 'confirmed'
              and sportmonks_player_id is not null;
            """
        )
        for wc_player_id, sportmonks_player_id in cur.fetchall():
            candidate = _candidate_for_player(candidates, int(sportmonks_player_id))
            candidate["wc_player_id"] = int(wc_player_id)

        cur.execute(
            """
            select x.local_player_id, p.name, p.date_of_birth_raw, d.player_name
            from control.tm_player_xref x
            join raw.tm_players p on p.player_id = x.tm_player_id
            left join mart.dim_player d on d.player_id = x.local_player_id
            where x.local_player_id is not null;
            """
        )
        for local_id, source_name, date_of_birth, canonical_name in cur.fetchall():
            candidate = _candidate_for_player(candidates, int(local_id))
            _append_unique(candidate["names"], source_name)
            _append_unique(candidate["names"], canonical_name)
            _append_unique(candidate["date_of_births"], _date(date_of_birth))

        cur.execute("select player_id, player_name from mart.dim_player where player_id is not null;")
        for player_id, player_name in cur.fetchall():
            candidate = candidates.get(int(player_id))
            if candidate is not None:
                _append_unique(candidate["names"], player_name)

        cur.execute(
            """
            select m.sportmonks_player_id, s.player_name
            from raw.wc_player_identity_map m
            join raw.wc_squads s on s.player_id = m.wc_player_id
            where m.match_confidence = 'confirmed'
              and m.sportmonks_player_id is not null;
            """
        )
        for sportmonks_id, player_name in cur.fetchall():
            _append_unique(candidates[int(sportmonks_id)]["names"], player_name)

    return list(candidates.values())


def load_team_candidates(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    candidates: dict[int, dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            select wc_team_id, sportmonks_team_id, wc_display_slug
            from raw.wc_team_identity_map
            where confidence = 'confirmed'
              and sportmonks_team_id is not null;
            """
        )
        for wc_team_id, sportmonks_team_id, display_slug in cur.fetchall():
            candidate = {
                "wc_team_id": int(wc_team_id),
                "sportmonks_team_id": int(sportmonks_team_id),
                "names": [],
            }
            slug_name = str(display_slug or "").removeprefix("world-cup-").replace("-", " ")
            _append_unique(candidate["names"], slug_name)
            candidates[int(wc_team_id)] = candidate

        cur.execute(
            """
            select m.wc_team_id, f.home_team_name
            from raw.wc_team_identity_map m
            join raw.fixtures f on f.home_team_id = m.wc_team_id
            where m.confidence = 'confirmed'
            union
            select m.wc_team_id, f.away_team_name
            from raw.wc_team_identity_map m
            join raw.fixtures f on f.away_team_id = m.wc_team_id
            where m.confidence = 'confirmed';
            """
        )
        for wc_team_id, team_name in cur.fetchall():
            if int(wc_team_id) in candidates:
                _append_unique(candidates[int(wc_team_id)]["names"], team_name)

    return list(candidates.values())


def _source_players(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select distinct on (source_player_id)
                source_player_id,
                player_id,
                player_name,
                payload->'source_payload'->>'date_of_birth' as date_of_birth
            from raw.wc_squads
            where competition_key = %s
              and season_label = %s
            order by source_player_id, updated_at desc;
            """,
            (COMPETITION_KEY, SEASON_LABEL),
        )
        return [
            {
                "source_player_id": str(source_player_id),
                "current_wc_player_id": int(player_id),
                "player_name": player_name,
                "date_of_birth": _date(date_of_birth),
            }
            for source_player_id, player_id, player_name, date_of_birth in cur.fetchall()
        ]


def _source_teams(conn: psycopg.Connection[Any]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            with source_teams as (
                select home_team_id as team_id, home_team_name as team_name
                from raw.fixtures
                where competition_key = %s and season_label = %s
                union
                select away_team_id as team_id, away_team_name as team_name
                from raw.fixtures
                where competition_key = %s and season_label = %s
            )
            select team_id, max(team_name) as team_name
            from source_teams
            group by team_id
            order by team_id;
            """,
            (COMPETITION_KEY, SEASON_LABEL, COMPETITION_KEY, SEASON_LABEL),
        )
        return [{"source_team_id": int(team_id), "team_name": team_name} for team_id, team_name in cur.fetchall()]


def _signals(result: dict[str, Any]) -> list[str]:
    method = result.get("method", "")
    signals = ["normalized_name"]
    if "dob" in method:
        signals.append("date_of_birth")
    if method == "token_subset_dob":
        signals.append("canonical_name_token_subset")
    return signals


def _player_report_row(source: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    candidate = result.get("candidate") or {}
    return {
        "entity_type": "player",
        "source_id": source["source_player_id"],
        "source_name": source["player_name"],
        "source_date_of_birth": source["date_of_birth"],
        "status": result["status"],
        "method": result.get("method"),
        "score": result.get("score"),
        "sportmonks_player_id": candidate.get("sportmonks_player_id"),
        "canonical_wc_player_id": candidate.get("wc_player_id"),
        "matched_name": (result.get("candidates") or [{}])[0].get("matched_name"),
        "candidates": result.get("candidates", []),
    }


def _team_report_row(source: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    candidate = result.get("candidate") or {}
    return {
        "entity_type": "team",
        "source_id": source["source_team_id"],
        "source_name": source["team_name"],
        "status": result["status"],
        "method": result.get("method"),
        "sportmonks_team_id": candidate.get("sportmonks_team_id"),
        "canonical_wc_team_id": candidate.get("wc_team_id"),
        "candidates": result.get("candidates", []),
    }


def _update_player_rows(cur: psycopg.Cursor[Any], source_id: int, canonical_id: int) -> None:
    for table in ("wc_squads", "wc_goals", "wc_bookings", "wc_substitutions"):
        cur.execute(
            f"""
            update raw.{table}
            set player_id = %s
            where edition_key = %s
              and player_id = %s;
            """,
            (canonical_id, EDITION_KEY, source_id),
        )


def _update_team_rows(cur: psycopg.Cursor[Any], source_id: int, canonical_id: int) -> None:
    cur.execute(
        """
        update raw.fixtures
        set home_team_id = case when home_team_id = %s then %s else home_team_id end,
            away_team_id = case when away_team_id = %s then %s else away_team_id end
        where competition_key = %s and season_label = %s;
        """,
        (source_id, canonical_id, source_id, canonical_id, COMPETITION_KEY, SEASON_LABEL),
    )
    for table in ("standings_snapshots", "wc_squads", "wc_goals", "wc_bookings", "wc_substitutions"):
        cur.execute(
            f"""
            update raw.{table}
            set team_id = %s
            where competition_key = %s
              and season_label = %s
              and team_id = %s;
            """,
            (canonical_id, COMPETITION_KEY, SEASON_LABEL, source_id),
        )
    cur.execute(
        """
        update raw.wc_goals
        set player_team_id = %s
        where edition_key = %s and player_team_id = %s;
        """,
        (canonical_id, EDITION_KEY, source_id),
    )


def resolve_entities(conn: psycopg.Connection[Any], apply: bool = False) -> dict[str, Any]:
    player_candidates = load_player_candidates(conn)
    team_candidates = load_team_candidates(conn)
    player_rows = _source_players(conn)
    team_rows = _source_teams(conn)

    player_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in player_candidates:
        seen_tokens: set[str] = set()
        for name in candidate.get("names", []):
            for token in _tokens(name):
                if token not in seen_tokens:
                    player_index[token].append(candidate)
                    seen_tokens.add(token)

    player_report = []
    player_results = []
    for source in player_rows:
        source_candidates: dict[tuple[Any, Any], dict[str, Any]] = {}
        for token in _tokens(source["player_name"]):
            for candidate in player_index.get(token, []):
                source_candidates[
                    (candidate.get("sportmonks_player_id"), candidate.get("wc_player_id"))
                ] = candidate
        result = match_player(
            source["player_name"],
            source["date_of_birth"],
            list(source_candidates.values()),
        )
        player_report.append(_player_report_row(source, result))
        player_results.append((source, result))

    team_report = []
    team_results = []
    for source in team_rows:
        result = match_team(source["team_name"], team_candidates)
        team_report.append(_team_report_row(source, result))
        team_results.append((source, result))

    if apply:
        with conn.transaction():
            with conn.cursor() as cur:
                for source, result in player_results:
                    if result["status"] != "resolved":
                        continue
                    candidate = result["candidate"]
                    source_id = int(source["current_wc_player_id"])
                    sportmonks_id = candidate.get("sportmonks_player_id")
                    if sportmonks_id is None:
                        continue
                    canonical_id = candidate.get("wc_player_id")
                    if canonical_id is not None and int(canonical_id) != source_id:
                        _update_player_rows(cur, source_id, int(canonical_id))
                        cur.execute("delete from raw.wc_player_identity_map where wc_player_id = %s", (source_id,))
                        source_id = int(canonical_id)
                    cur.execute(
                        """
                        update raw.wc_player_identity_map
                        set sportmonks_player_id = %s,
                            match_confidence = 'confirmed',
                            match_signals = %s::jsonb,
                            match_score = %s,
                            match_method = %s,
                            audited_by = 'script',
                            audit_notes = %s,
                            updated_at = now()
                        where wc_player_id = %s;
                        """,
                        (
                            int(sportmonks_id),
                            json.dumps(_signals(result)),
                            result.get("score"),
                            result.get("method"),
                            "2026 source entity auto-resolved against canonical player inventory",
                            source_id,
                        ),
                    )

                for source, result in team_results:
                    if result["status"] != "resolved":
                        continue
                    candidate = result["candidate"]
                    canonical_id = int(candidate["wc_team_id"])
                    source_id = int(source["source_team_id"])
                    if canonical_id != source_id:
                        _update_team_rows(cur, source_id, canonical_id)
                        cur.execute("delete from raw.wc_team_identity_map where wc_team_id = %s", (source_id,))
                        cur.execute(
                            """
                            insert into raw.wc_team_identity_map (
                                wc_team_id, wc_display_slug, sportmonks_team_id, confidence, status,
                                created_at, updated_at
                            )
                            select wc_team_id, wc_display_slug, sportmonks_team_id, confidence, status,
                                   created_at, now()
                            from raw.wc_team_identity_map
                            where wc_team_id = %s
                            on conflict (wc_team_id) do update set updated_at = excluded.updated_at;
                            """,
                            (canonical_id,),
                        )

        with conn.cursor() as cur:
            cur.execute("analyze raw.wc_player_identity_map;")
            cur.execute("analyze raw.wc_team_identity_map;")
            cur.execute("analyze raw.wc_squads;")
            cur.execute("analyze raw.wc_goals;")
            cur.execute("analyze raw.fixtures;")

    return {
        "edition_key": EDITION_KEY,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "applied": apply,
        "summary": {
            "players_total": len(player_report),
            "players_resolved": sum(row["status"] == "resolved" for row in player_report),
            "players_review": sum(row["status"] == "review" for row in player_report),
            "players_ambiguous": sum(row["status"] == "ambiguous" for row in player_report),
            "players_unmatched": sum(row["status"] == "unmatched" for row in player_report),
            "teams_total": len(team_report),
            "teams_resolved": sum(row["status"] == "resolved" for row in team_report),
            "teams_ambiguous": sum(row["status"] == "ambiguous" for row in team_report),
            "teams_unmatched": sum(row["status"] == "unmatched" for row in team_report),
        },
        "players": player_report,
        "teams": team_report,
    }


def _dsn() -> str:
    if value := os.getenv("FOOTBALL_PG_DSN") or os.getenv("DATABASE_URL"):
        return value
    user = os.getenv("POSTGRES_USER", "football")
    password = os.getenv("POSTGRES_PASSWORD", "football")
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "football_dw")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve FIFA World Cup 2026 source entities against canonical inventories.")
    parser.add_argument("--apply", action="store_true", help="apply only deterministic resolutions")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    with psycopg.connect(_dsn()) as conn:
        report = resolve_entities(conn, apply=args.apply)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    review_path = args.report.with_suffix(".review.csv")
    review_rows = [
        row
        for row in [*report["players"], *report["teams"]]
        if row["status"] in {"review", "ambiguous"}
    ]
    review_fields = [
        "entity_type",
        "source_id",
        "source_name",
        "source_date_of_birth",
        "status",
        "method",
        "score",
        "sportmonks_player_id",
        "sportmonks_team_id",
        "canonical_wc_player_id",
        "canonical_wc_team_id",
        "matched_name",
        "candidates",
    ]
    with review_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=review_fields)
        writer.writeheader()
        for row in review_rows:
            output = {field: row.get(field) for field in review_fields}
            output["candidates"] = json.dumps(row.get("candidates", []), ensure_ascii=False)
            writer.writerow(output)
    print(json.dumps(report["summary"], ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
