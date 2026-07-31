from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = ROOT / "FIFA-World-Cup-2026-Dataset-main" / "FIFA-World-Cup-2026-Dataset-main"
SOURCE_ZIP = ROOT / "FIFA-World-Cup-2026-Dataset-main.zip"
OUTPUT_DIR = ROOT / "artifacts" / "wc2026_delta"

PROVIDER = "mominullptr_wc2026"
COMPETITION_KEY = "fifa_world_cup_mens"
SEASON_LABEL = "2026"
EDITION_KEY = f"{COMPETITION_KEY}__{SEASON_LABEL}"
SEASON_NAME = "2026 FIFA Men's World Cup"

COMPETITION_ID = 344223137057272147
SEASON_ID = 9099783570221629243


def stable_id(*parts: object) -> int:
    digest = hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)
    return value or 1


def read_csv(name: str) -> list[dict[str, str]]:
    with (SOURCE_DIR / name).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def as_int(value: str | None) -> int | None:
    return int(value) if value not in (None, "") else None


def as_float(value: str | None) -> float | None:
    return float(value) if value not in (None, "") else None


def parse_minute(value: str | None) -> tuple[int | None, int]:
    if value in (None, ""):
        return None, 0
    if "+" in value:
        regulation, stoppage = value.split("+", 1)
        return int(regulation), int(stoppage)
    return int(value), 0


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def slug(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")


def source_hash() -> str:
    digest = hashlib.sha256()
    with SOURCE_ZIP.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(name: str, columns: list[str], rows: list[dict[str, object]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with (OUTPUT_DIR / name).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="raise")
        writer.writeheader()
        writer.writerows({column: row.get(column) for column in columns} for row in rows)


def main() -> None:
    if not SOURCE_DIR.is_dir() or not SOURCE_ZIP.is_file():
        raise SystemExit("Dataset or source ZIP not found.")

    now = datetime.now(timezone.utc)
    ingested_at = now.isoformat()
    run_id = f"wc2026_manual_ingestion__{now.strftime('%Y%m%dT%H%M%SZ')}"
    version = source_hash()
    source_rows = f"github-main-zip:{version[:12]}"

    teams = read_csv("teams.csv")
    venues = {int(row["venue_id"]): row for row in read_csv("venues.csv")}
    referees = {int(row["referee_id"]): row for row in read_csv("referees.csv")}
    stages = {int(row["stage_id"]): row for row in read_csv("tournament_stages.csv")}
    players = {int(row["player_id"]): row for row in read_csv("squads_and_players.csv")}
    matches = read_csv("matches.csv")
    detailed_matches = {int(row["match_id"]): row for row in read_csv("matches_detailed.csv")}
    events = read_csv("match_events.csv")

    team_by_id = {int(row["team_id"]): row for row in teams}
    team_ids = {
        team_id: stable_id(PROVIDER, "team", row["fifa_code"])
        for team_id, row in team_by_id.items()
    }
    player_ids = {
        player_id: stable_id(PROVIDER, "player", player_id)
        for player_id in players
    }
    fixture_ids = {
        int(row["match_id"]): stable_id(PROVIDER, "fixture", row["match_id"])
        for row in matches
    }
    match_internal_ids = {
        match_id: f"match__wc2026__{match_id}"
        for match_id in fixture_ids
    }
    team_internal_ids = {
        team_id: f"team__national_team__{row['fifa_code']}"
        for team_id, row in team_by_id.items()
    }
    player_internal_ids = {
        player_id: f"player__wc2026__{player_id}"
        for player_id in players
    }
    match_stage = {
        int(row["match_id"]): stages[int(row["stage_id"])]
        for row in matches
    }

    competition_payload = {
        "seed_type": "world_cup_dataset_transform",
        "count_teams": len(teams),
        "edition_key": EDITION_KEY,
        "source_name": PROVIDER,
        "format_flags": {
            "final": True,
            "final_round": False,
            "group_stage": True,
            "round_of_32": True,
            "round_of_16": True,
            "semi_finals": True,
            "quarter_finals": True,
            "third_place_match": True,
            "second_group_stage": False,
        },
        "host_country": "Canada, Mexico and United States",
        "tournament_id": "WC-2026",
        "source_dataset": "matches.csv, matches_detailed.csv, teams.csv, tournament_stages.csv, venues.csv",
        "tournament_name": SEASON_NAME,
    }

    competition_columns = [
        "provider", "season_id", "league_id", "season_year", "season_name",
        "starting_at", "ending_at", "payload", "ingested_run", "updated_at",
        "provider_league_id", "competition_key", "season_label", "provider_season_id",
        "ingested_at", "source_run_id",
    ]
    write_csv("raw_competition_seasons.csv", competition_columns, [{
        "provider": PROVIDER,
        "season_id": SEASON_ID,
        "league_id": COMPETITION_ID,
        "season_year": 2026,
        "season_name": SEASON_NAME,
        "starting_at": min(row["date"] for row in matches),
        "ending_at": max(row["date"] for row in matches),
        "payload": json_text(competition_payload),
        "ingested_run": run_id,
        "updated_at": ingested_at,
        "provider_league_id": COMPETITION_ID,
        "competition_key": COMPETITION_KEY,
        "season_label": SEASON_LABEL,
        "provider_season_id": SEASON_ID,
        "ingested_at": ingested_at,
        "source_run_id": run_id,
    }])

    fixture_columns = [
        "fixture_id", "date_utc", "timestamp", "timezone", "referee", "venue_id",
        "venue_name", "venue_city", "status_short", "status_long", "league_id",
        "league_name", "season", "round", "home_team_id", "home_team_name",
        "away_team_id", "away_team_name", "home_goals", "away_goals", "year",
        "month", "ingested_run", "date", "source_provider", "referee_id", "stage_id",
        "round_id", "attendance", "weather_description", "weather_temperature_c",
        "weather_wind_kph", "home_goals_ht", "away_goals_ht", "home_goals_ft",
        "away_goals_ft", "provider", "provider_league_id", "competition_key",
        "competition_type", "season_label", "provider_season_id", "season_name",
        "season_start_date", "season_end_date", "stage_name", "round_name", "group_name",
        "leg", "ingested_at", "source_run_id",
    ]
    fixture_rows: list[dict[str, object]] = []
    for match in matches:
        match_id = int(match["match_id"])
        stage = match_stage[match_id]
        detailed = detailed_matches[match_id]
        home = team_by_id[int(match["home_team_id"])]
        away = team_by_id[int(match["away_team_id"])]
        venue = venues[int(match["venue_id"])]
        referee = referees[int(match["referee_id"])]
        kickoff = f"{match['date']}T{match['kickoff_time_utc']}:00+00:00"
        group_name = f"Group {home['group_letter']}" if stage["stage_name"] == "Group Stage" else None
        round_name = stage["stage_name"]
        fixture_rows.append({
            "fixture_id": fixture_ids[match_id],
            "date_utc": kickoff,
            "timestamp": int(datetime.fromisoformat(kickoff).timestamp()),
            "timezone": "UTC",
            "referee": referee["name"],
            "venue_id": stable_id(PROVIDER, "venue", venue["venue_id"]),
            "venue_name": detailed["stadium_name"],
            "venue_city": detailed["city"],
            "status_short": "FT" if match["status"] == "Completed" else "NS",
            "status_long": "Match Finished" if match["status"] == "Completed" else match["status"],
            "league_id": COMPETITION_ID,
            "league_name": "FIFA Men's World Cup",
            "season": 2026,
            "round": round_name,
            "home_team_id": team_ids[int(match["home_team_id"])],
            "home_team_name": home["team_name"],
            "away_team_id": team_ids[int(match["away_team_id"])],
            "away_team_name": away["team_name"],
            "home_goals": as_int(match["home_score"]),
            "away_goals": as_int(match["away_score"]),
            "year": "2026",
            "month": match["date"][5:7],
            "ingested_run": run_id,
            "date": match["date"],
            "source_provider": PROVIDER,
            "referee_id": stable_id(PROVIDER, "referee", match["referee_id"]),
            "stage_id": stable_id(PROVIDER, "stage", match["stage_id"]),
            "round_id": stable_id(PROVIDER, "round", round_name),
            "home_goals_ft": as_int(match["home_score"]),
            "away_goals_ft": as_int(match["away_score"]),
            "provider": PROVIDER,
            "provider_league_id": COMPETITION_ID,
            "competition_key": COMPETITION_KEY,
            "competition_type": "international_cup",
            "season_label": SEASON_LABEL,
            "provider_season_id": SEASON_ID,
            "season_name": SEASON_NAME,
            "season_start_date": min(row["date"] for row in matches),
            "season_end_date": max(row["date"] for row in matches),
            "stage_name": round_name,
            "round_name": round_name,
            "group_name": group_name,
            "leg": 1,
            "ingested_at": ingested_at,
            "source_run_id": run_id,
        })
    write_csv("raw_fixtures.csv", fixture_columns, fixture_rows)

    group_rows: dict[str, dict[int, dict[str, int]]] = defaultdict(dict)
    for team_id, team in team_by_id.items():
        if team["group_letter"]:
            group_rows[team["group_letter"]][team_id] = {
                "points": 0, "played": 0, "won": 0, "draw": 0, "lost": 0,
                "goals_for": 0, "goals_against": 0,
            }
    for match in matches:
        if match_stage[int(match["match_id"])] ["stage_name"] != "Group Stage":
            continue
        home_id, away_id = int(match["home_team_id"]), int(match["away_team_id"])
        home_goals, away_goals = as_int(match["home_score"]) or 0, as_int(match["away_score"]) or 0
        group = team_by_id[home_id]["group_letter"]
        home_row, away_row = group_rows[group][home_id], group_rows[group][away_id]
        home_row["played"] += 1
        away_row["played"] += 1
        home_row["goals_for"] += home_goals
        home_row["goals_against"] += away_goals
        away_row["goals_for"] += away_goals
        away_row["goals_against"] += home_goals
        if home_goals > away_goals:
            home_row["won"] += 1; home_row["points"] += 3; away_row["lost"] += 1
        elif away_goals > home_goals:
            away_row["won"] += 1; away_row["points"] += 3; home_row["lost"] += 1
        else:
            home_row["draw"] += 1; away_row["draw"] += 1
            home_row["points"] += 1; away_row["points"] += 1

    knockout_team_ids = {
        int(match[field])
        for match in matches
        if match_stage[int(match["match_id"])] ["stage_name"] == "Round of 32"
        for field in ("home_team_id", "away_team_id")
    }
    standings_columns = [
        "provider", "league_id", "season_id", "stage_id", "round_id", "team_id",
        "position", "points", "games_played", "won", "draw", "lost", "goals_for",
        "goals_against", "goal_diff", "payload", "ingested_run", "updated_at",
        "provider_league_id", "competition_key", "season_label", "provider_season_id",
        "ingested_at", "source_run_id",
    ]
    standings = []
    for group, group_teams in sorted(group_rows.items()):
        ordered = sorted(
            group_teams.items(),
            key=lambda item: (
                -item[1]["points"],
                -(item[1]["goals_for"] - item[1]["goals_against"]),
                -item[1]["goals_for"],
                team_by_id[item[0]]["team_name"],
            ),
        )
        for position, (team_id, stats) in enumerate(ordered, 1):
            team = team_by_id[team_id]
            advanced = position <= 2 or team_id in knockout_team_ids
            standings.append({
                "provider": PROVIDER,
                "league_id": COMPETITION_ID,
                "season_id": SEASON_ID,
                "stage_id": stable_id(PROVIDER, "stage", "Group Stage"),
                "round_id": stable_id(PROVIDER, "round", "Group Stage"),
                "team_id": team_ids[team_id],
                "position": position,
                "points": stats["points"],
                "games_played": stats["played"],
                "won": stats["won"],
                "draw": stats["draw"],
                "lost": stats["lost"],
                "goals_for": stats["goals_for"],
                "goals_against": stats["goals_against"],
                "goal_diff": stats["goals_for"] - stats["goals_against"],
                "payload": json_text({
                    "advanced": advanced,
                    "group_key": group,
                    "stage_key": "group_stage_1",
                    "team_code": team["fifa_code"],
                    "team_name": team["team_name"],
                    "edition_key": EDITION_KEY,
                    "source_name": PROVIDER,
                    "source_row_id": f"standings-{group}-{team_id}",
                    "source_version": version,
                    "team_internal_id": team_internal_ids[team_id],
                    "group_internal_id": f"group__{EDITION_KEY}__{group}",
                    "stage_internal_id": f"stage__{EDITION_KEY}__group_stage_1",
                }),
                "ingested_run": run_id,
                "updated_at": ingested_at,
                "provider_league_id": COMPETITION_ID,
                "competition_key": COMPETITION_KEY,
                "season_label": SEASON_LABEL,
                "provider_season_id": SEASON_ID,
                "ingested_at": ingested_at,
                "source_run_id": run_id,
            })
    write_csv("raw_standings_snapshots.csv", standings_columns, standings)

    identity_team_columns = ["wc_team_id", "wc_display_slug", "sportmonks_team_id", "confidence", "status", "created_at", "updated_at"]
    write_csv("raw_wc_team_identity_map.csv", identity_team_columns, [
        {"wc_team_id": team_ids[team_id], "wc_display_slug": f"world-cup-{slug(team['team_name'])}", "confidence": "none", "status": "active", "created_at": ingested_at, "updated_at": ingested_at}
        for team_id, team in sorted(team_by_id.items())
    ])

    identity_player_columns = ["wc_player_id", "sportmonks_player_id", "match_confidence", "match_signals", "source_run_id", "created_at", "updated_at", "match_score", "match_method", "audited_by", "audit_notes", "blocked_reason"]
    write_csv("raw_wc_player_identity_map.csv", identity_player_columns, [
        {"wc_player_id": player_ids[player_id], "match_confidence": "none", "match_signals": json_text(["source_dataset_id"]), "source_run_id": run_id, "created_at": ingested_at, "updated_at": ingested_at}
        for player_id in sorted(players)
    ])

    squad_columns = [
        "wc_squad_pk", "edition_key", "provider", "competition_key", "season_label", "source_name",
        "source_version", "source_row_id", "source_team_id", "source_player_id", "team_internal_id",
        "player_internal_id", "team_id", "player_id", "team_name", "team_code", "player_name",
        "jersey_number", "position_name", "position_code", "payload", "source_run_id", "ingested_run",
        "created_at", "updated_at",
    ]
    squads = []
    for player_id, player in sorted(players.items()):
        team_id = int(player["team_id"])
        team = team_by_id[team_id]
        squads.append({
            "wc_squad_pk": stable_id(PROVIDER, "squad", player_id),
            "edition_key": EDITION_KEY, "provider": PROVIDER, "competition_key": COMPETITION_KEY,
            "season_label": SEASON_LABEL, "source_name": PROVIDER, "source_version": version,
            "source_row_id": str(player_id), "source_team_id": str(team_id), "source_player_id": str(player_id),
            "team_internal_id": team_internal_ids[team_id], "player_internal_id": player_internal_ids[player_id],
            "team_id": team_ids[team_id], "player_id": player_ids[player_id], "team_name": team["team_name"],
            "team_code": team["fifa_code"], "player_name": player["player_name"],
            "position_name": player["position"], "position_code": player["position"],
            "payload": json_text({"source_dataset": "squads_and_players.csv", "source_payload": player, "edition_key": EDITION_KEY, "source_name": PROVIDER, "source_version": version}),
            "source_run_id": run_id, "ingested_run": run_id, "created_at": ingested_at, "updated_at": ingested_at,
        })
    write_csv("raw_wc_squads.csv", squad_columns, squads)

    goal_columns = [
        "wc_goal_pk", "fixture_id", "internal_match_id", "edition_key", "provider", "competition_key", "season_label", "source_name", "source_version", "source_match_id", "source_goal_id", "source_team_id", "source_player_id", "source_player_team_id", "team_internal_id", "player_internal_id", "player_team_internal_id", "team_id", "player_id", "player_team_id", "team_name", "player_name", "player_team_name", "minute_regulation", "minute_stoppage", "match_period", "minute_label", "is_penalty", "is_own_goal", "payload", "source_run_id", "ingested_run", "created_at", "updated_at",
    ]
    booking_columns = [
        "wc_booking_pk", "fixture_id", "internal_match_id", "edition_key", "provider", "competition_key", "season_label", "source_name", "source_version", "source_match_id", "source_booking_id", "source_team_id", "source_player_id", "team_internal_id", "player_internal_id", "team_id", "player_id", "team_name", "player_name", "minute_regulation", "minute_stoppage", "match_period", "minute_label", "is_yellow_card", "is_red_card", "is_second_yellow_card", "is_sending_off", "payload", "source_run_id", "ingested_run", "created_at", "updated_at",
    ]
    goals, bookings = [], []
    for event in events:
        event_id, match_id = int(event["event_id"]), int(event["match_id"])
        team_id, player_id = int(event["team_id"]), int(event["player_id"])
        team, player = team_by_id[team_id], players[player_id]
        minute_regulation, minute_stoppage = parse_minute(event["minute"])
        minute = minute_regulation or 0
        common = {
            "fixture_id": fixture_ids[match_id], "internal_match_id": match_internal_ids[match_id], "edition_key": EDITION_KEY,
            "provider": PROVIDER, "competition_key": COMPETITION_KEY, "season_label": SEASON_LABEL, "source_name": PROVIDER,
            "source_version": version, "source_match_id": f"M-2026-{match_id}", "source_team_id": str(team_id), "source_player_id": str(player_id),
            "team_internal_id": team_internal_ids[team_id], "player_internal_id": player_internal_ids[player_id], "team_id": team_ids[team_id], "player_id": player_ids[player_id],
            "team_name": team["team_name"], "player_name": player["player_name"], "minute_regulation": minute, "minute_stoppage": minute_stoppage,
            "match_period": "first half" if minute <= 45 else "second half", "minute_label": f"{minute}'", "payload": json_text({"source_dataset": "match_events.csv", "source_payload": event, "edition_key": EDITION_KEY, "source_name": PROVIDER, "source_version": version}), "source_run_id": run_id, "ingested_run": run_id, "created_at": ingested_at, "updated_at": ingested_at,
        }
        if event["event_type"] == "Goal":
            goals.append({**common, "wc_goal_pk": stable_id(PROVIDER, "goal", event_id), "source_goal_id": f"G-2026-{event_id}", "source_player_team_id": str(team_id), "player_team_internal_id": team_internal_ids[team_id], "player_team_id": team_ids[team_id], "player_team_name": team["team_name"], "is_penalty": False, "is_own_goal": False})
        if event["event_type"] in {"Yellow Card", "Red Card"}:
            bookings.append({**common, "wc_booking_pk": stable_id(PROVIDER, "booking", event_id), "source_booking_id": f"B-2026-{event_id}", "is_yellow_card": event["event_type"] == "Yellow Card", "is_red_card": event["event_type"] == "Red Card", "is_second_yellow_card": False, "is_sending_off": event["event_type"] == "Red Card"})
    write_csv("raw_wc_goals.csv", goal_columns, goals)
    write_csv("raw_wc_bookings.csv", booking_columns, bookings)

    event_columns = [
        "wc_match_event_pk", "internal_match_id", "edition_key", "source_name", "source_version", "source_match_id", "source_event_id", "event_index", "team_internal_id", "player_internal_id", "event_type", "period", "minute", "second", "location_x", "location_y", "outcome_label", "play_pattern_label", "is_three_sixty_backed", "event_payload", "created_at", "updated_at", "fixture_id",
    ]
    write_csv("raw_wc_match_events.csv", event_columns, [
        {
            "wc_match_event_pk": stable_id(PROVIDER, "event", event["event_id"]), "internal_match_id": match_internal_ids[int(event["match_id"])], "edition_key": EDITION_KEY, "source_name": PROVIDER, "source_version": version, "source_match_id": f"M-2026-{event['match_id']}", "source_event_id": f"E-2026-{event['event_id']}", "event_index": int(event["event_id"]), "team_internal_id": team_internal_ids[int(event["team_id"])], "player_internal_id": player_internal_ids[int(event["player_id"])], "event_type": event["event_type"], "minute": parse_minute(event["minute"])[0], "second": 0, "is_three_sixty_backed": False, "event_payload": json_text({"source_dataset": "match_events.csv", "source_payload": event}), "created_at": ingested_at, "updated_at": ingested_at, "fixture_id": fixture_ids[int(event["match_id"])],
        }
        for event in events
    ])

    substitution_columns = [
        "wc_substitution_pk", "fixture_id", "internal_match_id", "edition_key", "provider", "competition_key", "season_label", "source_name", "source_version", "source_match_id", "source_substitution_id", "source_team_id", "source_player_id", "team_internal_id", "player_internal_id", "team_id", "player_id", "team_name", "player_name", "minute_regulation", "minute_stoppage", "match_period", "minute_label", "is_going_off", "is_coming_on", "substitution_role", "payload", "source_run_id", "ingested_run", "created_at", "updated_at",
    ]
    write_csv("raw_wc_substitutions.csv", substitution_columns, [])

    review_columns = ["review_pk", "entity_type", "edition_key", "source_name", "source_external_id", "candidate_internal_id", "confidence_level", "review_reason", "candidate_payload", "review_status", "reviewer_name", "reviewed_at", "resolved_internal_id", "created_at"]
    write_csv("control_wc_entity_match_review_queue.csv", review_columns, [])

    snapshot_columns = ["snapshot_pk", "source_name", "source_url", "source_version", "source_commit_or_release", "edition_scope", "accessed_at", "checksum_sha256", "local_path", "license_code", "attribution_note", "usage_decision", "is_active", "created_at"]
    write_csv("control_wc_source_snapshots.csv", snapshot_columns, [{
        "snapshot_pk": stable_id(PROVIDER, "snapshot", version), "source_name": PROVIDER, "source_url": "https://github.com/mominullptr/FIFA-World-Cup-2026-Dataset", "source_version": version, "source_commit_or_release": source_rows, "edition_scope": EDITION_KEY, "accessed_at": ingested_at, "checksum_sha256": version, "local_path": str(SOURCE_ZIP), "license_code": "CC0-1.0", "attribution_note": "FIFA World Cup 2026 Dataset by mominullptr; source README declares CC0-1.0.", "usage_decision": "now", "is_active": True, "created_at": ingested_at,
    }])

    counts = {
        "matches": len(matches), "teams": len(teams), "players": len(players), "events": len(events),
        "goals": len(goals), "bookings": len(bookings), "standings": len(standings), "squads": len(squads),
    }
    expected = {"matches": 104, "teams": 48, "players": 1248, "events": 834, "goals": 308, "bookings": 268, "standings": 48, "squads": 1248}
    if counts != expected:
        raise SystemExit(f"Unexpected transformed counts: {counts}; expected {expected}")
    print(json.dumps({"output_dir": str(OUTPUT_DIR), "source_version": version, "run_id": run_id, "counts": counts}, indent=2))


if __name__ == "__main__":
    main()
