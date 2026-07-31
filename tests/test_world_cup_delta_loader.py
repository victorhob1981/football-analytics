import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOADER = ROOT / "db" / "bootstrap" / "load_world_cup_delta.sql"


def test_world_cup_delta_loader_replaces_only_rows_present_in_the_batch():
    sql = LOADER.read_text(encoding="utf-8").lower()

    incoming_tables = (
        "competition_seasons",
        "fixtures",
        "standings_snapshots",
        "wc_player_identity_map",
        "wc_team_identity_map",
        "wc_squads",
        "wc_goals",
        "wc_match_events",
        "wc_bookings",
        "wc_substitutions",
        "wc_entity_match_review_queue",
        "wc_source_snapshots",
    )

    for table in incoming_tables:
        assert f"create temp table incoming_{table}" in sql
        assert f"using incoming_{table}" in sql
        assert f"select * from incoming_{table}" in sql

    assert re.search(r"delete from (raw|control)\.[a-z_]+;", sql) is None
    assert "delete from raw.competition_seasons where competition_key" not in sql
