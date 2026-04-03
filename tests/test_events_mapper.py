from __future__ import annotations

from pathlib import Path
import sys

import pytest


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

import common.mappers.events_mapper as events_mapper


def _payload_with_events(events: list[dict]) -> dict:
    return {
        "source_params": {"fixture": 1001},
        "errors": [],
        "response": events,
    }


@pytest.mark.parametrize(
    ("raw_value", "expected_time", "expected_anomalous"),
    [
        (None, None, False),
        (-1, None, True),
        (0, 0, False),
        (45, 45, False),
    ],
)
def test_normalize_time_elapsed(raw_value, expected_time, expected_anomalous):
    normalized, anomalous = events_mapper.normalize_time_elapsed(raw_value)
    assert normalized == expected_time
    assert anomalous is expected_anomalous


def test_build_match_events_dataframe_deduplicates_identical_events():
    payload = _payload_with_events(
        [
            {
                "time": {"elapsed": 10, "extra": 0},
                "team": {"id": 1, "name": "Team A"},
                "player": {"id": 9, "name": "Player A"},
                "assist": {"id": 11, "name": "Assist A"},
                "type": "Goal",
                "detail": "Normal Goal",
                "comments": None,
            },
            {
                "time": {"elapsed": 10, "extra": 0},
                "team": {"id": 1, "name": "Team A"},
                "player": {"id": 9, "name": "Player A"},
                "assist": {"id": 11, "name": "Assist A"},
                "type": "Goal",
                "detail": "Normal Goal",
                "comments": None,
            },
        ]
    )

    df = events_mapper.build_match_events_dataframe([payload])

    assert len(df) == 1


def test_build_match_events_dataframe_raises_on_conflicting_event_id_collision(monkeypatch):
    monkeypatch.setattr(events_mapper, "_event_id", lambda *args, **kwargs: "forced_collision")
    payload = _payload_with_events(
        [
            {
                "time": {"elapsed": 10, "extra": 0},
                "team": {"id": 1, "name": "Team A"},
                "player": {"id": 9, "name": "Player A"},
                "assist": {"id": None, "name": None},
                "type": "Goal",
                "detail": "Normal Goal",
                "comments": None,
            },
            {
                "time": {"elapsed": 20, "extra": 0},
                "team": {"id": 2, "name": "Team B"},
                "player": {"id": 10, "name": "Player B"},
                "assist": {"id": None, "name": None},
                "type": "Card",
                "detail": "Yellow Card",
                "comments": None,
            },
        ]
    )

    with pytest.raises(RuntimeError, match="Colisao de event_id"):
        events_mapper.build_match_events_dataframe([payload])


def test_event_id_fallback_is_stable_for_same_inputs():
    event_id_a = events_mapper._event_id(
        fixture_id=18489610,
        time_elapsed_raw=90,
        time_extra_raw=1,
        team_id=6468,
        team_name="Criciuma",
        event_type="Yellowcard",
        detail="Foul",
        player_id=12393283,
        player_name="Rodrigo Freitas",
        assist_id=None,
        assist_name=None,
        comments=None,
        provider_event_id=None,
    )
    event_id_b = events_mapper._event_id(
        fixture_id=18489610,
        time_elapsed_raw=90,
        time_extra_raw=1,
        team_id=6468,
        team_name="Criciuma",
        event_type="Yellowcard",
        detail="Foul",
        player_id=12393283,
        player_name="Rodrigo Freitas",
        assist_id=None,
        assist_name=None,
        comments=None,
        provider_event_id=None,
    )

    assert event_id_a == event_id_b


def test_event_id_uses_provider_event_id_when_present():
    event_id_a = events_mapper._event_id(
        fixture_id=18489610,
        time_elapsed_raw=90,
        time_extra_raw=1,
        team_id=6468,
        team_name="Criciuma",
        event_type="Yellowcard",
        detail="Foul",
        player_id=12393283,
        player_name="Rodrigo",
        assist_id=None,
        assist_name=None,
        comments=None,
        provider_event_id="evt-123",
    )
    event_id_b = events_mapper._event_id(
        fixture_id=18489610,
        time_elapsed_raw=90,
        time_extra_raw=1,
        team_id=6468,
        team_name="Criciuma EC",
        event_type="Yellowcard",
        detail="Foul",
        player_id=12393283,
        player_name="Rodrigo Freitas",
        assist_id=None,
        assist_name=None,
        comments="updated",
        provider_event_id="evt-123",
    )

    assert event_id_a == event_id_b


def test_build_match_events_dataframe_distinguishes_fallback_events_with_different_names():
    payload = _payload_with_events(
        [
            {
                "time": {"elapsed": 90, "extra": 1},
                "team": {"id": 6468, "name": "Criciuma"},
                "player": {"id": 12393283, "name": "Rodrigo"},
                "assist": {"id": None, "name": None},
                "type": "Yellowcard",
                "detail": "Foul",
                "comments": None,
            },
            {
                "time": {"elapsed": 90, "extra": 1},
                "team": {"id": 6468, "name": "Criciuma"},
                "player": {"id": 12393283, "name": "Rodrigo Freitas"},
                "assist": {"id": None, "name": None},
                "type": "Yellowcard",
                "detail": "Foul",
                "comments": None,
            },
        ]
    )

    df = events_mapper.build_match_events_dataframe([payload])

    assert len(df) == 2
    assert df["event_id"].nunique() == 2


def test_build_match_events_dataframe_normalizes_nbsp_without_false_collision():
    payload = _payload_with_events(
        [
            {
                "time": {"elapsed": 87, "extra": None},
                "team": {"id": 794, "name": "FSV Mainz 05"},
                "player": {"id": 31931, "name": "Stefan Bell"},
                "assist": {"id": None, "name": None},
                "type": "Yellowcard",
                "detail": "Argument",
                "comments": None,
            },
            {
                "time": {"elapsed": 87, "extra": None},
                "team": {"id": 794, "name": "FSV Mainz 05"},
                "player": {"id": 31931, "name": "Stefan Bell\u00a0"},
                "assist": {"id": None, "name": None},
                "type": "Yellowcard",
                "detail": "Argument",
                "comments": None,
            },
        ]
    )

    df = events_mapper.build_match_events_dataframe([payload])

    assert len(df) == 1
    assert df.iloc[0]["player_name"] == "Stefan Bell"


def test_build_match_events_dataframe_raises_on_conflicting_provider_event_collision():
    payload = _payload_with_events(
        [
            {
                "id": 77,
                "time": {"elapsed": 10, "extra": 0},
                "team": {"id": 1, "name": "Team A"},
                "player": {"id": 9, "name": "Player A"},
                "assist": {"id": None, "name": None},
                "type": "Goal",
                "detail": "Normal Goal",
                "comments": None,
            },
            {
                "id": 77,
                "time": {"elapsed": 20, "extra": 0},
                "team": {"id": 2, "name": "Team B"},
                "player": {"id": 10, "name": "Player B"},
                "assist": {"id": None, "name": None},
                "type": "Card",
                "detail": "Yellow Card",
                "comments": None,
            },
        ]
    )

    with pytest.raises(RuntimeError, match="Colisao de event_id"):
        events_mapper.build_match_events_dataframe([payload])
