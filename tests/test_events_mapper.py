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
