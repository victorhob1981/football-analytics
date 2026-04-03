from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
import sys


DAGS_DIR = Path("infra/airflow/dags").resolve()
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))


from common.raw_writer import write_raw_payload


class _FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[dict[str, object]] = []

    def upload_fileobj(self, fileobj: BytesIO, bucket: str, key: str) -> None:
        self.uploads.append(
            {
                "bucket": bucket,
                "key": key,
                "body": fileobj.read().decode("utf-8"),
            }
        )


def test_write_raw_payload_preserves_provider_endpoint_for_combined_route():
    s3_client = _FakeS3Client()

    write_raw_payload(
        s3_client=s3_client,
        bucket="football-bronze",
        key="events/league=71/season=2024/fixture_id=10/run=1/data.json",
        payload={
            "provider_meta": {
                "endpoint": "/fixtures/multi/10,20",
                "timezone": "UTC",
            },
            "response": [],
            "results": 0,
        },
        provider="sportmonks",
        endpoint="fixtures/events",
        source_params={"fixture": 10, "league_id": 71, "season": 2024},
        entity_type="match_events",
    )

    persisted = json.loads(s3_client.uploads[0]["body"])

    assert persisted["source_endpoint"] == "/fixtures/multi/10,20"
    assert persisted["artifact_endpoint"] == "fixtures/events"
    assert persisted["entity_type"] == "match_events"
    assert persisted["provider_meta"]["endpoint"] == "/fixtures/multi/10,20"


def test_write_raw_payload_keeps_legacy_endpoint_when_provider_endpoint_missing():
    s3_client = _FakeS3Client()

    write_raw_payload(
        s3_client=s3_client,
        bucket="football-bronze",
        key="statistics/league=71/season=2024/fixture_id=10/run=1/data.json",
        payload={
            "provider_meta": {},
            "response": [],
            "results": 0,
        },
        provider="sportmonks",
        endpoint="fixtures/statistics",
        source_params={"fixture": 10},
        entity_type="statistics",
    )

    persisted = json.loads(s3_client.uploads[0]["body"])

    assert persisted["source_endpoint"] == "fixtures/statistics"
    assert persisted["artifact_endpoint"] == "fixtures/statistics"
    assert persisted["entity_type"] == "statistics"
