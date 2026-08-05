from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((percent / 100) * (len(ordered) - 1))))
    return ordered[index]


def measure(client, path: str, *, warmups: int, samples: int) -> dict[str, object]:
    for _ in range(warmups):
        client.get(path)

    durations: list[float] = []
    statuses: list[int] = []
    for _ in range(samples):
        started = time.perf_counter()
        response = client.get(path)
        durations.append((time.perf_counter() - started) * 1000)
        statuses.append(response.status_code)

    return {
        "path": path,
        "samples": samples,
        "statuses": sorted(set(statuses)),
        "errors": sum(status >= 400 for status in statuses),
        "p50_ms": round(percentile(durations, 50), 2),
        "p95_ms": round(percentile(durations, 95), 2),
        "max_ms": round(max(durations), 2) if durations else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=r"D:\football-analytics-rebuild\benchmarks\search-slo.json",
    )
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--warmups", type=int, default=5)
    args = parser.parse_args()

    os.environ.setdefault(
        "FOOTBALL_PG_DSN",
        "postgresql://football:football@localhost:5433/football_dw_v2",
    )
    os.environ.setdefault("BFF_DATA_LAYER", "serving_v2")
    os.environ.setdefault("BFF_RATE_LIMIT_ENABLED", "false")

    from fastapi.testclient import TestClient

    from api.src.main import app

    probes = [
        "/api/v1/search?q=flamengo&limit=5",
        "/api/v1/search?q=messi&limit=5",
        "/api/v1/search?q=brasil&limit=5",
        "/api/v1/search?q=copa&limit=5",
        "/api/v1/teams?search=flamengo&pageSize=5",
    ]
    with TestClient(app) as client:
        team_response = client.get("/api/v1/teams?search=flamengo&pageSize=1")
        team_id = None
        if team_response.is_success:
            items = team_response.json().get("data", {}).get("items", [])
            if items:
                team_id = items[0].get("teamId")
        if team_id:
            probes.append(f"/api/v1/teams/{team_id}")

        results = [
            measure(client, path, warmups=args.warmups, samples=args.samples)
            for path in probes
        ]

    output = {
        "contract": {
            "environment": "local_candidate_v2",
            "rate_limit_enabled": False,
            "warmups": args.warmups,
            "samples": args.samples,
            "targets_ms": {"search_p95": 300, "profile_p95": 500},
        },
        "results": results,
        "slo": {
            "search_p95_ms": max(
                (float(item["p95_ms"]) for item in results if "/search?" in str(item["path"])),
                default=0.0,
            ),
            "profile_p95_ms": max(
                (float(item["p95_ms"]) for item in results if "/teams/" in str(item["path"])),
                default=0.0,
            ),
            "search_pass": all(
                float(item["p95_ms"]) < 300
                and int(item["errors"]) == 0
                for item in results
                if "/search?" in str(item["path"])
            ),
            "profile_pass": all(
                float(item["p95_ms"]) < 500
                and int(item["errors"]) == 0
                for item in results
                if "/teams/" in str(item["path"])
            ),
        },
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
