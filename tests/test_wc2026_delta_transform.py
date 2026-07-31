import csv
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "artifacts" / "wc2026_delta"
SOURCE_DIR = ROOT / "FIFA-World-Cup-2026-Dataset-main" / "FIFA-World-Cup-2026-Dataset-main"
SOURCE_ZIP = ROOT / "FIFA-World-Cup-2026-Dataset-main.zip"


@pytest.mark.skipif(
    not SOURCE_DIR.is_dir() or not SOURCE_ZIP.is_file(),
    reason="local World Cup 2026 dataset is intentionally not versioned",
)
def test_wc2026_transform_writes_expected_batch():
    result = subprocess.run(
        [sys.executable, "tools/build_wc2026_delta.py"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert '"matches": 104' in result.stdout
    assert '"teams": 48' in result.stdout
    assert '"players": 1248' in result.stdout
    assert '"events": 834' in result.stdout

    with (OUTPUT / "raw_competition_seasons.csv").open(encoding="utf-8", newline="") as handle:
        season = next(csv.DictReader(handle))
    assert season["provider"] == "mominullptr_wc2026"
    assert season["season_label"] == "2026"
    assert season["competition_key"] == "fifa_world_cup_mens"
