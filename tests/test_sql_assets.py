from pathlib import Path


REQUIRED_SQL_FILES = [
    "warehouse/queries/mart_team_monthly_upsert.sql",
    "warehouse/queries/mart_league_summary_upsert.sql",
    "warehouse/queries/mart_standings_evolution_upsert.sql",
    "warehouse/queries/mart_team_performance_upsert.sql",
]


def test_required_sql_files_exist_and_non_empty():
    missing = []
    empty = []

    for rel_path in REQUIRED_SQL_FILES:
        path = Path(rel_path)
        if not path.exists():
            missing.append(rel_path)
            continue

        content = path.read_text(encoding="utf-8").strip()
        if not content:
            empty.append(rel_path)

    assert not missing, f"Arquivos SQL ausentes: {missing}"
    assert not empty, f"Arquivos SQL vazios: {empty}"
