from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_official_pipeline_materializes_serving_and_runs_post_dbt_validation() -> None:
    source = (ROOT / "tools" / "run_mart_v2_pipeline.ps1").read_text(encoding="utf-8")

    assert "-SkipServing" in source
    assert '"run"' in source
    assert '"test"' in source
    assert "009_validation.sql" in source
    assert "validate_mart_v2.ps1" in source
    assert "pg_restore" not in source.lower()
    assert "pg_dump" not in source.lower()


def test_dbt_serving_contract_declares_the_public_projections() -> None:
    model_dir = ROOT / "platform" / "dbt_v2" / "models" / "serving"
    models = {path.stem for path in model_dir.glob("*.sql")}

    assert models == {
        "competition_catalog",
        "competition_presentation",
        "edition_catalog",
        "match_catalog",
        "player_profile",
        "publication_metrics",
        "search_document",
        "team_profile",
    }
