from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_coverage_reconciliation_requires_an_exact_reasoned_delta() -> None:
    contract = (ROOT / "db/rebuild_v2/002_control_contract.sql").read_text(encoding="utf-8")
    validation = (ROOT / "db/rebuild_v2/009_validation.sql").read_text(encoding="utf-8")

    assert "control.coverage_delta_reason" in contract
    assert "delta_rows" in contract
    assert "reason_code" in contract
    assert "approved_source_scope_change" in validation
    assert "cross_source_canonical_dedup" in validation
    assert "same_source_canonical_dedup" in validation
    assert "approved_source_nonpublished" in validation
    assert "duplicate_semantic_candidate" in validation
    assert "published_rows + quarantined_rows" in validation
    assert "unexplained published match delta" in validation


def test_legacy_summary_is_not_marked_explained_only_from_zero_pending_rows() -> None:
    validation = (ROOT / "db/rebuild_v2/009_validation.sql").read_text(encoding="utf-8")

    obsolete_rule = (
        "CASE WHEN (SELECT count(*) FROM mart_v2.match_source "
        "WHERE reconciliation_state = 'pending') > 0 THEN 'pending' ELSE 'explained' END"
    )
    assert obsolete_rule not in validation


def test_fingerprints_cover_all_logical_tables_and_partition_rollups() -> None:
    validation = (ROOT / "db/rebuild_v2/009_validation.sql").read_text(encoding="utf-8")

    assert "pg_inherits" in validation
    assert "to_jsonb(t)" in validation
    assert "logical_partition_rollup" in validation
    assert "source_run_id" in validation


def test_validation_runbook_excludes_restore() -> None:
    script = (ROOT / "tools/validate_mart_v2.ps1").read_text(encoding="utf-8")

    for forbidden in ("pg_dump", "pg_restore", "createdb", "restore_validation", "restore_database"):
        assert forbidden not in script


def test_rebuild_runbook_does_not_reuse_a_hardcoded_run_key() -> None:
    script = (ROOT / "tools/rebuild_mart_v2.ps1").read_text(encoding="utf-8")

    assert '[string]$RunKey = ""' in script
    assert "if (-not $RunKey)" in script
    assert "Get-Date -Format" in script


def test_pipeline_runs_sql_rebuild_dbt_contracts_and_read_only_validation() -> None:
    script = (ROOT / "tools/run_mart_v2_pipeline.ps1").read_text(encoding="utf-8")

    assert "rebuild_mart_v2.ps1" in script
    assert '"dbt", "test"' in script
    assert "validate_mart_v2.ps1" in script
    assert "D:\\" in script
