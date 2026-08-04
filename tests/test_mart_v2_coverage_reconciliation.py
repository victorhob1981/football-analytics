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
    assert "unexplained published match delta" in validation


def test_legacy_summary_is_not_marked_explained_only_from_zero_pending_rows() -> None:
    validation = (ROOT / "db/rebuild_v2/009_validation.sql").read_text(encoding="utf-8")

    obsolete_rule = (
        "CASE WHEN (SELECT count(*) FROM mart_v2.match_source "
        "WHERE reconciliation_state = 'pending') > 0 THEN 'pending' ELSE 'explained' END"
    )
    assert obsolete_rule not in validation
