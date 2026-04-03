from __future__ import annotations

import unittest

from api.src.core.filters import StageFormatFilter, VenueFilter, append_fact_match_filters, validate_and_build_global_filters


class FilterAliasTests(unittest.TestCase):
    def test_global_filters_expand_canonical_competition_to_supported_source_ids(self) -> None:
        filters = validate_and_build_global_filters(
            competition_id="71",
            season_id="2025",
            round_id=None,
            venue=VenueFilter.all,
            last_n=None,
            date_start=None,
            date_end=None,
            date_range_start=None,
            date_range_end=None,
        )

        self.assertEqual(filters.competition_id, 71)
        self.assertEqual(filters.competition_ids, (71, 648))

    def test_global_filters_keep_provider_competition_compatible_with_same_canonical_scope(self) -> None:
        filters = validate_and_build_global_filters(
            competition_id="648",
            season_id="2025",
            round_id=None,
            venue=VenueFilter.all,
            last_n=None,
            date_start=None,
            date_end=None,
            date_range_start=None,
            date_range_end=None,
        )

        self.assertEqual(filters.competition_id, 648)
        self.assertEqual(filters.competition_ids, (71, 648))

    def test_append_fact_match_filters_uses_competition_aliases_in_single_clause(self) -> None:
        filters = validate_and_build_global_filters(
            competition_id="71",
            season_id="2025",
            round_id=None,
            venue=VenueFilter.all,
            last_n=None,
            date_start=None,
            date_end=None,
            date_range_start=None,
            date_range_end=None,
        )
        clauses: list[str] = ["1=1"]
        params: list[object] = []

        append_fact_match_filters(clauses, params, alias="fm", filters=filters)

        self.assertIn("fm.league_id = any(%s)", clauses)
        self.assertEqual(params[0], [71, 648])
        self.assertEqual(params[1], 2025)

    def test_global_filters_accept_stage_scoping_fields(self) -> None:
        filters = validate_and_build_global_filters(
            competition_id="390",
            season_id="2024",
            round_id=None,
            stage_id="77468966",
            stage_format="group_table",
            venue=VenueFilter.all,
            last_n=None,
            date_start=None,
            date_end=None,
            date_range_start=None,
            date_range_end=None,
        )

        self.assertEqual(filters.stage_id, 77468966)
        self.assertEqual(filters.stage_format, StageFormatFilter.group_table)

    def test_append_fact_match_filters_adds_stage_id_and_stage_format_scope(self) -> None:
        filters = validate_and_build_global_filters(
            competition_id="390",
            season_id="2024",
            round_id=None,
            stage_id="77468966",
            stage_format="group_table",
            venue=VenueFilter.all,
            last_n=None,
            date_start=None,
            date_end=None,
            date_range_start=None,
            date_range_end=None,
        )
        clauses: list[str] = ["1=1"]
        params: list[object] = []

        append_fact_match_filters(clauses, params, alias="fm", filters=filters)

        self.assertIn("fm.stage_id = %s", clauses)
        self.assertTrue(any("stage_filter_scope.stage_format = %s" in clause for clause in clauses))
        self.assertIn(77468966, params)
        self.assertIn("group_table", params)


if __name__ == "__main__":
    unittest.main()
