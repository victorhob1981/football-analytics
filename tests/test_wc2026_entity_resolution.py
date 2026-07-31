from tools.resolve_wc2026_entities import match_player, match_team, normalize_name


def test_normalize_name_ignores_accents_and_punctuation():
    assert normalize_name("Lionel Andrés Messi") == "lionel andres messi"
    assert normalize_name("Hee-chan Hwang") == "hee chan hwang"


def test_match_player_uses_date_of_birth_for_name_variant():
    candidates = [
        {
            "sportmonks_player_id": 184798,
            "wc_player_id": 7040928914210329456,
            "names": ["Lionel Messi", "Lionel Andrés Messi Cuccittini"],
            "date_of_births": ["1987-06-24"],
        }
    ]

    result = match_player("Lionel Andrés Messi", "1987-06-24", candidates)

    assert result["status"] == "resolved"
    assert result["candidate"]["sportmonks_player_id"] == 184798
    assert result["candidate"]["wc_player_id"] == 7040928914210329456
    assert result["method"] == "token_subset_dob"


def test_match_player_does_not_pick_between_equal_candidates():
    candidates = [
        {"sportmonks_player_id": 1, "names": ["José Silva"], "date_of_births": []},
        {"sportmonks_player_id": 2, "names": ["José Silva"], "date_of_births": []},
    ]

    result = match_player("Jose Silva", "2000-01-01", candidates)

    assert result["status"] == "ambiguous"
    assert len(result["candidates"]) == 2


def test_match_team_resolves_supported_alias_only_when_unique():
    candidates = [
        {
            "wc_team_id": 7030981485851247663,
            "sportmonks_team_id": 18571,
            "names": ["United States"],
        }
    ]

    result = match_team("USA", candidates)

    assert result["status"] == "resolved"
    assert result["candidate"]["wc_team_id"] == 7030981485851247663
