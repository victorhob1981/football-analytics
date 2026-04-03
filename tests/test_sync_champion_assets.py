from tools.sync_champion_assets import (
    ChampionTarget,
    IMAGE_VARIANT,
    HONOR_CODE,
    SearchCandidate,
    build_asset_id,
    build_search_queries,
    build_season_terms,
    normalize_season_key,
    qualifies_for_auto_download,
    score_candidate,
)


def make_target() -> ChampionTarget:
    return ChampionTarget(
        competition_key="brasileirao_a",
        provider_league_id=648,
        competition_name="Serie A",
        season_label="2025",
        season_key="2025",
        season_display="2025",
        season_terms=build_season_terms("2025"),
        team_id=1024,
        team_name="Flamengo",
        team_slug="flamengo",
        champion_source="standings",
        champion_stage_name="Regular Season",
        honor_code=HONOR_CODE,
        image_variant=IMAGE_VARIANT,
        sequence=1,
        asset_id=build_asset_id("brasileirao_a", "2025", "flamengo"),
        filename_stem=build_asset_id("brasileirao_a", "2025", "flamengo"),
    )


def test_normalize_season_key_supports_cross_year_labels():
    assert normalize_season_key("2024_25") == "2024-2025"
    assert normalize_season_key("2024/2025") == "2024-2025"
    assert normalize_season_key("2025") == "2025"


def test_build_asset_id_follows_locked_contract():
    assert build_asset_id("brasileirao_a", "2025", "flamengo") == (
        "brasileirao_a__2025__champion__flamengo__trophy-lift__v01"
    )


def test_build_search_queries_keep_team_competition_and_season_context():
    target = make_target()

    queries = build_search_queries(target)

    assert any("Flamengo" in query for query in queries)
    assert any("Brasileirão" in query or "Campeonato Brasileiro" in query for query in queries)
    assert any("2025" in query for query in queries)


def test_score_candidate_prefers_strong_contextual_match():
    target = make_target()
    strong_candidate = SearchCandidate(
        provider="wikimedia_commons",
        provider_id="file-1",
        title="Flamengo Campeão Brasileirão 2025",
        creator="Photographer",
        license="CC BY-SA 4.0",
        license_url="https://creativecommons.org/licenses/by-sa/4.0/",
        source_url="https://example.com/flamengo.jpg",
        source_page_url="https://example.com/flamengo",
        content_type="image/jpeg",
        width=1600,
        height=900,
        search_query="Flamengo Brasileirão 2025 champion trophy",
        searchable_text="Flamengo Campeão Brasileirão 2025 trophy celebration final",
    )
    weak_candidate = SearchCandidate(
        provider="openverse",
        provider_id="file-2",
        title="Manchester City FC's trophy in India",
        creator="Example",
        license="by-nc-nd",
        license_url="https://creativecommons.org/licenses/by-nc-nd/2.0/",
        source_url="https://example.com/city.jpg",
        source_page_url="https://example.com/city",
        content_type="image/jpeg",
        width=1024,
        height=683,
        search_query="Flamengo Brasileirão 2025 champion trophy",
        searchable_text="Manchester City FC trophy in India football sports",
    )

    strong_score = score_candidate(target, strong_candidate)
    weak_score = score_candidate(target, weak_candidate)

    assert strong_score.score >= 80
    assert weak_score.score < 60
    assert strong_score.score > weak_score.score


def test_auto_download_requires_competition_context():
    target = make_target()
    city_trophy_candidate = SearchCandidate(
        provider="openverse",
        provider_id="file-4",
        title="Manchester City FC's trophy in India",
        creator="Example",
        license="by-nc-nd",
        license_url="https://creativecommons.org/licenses/by-nc-nd/2.0/",
        source_url="https://example.com/city-trophy.jpg",
        source_page_url="https://example.com/city-trophy",
        content_type="image/jpeg",
        width=1024,
        height=683,
        search_query="Flamengo Brasileirão 2025 champion trophy",
        searchable_text="Manchester City FC's trophy in India football sports",
    )

    scored = score_candidate(target, city_trophy_candidate)

    assert scored.score >= 20
    assert not qualifies_for_auto_download(scored, min_confidence_score=60)


def test_score_candidate_penalizes_logo_like_result():
    target = make_target()
    logo_candidate = SearchCandidate(
        provider="openverse",
        provider_id="file-3",
        title="Flamengo logo poster",
        creator="Example",
        license="cc0",
        license_url="https://creativecommons.org/publicdomain/zero/1.0/",
        source_url="https://example.com/logo.png",
        source_page_url="https://example.com/logo",
        content_type="image/png",
        width=1200,
        height=1200,
        search_query="Flamengo Brasileirão 2025 champion trophy",
        searchable_text="Flamengo logo poster wallpaper badge",
    )

    scored = score_candidate(target, logo_candidate)

    assert scored.score < 40
    assert "negative_keyword" in scored.reasons
