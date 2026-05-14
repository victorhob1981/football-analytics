from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

JsonDict = dict[str, Any]


class ProviderAdapter(ABC):
    name: str

    @abstractmethod
    def get_fixtures(
        self,
        *,
        league_id: int,
        season: int,
        date_from: str,
        date_to: str,
    ) -> tuple[JsonDict, dict[str, str]]:
        """Fetch fixtures for a date window and return canonical bronze payload."""

    @abstractmethod
    def get_fixture_statistics(
        self,
        *,
        fixture_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        """Fetch fixture statistics and return canonical bronze payload."""

    @abstractmethod
    def get_fixture_events(
        self,
        *,
        fixture_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        """Fetch fixture events and return canonical bronze payload."""

    @abstractmethod
    def get_standings(
        self,
        *,
        league_id: int,
        season: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        """Fetch standings in canonical envelope format."""

    # Backward-compatible aliases
    def fetch_fixtures(
        self,
        *,
        league_id: int,
        season: int,
        date_from: str,
        date_to: str,
    ) -> tuple[JsonDict, dict[str, str]]:
        return self.get_fixtures(
            league_id=league_id,
            season=season,
            date_from=date_from,
            date_to=date_to,
        )

    def fetch_fixture_statistics(
        self,
        *,
        fixture_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        return self.get_fixture_statistics(fixture_id=fixture_id)

    def fetch_fixture_events(
        self,
        *,
        fixture_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        return self.get_fixture_events(fixture_id=fixture_id)

    # Extended contract (P2+). Providers may opt-in progressively.
    def get_competition_structure(
        self,
        *,
        league_id: int,
        season: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_competition_structure.")

    def get_fixture_lineups(
        self,
        *,
        fixture_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_fixture_lineups.")

    def get_fixture_player_statistics(
        self,
        *,
        fixture_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_fixture_player_statistics.")

    def get_player_season_statistics(
        self,
        *,
        player_id: int,
        season: int | None = None,
        league_id: int | None = None,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_player_season_statistics.")

    def get_player_transfers(
        self,
        *,
        player_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_player_transfers.")

    def get_team_sidelined(
        self,
        *,
        team_id: int,
        season: int | None = None,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_team_sidelined.")

    def get_team_coaches(
        self,
        *,
        team_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_team_coaches.")

    def get_head_to_head(
        self,
        *,
        team_id: int,
        opponent_id: int,
    ) -> tuple[JsonDict, dict[str, str]]:
        raise NotImplementedError(f"Provider '{self.name}' nao implementa get_head_to_head.")
