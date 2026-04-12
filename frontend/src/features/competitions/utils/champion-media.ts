export type SeasonChampionArtwork = {
  src: string;
  teamName: string;
};

function buildEditionKey(competitionKey: string, seasonLabel: string): string {
  return `${competitionKey}::${seasonLabel}`;
}

const CHAMPION_ARTWORK_BY_EDITION: Record<string, SeasonChampionArtwork> = {};

export function resolveSeasonChampionArtwork(
  competitionKey: string,
  seasonLabel: string,
): SeasonChampionArtwork | null {
  return CHAMPION_ARTWORK_BY_EDITION[buildEditionKey(competitionKey, seasonLabel)] ?? null;
}
