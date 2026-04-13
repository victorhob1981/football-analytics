import { CHAMPION_ARTWORK_BY_EDITION } from "./champion-media.generated";

export type SeasonChampionArtwork = {
  src: string;
};

function buildEditionKey(competitionKey: string, seasonLabel: string): string {
  return `${competitionKey}::${seasonLabel}`;
}

export function resolveSeasonChampionArtwork(
  competitionKey: string,
  seasonLabel: string,
): SeasonChampionArtwork | null {
  return CHAMPION_ARTWORK_BY_EDITION[buildEditionKey(competitionKey, seasonLabel)] ?? null;
}
