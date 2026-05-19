export type SeasonChampionArtwork = {
  src: string;
  teamName: string;
};

function buildEditionKey(competitionKey: string, seasonLabel: string): string {
  return `${competitionKey}::${seasonLabel}`;
}

const CHAMPION_ARTWORK_BY_EDITION: Record<string, SeasonChampionArtwork> = {
  "brasileirao_a::2021": {
    src: "/champions/brasileirao/2021_atletico_mg.jpg",
    teamName: "Atletico Mineiro",
  },
  "brasileirao_a::2022": {
    src: "/champions/brasileirao/2022_palmeiras.jpg",
    teamName: "Palmeiras",
  },
  "brasileirao_a::2023": {
    src: "/champions/brasileirao/2023_palmeiras.jpg",
    teamName: "Palmeiras",
  },
  "brasileirao_a::2024": {
    src: "/champions/brasileirao/2024_botafogo.jpg",
    teamName: "Botafogo",
  },
  "brasileirao_a::2025": {
    src: "/champions/brasileirao/2025_flamengo.jpg",
    teamName: "Flamengo",
  },
  "bundesliga::2020/2021": {
    src: "/champions/bundesliga/2020_21_bayern.jpg",
    teamName: "Bayern",
  },
  "bundesliga::2021/2022": {
    src: "/champions/bundesliga/2021_22_bayern.jpg",
    teamName: "Bayern",
  },
  "bundesliga::2022/2023": {
    src: "/champions/bundesliga/2022_23_bayern.jpg",
    teamName: "Bayern",
  },
  "bundesliga::2023/2024": {
    src: "/champions/bundesliga/2023_24_leverkusen.jpg",
    teamName: "Bayer Leverkusen",
  },
  "bundesliga::2024/2025": {
    src: "/champions/bundesliga/2024_25_bayern.jpg",
    teamName: "Bayern",
  },
  "champions_league::2020/2021": {
    src: "/champions/champions_league/2020_21_chelsea.jpg",
    teamName: "Chelsea",
  },
  "champions_league::2021/2022": {
    src: "/champions/champions_league/2021_22_real_madrid.jpg",
    teamName: "Real Madrid",
  },
  "champions_league::2022/2023": {
    src: "/champions/champions_league/2022_23_man_city.jpg",
    teamName: "Manchester City",
  },
  "champions_league::2023/2024": {
    src: "/champions/champions_league/2023_24_real_madrid.jpg",
    teamName: "Real Madrid",
  },
  "champions_league::2024/2025": {
    src: "/champions/champions_league/2024_25_psg.jpg",
    teamName: "PSG",
  },
  "copa_do_brasil::2021": {
    src: "/champions/copa_do_brasil/2021_atletico_mg.jpg",
    teamName: "Atletico Mineiro",
  },
  "copa_do_brasil::2022": {
    src: "/champions/copa_do_brasil/2022_flamengo.jpg",
    teamName: "Flamengo",
  },
  "copa_do_brasil::2023": {
    src: "/champions/copa_do_brasil/2023_sao_paulo.jpg",
    teamName: "Sao Paulo",
  },
  "copa_do_brasil::2024": {
    src: "/champions/copa_do_brasil/2024_flamengo.jpg",
    teamName: "Flamengo",
  },
  "copa_do_brasil::2025": {
    src: "/champions/copa_do_brasil/2025_corinthians.jpg",
    teamName: "Corinthians",
  },
  "la_liga::2020/2021": {
    src: "/champions/la_liga/2020_21_atletico_madrid.jpg",
    teamName: "Atletico de Madrid",
  },
  "la_liga::2021/2022": {
    src: "/champions/la_liga/2021_22_real_madrid.jpg",
    teamName: "Real Madrid",
  },
  "la_liga::2022/2023": {
    src: "/champions/la_liga/2022_23_barcelona.jpg",
    teamName: "Barcelona",
  },
  "la_liga::2023/2024": {
    src: "/champions/la_liga/2023_24_real_madrid.jpg",
    teamName: "Real Madrid",
  },
  "la_liga::2024/2025": {
    src: "/champions/la_liga/2024_25_barcelona.jpg",
    teamName: "Barcelona",
  },
  "ligue_1::2020/2021": {
    src: "/champions/ligue_1/2020_21_lille.jpg",
    teamName: "Lille",
  },
  "ligue_1::2021/2022": {
    src: "/champions/ligue_1/2021_22_psg.jpg",
    teamName: "PSG",
  },
  "ligue_1::2022/2023": {
    src: "/champions/ligue_1/2022_23_psg.jpg",
    teamName: "PSG",
  },
  "ligue_1::2023/2024": {
    src: "/champions/ligue_1/2023_24_psg.jpg",
    teamName: "PSG",
  },
  "ligue_1::2024/2025": {
    src: "/champions/ligue_1/2024_25_psg.jpg",
    teamName: "PSG",
  },
  "premier_league::2020/2021": {
    src: "/champions/premier_league/2020_21_man_city.jpg",
    teamName: "Manchester City",
  },
  "premier_league::2021/2022": {
    src: "/champions/premier_league/2021_22_man_city.jpg",
    teamName: "Manchester City",
  },
  "premier_league::2022/2023": {
    src: "/champions/premier_league/2022_23_man_city.jpg",
    teamName: "Manchester City",
  },
  "premier_league::2023/2024": {
    src: "/champions/premier_league/2023_24_man_city.jpg",
    teamName: "Manchester City",
  },
  "premier_league::2024/2025": {
    src: "/champions/premier_league/2024_25_liverpool.jpg",
    teamName: "Liverpool",
  },
  "serie_a_italy::2020/2021": {
    src: "/champions/serie_a/2020_21_inter.jpg",
    teamName: "Inter",
  },
  "serie_a_italy::2021/2022": {
    src: "/champions/serie_a/2021_22_milan.jpg",
    teamName: "Milan",
  },
  "serie_a_italy::2022/2023": {
    src: "/champions/serie_a/2022_23_napoli.jpg",
    teamName: "Napoli",
  },
  "serie_a_italy::2023/2024": {
    src: "/champions/serie_a/2023_24_inter.jpg",
    teamName: "Inter",
  },
  "serie_a_italy::2024/2025": {
    src: "/champions/serie_a/2024_25_napoli.jpg",
    teamName: "Napoli",
  },
};

export function resolveSeasonChampionArtwork(
  competitionKey: string,
  seasonLabel: string,
): SeasonChampionArtwork | null {
  return CHAMPION_ARTWORK_BY_EDITION[buildEditionKey(competitionKey, seasonLabel)] ?? null;
}
