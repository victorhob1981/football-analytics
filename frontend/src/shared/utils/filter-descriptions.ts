export function describeVenueLabel(venue: string | null | undefined): string {
  if (venue === "home") {
    return "Casa";
  }

  if (venue === "away") {
    return "Fora";
  }

  return "Todos os mandos";
}

export function describeTimeWindowLabel(params: {
  roundId: string | null;
  lastN: number | null;
  dateRangeStart: string | null;
  dateRangeEnd: string | null;
}): string {
  if (params.lastN !== null) {
    return `Últimas ${params.lastN} partidas`;
  }

  if (params.dateRangeStart !== null || params.dateRangeEnd !== null) {
    const startLabel = params.dateRangeStart ?? "...";
    const endLabel = params.dateRangeEnd ?? "...";

    return `${startLabel} até ${endLabel}`;
  }

  if (params.roundId !== null) {
    return `Rodada ${params.roundId}`;
  }

  return "Temporada inteira";
}
