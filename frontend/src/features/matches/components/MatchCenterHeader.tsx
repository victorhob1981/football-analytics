import type { MatchListItem } from "@/features/matches/types";
import { formatDate } from "@/shared/utils/formatters";

type MatchCenterHeaderProps = {
  match: MatchListItem;
};

function resolveScore(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "-";
}

export function MatchCenterHeader({ match }: MatchCenterHeaderProps) {
  const homeTeamName = match.homeTeamName ?? "Mandante";
  const awayTeamName = match.awayTeamName ?? "Visitante";
  const score = `${resolveScore(match.homeScore)} x ${resolveScore(match.awayScore)}`;

  return (
    <header className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h1 className="text-xl font-semibold text-slate-900">
        {homeTeamName} vs {awayTeamName}
      </h1>
      <p className="text-sm text-slate-600">
        Match ID: {match.matchId}
        {match.competitionName ? ` | Competicao: ${match.competitionName}` : ""}
        {match.roundId ? ` | Rodada: ${match.roundId}` : ""}
      </p>
      <p className="text-sm text-slate-600">
        Data: {formatDate(match.kickoffAt)}
        {match.status ? ` | Status: ${match.status}` : ""}
        {match.venueName ? ` | Venue: ${match.venueName}` : ""}
      </p>
      <p className="text-lg font-medium text-slate-900">Placar: {score}</p>
    </header>
  );
}
