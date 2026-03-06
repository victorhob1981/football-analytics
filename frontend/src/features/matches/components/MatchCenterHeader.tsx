import type { MatchListItem } from "@/features/matches/types";
import { formatDate } from "@/shared/utils/formatters";

type MatchCenterHeaderProps = {
  match: MatchListItem;
};

function resolveScore(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? String(value) : "-";
}

function formatWeather(match: MatchListItem): string {
  const parts: string[] = [];

  if (match.weatherDescription?.trim()) {
    parts.push(match.weatherDescription.trim());
  }

  if (typeof match.weatherTemperatureC === "number") {
    parts.push(`${match.weatherTemperatureC.toFixed(1)} C`);
  }

  if (typeof match.weatherWindKph === "number") {
    parts.push(`${match.weatherWindKph.toFixed(1)} km/h`);
  }

  return parts.length > 0 ? parts.join(" | ") : "Nao informado";
}

function toTeamInitials(teamName: string): string {
  const chunks = teamName
    .trim()
    .split(" ")
    .filter(Boolean);

  if (chunks.length === 0) {
    return "FC";
  }

  if (chunks.length === 1) {
    return chunks[0].slice(0, 2).toUpperCase();
  }

  return `${chunks[0][0] ?? ""}${chunks[1][0] ?? ""}`.toUpperCase();
}

function TeamBadge({ teamName, logoUrl }: { teamName: string; logoUrl?: string | null }) {
  const initials = toTeamInitials(teamName);
  const hasLogo = typeof logoUrl === "string" && logoUrl.trim().length > 0;

  return (
    <span
      aria-label={`Logo ${teamName}`}
      className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-slate-300 bg-slate-100 text-[11px] font-semibold text-slate-700"
      style={hasLogo ? { backgroundImage: `url(${logoUrl})`, backgroundSize: "cover", backgroundPosition: "center" } : undefined}
      title={teamName}
    >
      {hasLogo ? null : initials}
    </span>
  );
}

export function MatchCenterHeader({ match }: MatchCenterHeaderProps) {
  const homeTeamName = match.homeTeamName ?? "Mandante";
  const awayTeamName = match.awayTeamName ?? "Visitante";
  const score = `${resolveScore(match.homeScore)} x ${resolveScore(match.awayScore)}`;
  const attendanceText =
    typeof match.attendance === "number" ? new Intl.NumberFormat("pt-BR").format(match.attendance) : "Nao informado";
  const refereeText = match.refereeName?.trim().length ? match.refereeName : "Nao informado";
  const weatherText = formatWeather(match);

  return (
    <header className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <TeamBadge logoUrl={match.homeTeamLogoUrl} teamName={homeTeamName} />
          <h1 className="text-xl font-semibold text-slate-900">{homeTeamName}</h1>
        </div>
        <p className="text-lg font-medium text-slate-900">Placar: {score}</p>
        <div className="flex items-center gap-2">
          <h2 className="text-xl font-semibold text-slate-900">{awayTeamName}</h2>
          <TeamBadge logoUrl={match.awayTeamLogoUrl} teamName={awayTeamName} />
        </div>
      </div>
      <p className="text-sm text-slate-600">
        Match ID: {match.matchId}
        {match.competitionName ? ` | Competicao: ${match.competitionName}` : ""}
        {match.roundId ? ` | Rodada: ${match.roundId}` : ""}
      </p>
      <p className="text-sm text-slate-600">
        Data: {formatDate(match.kickoffAt)}
        {match.status ? ` | Status: ${match.status}` : ""}
        {match.venueName ? ` | Venue: ${match.venueName}` : " | Venue: Nao informado"}
      </p>
      <p className="text-sm text-slate-600">Arbitro: {refereeText} | Publico: {attendanceText}</p>
      <p className="text-sm text-slate-600">Weather: {weatherText}</p>
    </header>
  );
}
