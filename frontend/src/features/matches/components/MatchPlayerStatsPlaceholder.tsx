import type { MatchPlayerStat } from "@/features/matches/types";
import { formatMetricValue } from "@/config/metrics.registry";

type MatchPlayerStatsPlaceholderProps = {
  playerStats: MatchPlayerStat[] | undefined;
};

function toMetricLine(playerStat: MatchPlayerStat): string {
  const goals = formatMetricValue("goals", playerStat.goals);
  const assists = formatMetricValue("assists", playerStat.assists);
  const minutes = formatMetricValue("minutes_played", playerStat.minutesPlayed);
  const rating = formatMetricValue("player_rating", playerStat.rating);

  return `G: ${goals} | A: ${assists} | Min: ${minutes} | Rating: ${rating}`;
}

export function MatchPlayerStatsPlaceholder({ playerStats }: MatchPlayerStatsPlaceholderProps) {
  const statsItems = playerStats ?? [];

  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Player Stats (placeholder)</h2>

      {statsItems.length === 0 ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          TODO: tabela completa de estatisticas individuais por jogador. Payload atual sem player stats.
        </p>
      ) : (
        <ul className="space-y-1 text-sm text-slate-700">
          {statsItems.slice(0, 8).map((playerStat, index) => (
            <li
              className="rounded border border-slate-100 bg-slate-50 px-2 py-1"
              key={`${playerStat.playerId ?? `player-${index}`}-${playerStat.teamId ?? "team-unknown"}`}
            >
              <p className="font-medium text-slate-900">{playerStat.playerName ?? "Jogador sem nome"}</p>
              <p className="text-slate-600">{toMetricLine(playerStat)}</p>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
