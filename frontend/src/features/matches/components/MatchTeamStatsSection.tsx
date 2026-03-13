import type { MatchTeamStat } from "@/features/matches/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";

type MatchTeamStatsSectionProps = {
  teamStats: MatchTeamStat[] | undefined;
  homeTeamName?: string | null;
  awayTeamName?: string | null;
};

function formatMetricValue(metricKey: string, value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }

  if (metricKey === "ball_possession") {
    return `${Math.round(value)}%`;
  }

  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

export function MatchTeamStatsSection({ teamStats, homeTeamName, awayTeamName }: MatchTeamStatsSectionProps) {
  const rows = teamStats ?? [];

  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Team Stats</h2>

      {rows.length === 0 ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          Estatisticas comparativas de times indisponiveis para esta partida.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left text-xs uppercase text-slate-500">
                <th className="px-2 py-2">Metrica</th>
                <th className="px-2 py-2 text-right">{homeTeamName ?? "Mandante"}</th>
                <th className="px-2 py-2 text-right">{awayTeamName ?? "Visitante"}</th>
                <th className="px-2 py-2">Coverage</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {rows.map((row) => (
                <tr key={row.metricKey}>
                  <td className="px-2 py-2 font-medium text-slate-800">{row.metricLabel}</td>
                  <td className="px-2 py-2 text-right text-slate-700">{formatMetricValue(row.metricKey, row.homeValue)}</td>
                  <td className="px-2 py-2 text-right text-slate-700">{formatMetricValue(row.metricKey, row.awayValue)}</td>
                  <td className="px-2 py-2">
                    {row.coverage ? <CoverageBadge coverage={row.coverage} /> : <span className="text-xs text-slate-500">-</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
