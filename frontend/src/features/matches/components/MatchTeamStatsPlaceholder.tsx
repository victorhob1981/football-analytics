import { formatNumber, formatPercentage } from "@/shared/utils/formatters";

import type { MatchTeamStat } from "@/features/matches/types";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import {
  ProfileCoveragePill,
  ProfilePanel,
  ProfileTag,
} from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import type { CoverageState } from "@/shared/types/coverage.types";

type MatchTeamStatsPlaceholderProps = {
  coverage?: CoverageState;
  teamStats: MatchTeamStat[] | undefined;
  homeTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamId?: string | null;
  awayTeamName?: string | null;
};

type MetricDefinition = {
  key: keyof MatchTeamStat;
  label: string;
  category: string;
  format: (value: number) => string;
  scaleMax?: number;
};

const TEAM_STATS_METRICS: MetricDefinition[] = [
  {
    key: "totalShots",
    label: "Finalizações",
    category: "Ataque",
    format: (value) => formatNumber(value),
  },
  {
    key: "shotsOnGoal",
    label: "No alvo",
    category: "Ataque",
    format: (value) => formatNumber(value),
  },
  {
    key: "corners",
    label: "Escanteios",
    category: "Ataque",
    format: (value) => formatNumber(value),
  },
  {
    key: "possessionPct",
    label: "Posse",
    category: "Controle e passe",
    format: (value) => formatPercentage(value, 0),
    scaleMax: 100,
  },
  {
    key: "totalPasses",
    label: "Passes totais",
    category: "Controle e passe",
    format: (value) => formatNumber(value),
  },
  {
    key: "passAccuracyPct",
    label: "Precisão",
    category: "Controle e passe",
    format: (value) => formatPercentage(value, 1),
    scaleMax: 100,
  },
  {
    key: "fouls",
    label: "Faltas",
    category: "Disciplina e defesa",
    format: (value) => formatNumber(value),
  },
  {
    key: "yellowCards",
    label: "Amarelos",
    category: "Disciplina e defesa",
    format: (value) => formatNumber(value),
  },
  {
    key: "redCards",
    label: "Vermelhos",
    category: "Disciplina e defesa",
    format: (value) => formatNumber(value),
  },
  {
    key: "goalkeeperSaves",
    label: "Defesas",
    category: "Disciplina e defesa",
    format: (value) => formatNumber(value),
  },
];

function resolveMetricValue(
  teamStats: MatchTeamStat | null,
  metric: MetricDefinition,
): number | null {
  const rawValue = teamStats?.[metric.key];
  return typeof rawValue === "number" && Number.isFinite(rawValue) ? rawValue : null;
}

function resolveBarWidth(
  value: number | null,
  counterpart: number | null,
  scaleMax?: number,
): number {
  if (value === null) {
    return 0;
  }

  const base = typeof scaleMax === "number" ? scaleMax : Math.max(value, counterpart ?? 0, 0);
  if (base <= 0) {
    return 0;
  }

  return Math.max(8, Math.min((value / base) * 100, 100));
}

function resolveFormattedMetric(
  teamStats: MatchTeamStat | null,
  metric: MetricDefinition,
): string {
  const value = resolveMetricValue(teamStats, metric);
  return value === null ? "-" : metric.format(value);
}

function getTeamMonogram(teamName: string): string {
  const initials = teamName
    .split(/\s+/)
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => chunk[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 3);

  return initials.length > 0 ? initials : "CLB";
}

function resolveTeamStatsRows(
  teamStats: MatchTeamStat[],
  homeTeamId?: string | null,
  awayTeamId?: string | null,
): {
  homeTeamStats: MatchTeamStat | null;
  awayTeamStats: MatchTeamStat | null;
  payloadCount: number;
} {
  const homeTeamStats =
    teamStats.find((item) => item.teamId && homeTeamId && item.teamId === homeTeamId) ?? null;
  const awayTeamStats =
    teamStats.find((item) => item.teamId && awayTeamId && item.teamId === awayTeamId) ?? null;

  return {
    homeTeamStats,
    awayTeamStats,
    payloadCount: teamStats.length,
  };
}

function MetricRow({
  metric,
  homeTeamStats,
  awayTeamStats,
}: {
  metric: MetricDefinition;
  homeTeamStats: MatchTeamStat | null;
  awayTeamStats: MatchTeamStat | null;
}) {
  const homeValue = resolveMetricValue(homeTeamStats, metric);
  const awayValue = resolveMetricValue(awayTeamStats, metric);

  if (homeValue === null && awayValue === null) {
    return null;
  }

  const homeWidth = resolveBarWidth(homeValue, awayValue, metric.scaleMax);
  const awayWidth = resolveBarWidth(awayValue, homeValue, metric.scaleMax);

  return (
    <div className="grid gap-3 md:grid-cols-[96px_minmax(0,1fr)_96px] md:items-center">
      <p className="text-sm font-semibold text-[#111c2d] md:text-right">
        {resolveFormattedMetric(homeTeamStats, metric)}
      </p>
      <div className="space-y-2">
        <div className="flex items-center justify-center">
          <span className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            {metric.label}
          </span>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div className="flex h-2 items-center rounded-full bg-[rgba(216,227,251,0.72)]">
            <div
              className="ml-auto h-2 rounded-full bg-[#00513b]"
              style={{ width: `${homeWidth}%` }}
            />
          </div>
          <div className="flex h-2 items-center rounded-full bg-[rgba(216,227,251,0.72)]">
            <div
              className="h-2 rounded-full bg-[#4b647b]"
              style={{ width: `${awayWidth}%` }}
            />
          </div>
        </div>
      </div>
      <p className="text-sm font-semibold text-[#111c2d] md:text-left">
        {resolveFormattedMetric(awayTeamStats, metric)}
      </p>
    </div>
  );
}

function MetricGroup({
  category,
  metrics,
  homeTeamStats,
  awayTeamStats,
}: {
  category: string;
  metrics: MetricDefinition[];
  homeTeamStats: MatchTeamStat | null;
  awayTeamStats: MatchTeamStat | null;
}) {
  const visibleMetrics = metrics.filter((metric) => {
    const homeValue = resolveMetricValue(homeTeamStats, metric);
    const awayValue = resolveMetricValue(awayTeamStats, metric);
    return homeValue !== null || awayValue !== null;
  });

  if (visibleMetrics.length === 0) {
    return null;
  }

  return (
    <section className="space-y-4 rounded-[1.35rem] bg-[rgba(240,243,255,0.82)] p-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            {category}
          </p>
          <h3 className="mt-2 font-[family:var(--font-profile-headline)] text-xl font-extrabold text-[#111c2d]">
            Comparativo lado a lado
          </h3>
        </div>
        <ProfileTag>{visibleMetrics.length} métricas</ProfileTag>
      </div>
      <div className="space-y-4">
        {visibleMetrics.map((metric) => (
          <MetricRow
            awayTeamStats={awayTeamStats}
            homeTeamStats={homeTeamStats}
            key={metric.key}
            metric={metric}
          />
        ))}
      </div>
    </section>
  );
}

export function MatchTeamStatsPlaceholder({
  coverage = { status: "unknown", label: "Estatísticas dos times" },
  teamStats,
  homeTeamId,
  homeTeamName,
  awayTeamId,
  awayTeamName,
}: MatchTeamStatsPlaceholderProps) {
  const teamStatsItems = teamStats ?? [];
  const { homeTeamStats, awayTeamStats, payloadCount } = resolveTeamStatsRows(
    teamStatsItems,
    homeTeamId,
    awayTeamId,
  );
  const metricsByCategory = TEAM_STATS_METRICS.reduce<Record<string, MetricDefinition[]>>(
    (groups, metric) => {
      groups[metric.category] = groups[metric.category] ?? [];
      groups[metric.category].push(metric);
      return groups;
    },
    {},
  );
  const visibleCategories = Object.entries(metricsByCategory).filter(([, metrics]) =>
    metrics.some((metric) => {
      const homeValue = resolveMetricValue(homeTeamStats, metric);
      const awayValue = resolveMetricValue(awayTeamStats, metric);
      return homeValue !== null || awayValue !== null;
    }),
  );

  return (
    <ProfilePanel className="space-y-5">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Times
          </p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
            Comparativo dos times
          </h2>
          <p className="mt-2 text-sm leading-6 text-[#57657a]">
            Ataque, controle e disciplina lado a lado, com os números que já estão disponíveis para o jogo.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <ProfileCoveragePill coverage={coverage} />
          <ProfileTag>{payloadCount} lados com dados</ProfileTag>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] md:items-center">
        <div className="rounded-[1.1rem] bg-[rgba(240,243,255,0.82)] px-4 py-3 text-center md:text-left">
          <div className="flex flex-col items-center gap-3 md:flex-row">
            <ProfileMedia
              alt={homeTeamStats?.teamName?.trim() || homeTeamName?.trim() || "Mandante"}
              assetId={homeTeamStats?.teamId ?? homeTeamId}
              category="clubs"
              className="h-12 w-12 border-[rgba(191,201,195,0.45)] bg-white"
              fallback={getTeamMonogram(
                homeTeamStats?.teamName?.trim() || homeTeamName?.trim() || "Mandante",
              )}
              fallbackClassName="text-sm"
              imageClassName="p-2"
              shape="circle"
            />
            <div>
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
                Mandante
              </p>
              <p className="mt-2 text-lg font-semibold text-[#111c2d]">
                {homeTeamStats?.teamName?.trim() || homeTeamName?.trim() || "Mandante"}
              </p>
            </div>
          </div>
        </div>
        <ProfileTag className="justify-center">x</ProfileTag>
        <div className="rounded-[1.1rem] bg-[rgba(240,243,255,0.82)] px-4 py-3 text-center md:text-right">
          <div className="flex flex-col items-center gap-3 md:flex-row md:justify-end">
            <div>
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
                Visitante
              </p>
              <p className="mt-2 text-lg font-semibold text-[#111c2d]">
                {awayTeamStats?.teamName?.trim() || awayTeamName?.trim() || "Visitante"}
              </p>
            </div>
            <ProfileMedia
              alt={awayTeamStats?.teamName?.trim() || awayTeamName?.trim() || "Visitante"}
              assetId={awayTeamStats?.teamId ?? awayTeamId}
              category="clubs"
              className="h-12 w-12 border-[rgba(191,201,195,0.45)] bg-white"
              fallback={getTeamMonogram(
                awayTeamStats?.teamName?.trim() || awayTeamName?.trim() || "Visitante",
              )}
              fallbackClassName="text-sm"
              imageClassName="p-2"
              shape="circle"
            />
          </div>
        </div>
      </div>

      <PartialDataBanner
        coverage={coverage}
        message="Algumas estatísticas comparativas ainda estão incompletas para um dos lados."
      />

      {teamStatsItems.length === 0 || visibleCategories.length === 0 ? (
        <div className="rounded-[1.2rem] border border-dashed border-[rgba(112,121,116,0.32)] bg-[rgba(240,243,255,0.78)] px-4 py-5 text-sm text-[#57657a]">
          Nenhum comparativo disponível para esta partida.
        </div>
      ) : (
        <div className="grid gap-4 xl:grid-cols-3">
          {visibleCategories.map(([category, metrics]) => (
            <MetricGroup
              awayTeamStats={awayTeamStats}
              category={category}
              homeTeamStats={homeTeamStats}
              key={category}
              metrics={metrics}
            />
          ))}
        </div>
      )}
    </ProfilePanel>
  );
}
