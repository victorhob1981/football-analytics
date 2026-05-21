import Link from "next/link";

import type { ColumnDef } from "@tanstack/react-table";

import { formatMetricValue } from "@/config/metrics.registry";
import type { MatchPlayerStat } from "@/features/matches/types";
import { DataTable } from "@/shared/components/data-display/DataTable";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import {
  ProfileCoveragePill,
  ProfileMetricTile,
  ProfilePanel,
  ProfileTag,
} from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import type { CoverageState } from "@/shared/types/coverage.types";
import type {
  CompetitionSeasonContext,
  CompetitionSeasonContextInput,
} from "@/shared/types/context.types";
import {
  appendFilterQueryString,
  buildCanonicalPlayerPath,
  buildPlayerResolverPath,
} from "@/shared/utils/context-routing";

type MatchPlayerStatsPlaceholderProps = {
  coverage?: CoverageState;
  competitionContext?: CompetitionSeasonContext | null;
  contextInput?: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  };
  playerStats: MatchPlayerStat[] | undefined;
};

function resolvePlayerHref(
  playerId: string | null | undefined,
  competitionContext: CompetitionSeasonContext | null,
  contextInput: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  },
): string | null {
  if (!playerId) {
    return null;
  }

  if (!competitionContext) {
    return buildPlayerResolverPath(playerId, contextInput);
  }

  return appendFilterQueryString(
    buildCanonicalPlayerPath(competitionContext, playerId),
    contextInput,
    ["competitionId", "seasonId"],
  );
}

function resolveTopPlayer(
  statsItems: MatchPlayerStat[],
  metricSelector: (playerStat: MatchPlayerStat) => number | null | undefined,
): MatchPlayerStat | null {
  return (
    [...statsItems].sort((left, right) => {
      const leftValue = metricSelector(left) ?? Number.NEGATIVE_INFINITY;
      const rightValue = metricSelector(right) ?? Number.NEGATIVE_INFINITY;
      return rightValue - leftValue;
    })[0] ?? null
  );
}

function formatLeaderLabel(
  playerStat: MatchPlayerStat | null,
  metricKey: Parameters<typeof formatMetricValue>[0],
  metricValue: number | null | undefined,
): string {
  if (!playerStat) {
    return "-";
  }

  return `${playerStat.playerName?.trim() || "Jogador"} · ${formatMetricValue(metricKey, metricValue)}`;
}

function getPlayerMonogram(playerName: string): string {
  const initials = playerName
    .split(/\s+/)
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => chunk[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 2);

  return initials.length > 0 ? initials : "PL";
}

export function MatchPlayerStatsPlaceholder({
  coverage = { status: "unknown", label: "Estatísticas dos jogadores" },
  competitionContext = null,
  contextInput = {},
  playerStats,
}: MatchPlayerStatsPlaceholderProps) {
  const statsItems = [...(playerStats ?? [])].sort((left, right) => {
    const rightRating = right.rating ?? Number.NEGATIVE_INFINITY;
    const leftRating = left.rating ?? Number.NEGATIVE_INFINITY;

    if (rightRating !== leftRating) {
      return rightRating - leftRating;
    }

    const rightGoals = right.goals ?? Number.NEGATIVE_INFINITY;
    const leftGoals = left.goals ?? Number.NEGATIVE_INFINITY;
    return rightGoals - leftGoals;
  });

  const topRatedPlayer = resolveTopPlayer(statsItems, (playerStat) => playerStat.rating);
  const topScorer = resolveTopPlayer(statsItems, (playerStat) => playerStat.goals);
  const topCreator = resolveTopPlayer(statsItems, (playerStat) => playerStat.keyPasses);

  const columns: Array<ColumnDef<MatchPlayerStat, unknown>> = [
    {
      accessorFn: (row) => row.playerName ?? row.playerId ?? "",
      id: "player",
      header: "Jogador",
      cell: ({ row }) => {
        const playerName = row.original.playerName?.trim() || "Jogador sem nome";
        const playerHref = resolvePlayerHref(
          row.original.playerId,
          competitionContext,
          contextInput,
        );
        const teamLabel = row.original.teamName?.trim() || "Sem time";
        const content = (
          <div className="flex items-center gap-3">
            <ProfileMedia
              alt={playerName}
              assetId={row.original.playerId}
              category="players"
              className="h-10 w-10 border-0 bg-[rgba(216,227,251,0.82)]"
              fallback={getPlayerMonogram(playerName)}
              imageClassName="p-1.5"
              shape="circle"
            />
            <div className="space-y-1">
              <p className="font-semibold text-[#111c2d]">{playerName}</p>
              <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
                {[
                  teamLabel,
                  row.original.positionName?.trim() || null,
                  row.original.isStarter ? "Titular" : "Banco",
                ]
                  .filter(Boolean)
                  .join(" · ")}
              </p>
            </div>
          </div>
        );

        return playerHref ? (
          <Link className="block transition-colors hover:text-[#00513b]" href={playerHref}>
            {content}
          </Link>
        ) : (
          content
        );
      },
    },
    {
      accessorFn: (row) => row.minutesPlayed ?? null,
      id: "minutes",
      header: "Min",
      cell: ({ row }) => formatMetricValue("minutes_played", row.original.minutesPlayed),
    },
    {
      accessorFn: (row) => row.goals ?? null,
      id: "goals",
      header: "Gols",
      cell: ({ row }) => formatMetricValue("goals", row.original.goals),
    },
    {
      accessorFn: (row) => row.assists ?? null,
      id: "assists",
      header: "Assistências",
      cell: ({ row }) => formatMetricValue("assists", row.original.assists),
    },
    {
      accessorFn: (row) => row.shotsTotal ?? null,
      id: "shots",
      header: "Finalizações",
      cell: ({ row }) => formatMetricValue("shots_total", row.original.shotsTotal),
    },
    {
      accessorFn: (row) => row.shotsOnGoal ?? null,
      id: "shots-on-goal",
      header: "No alvo",
      cell: ({ row }) => formatMetricValue("shots_on_target", row.original.shotsOnGoal),
    },
    {
      accessorFn: (row) => row.keyPasses ?? null,
      id: "key-passes",
      header: "Passes-chave",
      cell: ({ row }) => formatMetricValue("key_passes", row.original.keyPasses),
    },
    {
      accessorFn: (row) => row.rating ?? null,
      id: "rating",
      header: "Nota",
      cell: ({ row }) => formatMetricValue("player_rating", row.original.rating),
    },
  ];

  return (
    <ProfilePanel className="space-y-5" tone="soft">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Jogadores
          </p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
            Atuação individual
          </h2>
          <p className="mt-2 text-sm leading-6 text-[#57657a]">
            Minutos, gols, assistências, finalizações, passes-chave e nota em uma leitura direta da partida.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <ProfileCoveragePill coverage={coverage} />
          <ProfileTag>{statsItems.length} jogadores avaliados</ProfileTag>
        </div>
      </div>

      <PartialDataBanner
        coverage={coverage}
        message="Algumas estatísticas individuais ainda estão incompletas nesta partida."
      />

      <div className="grid gap-3 md:grid-cols-3">
        <ProfileMetricTile
          label="Melhor nota"
          value={formatLeaderLabel(topRatedPlayer, "player_rating", topRatedPlayer?.rating)}
        />
        <ProfileMetricTile
          label="Mais gols"
          value={formatLeaderLabel(topScorer, "goals", topScorer?.goals)}
        />
        <ProfileMetricTile
          label="Mais passes-chave"
          value={formatLeaderLabel(topCreator, "key_passes", topCreator?.keyPasses)}
        />
      </div>

      <DataTable<MatchPlayerStat>
        columns={columns}
        data={statsItems}
        emptyDescription="Nenhuma estatística individual retornada para esta partida."
        emptyTitle="Sem estatísticas de jogadores"
        initialPageSize={8}
        pageSizeOptions={[8, 12, 20]}
        variant="profile"
      />
    </ProfilePanel>
  );
}
