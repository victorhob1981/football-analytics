"use client";

import { useMemo, useState } from "react";

import Link from "next/link";
import { useSearchParams } from "next/navigation";

import type { ColumnDef } from "@tanstack/react-table";

import { getCompetitionById } from "@/config/competitions.registry";
import { getMetric, formatMetricValue } from "@/config/metrics.registry";
import { listRankingsByEntity } from "@/config/ranking.registry";
import type { RankingDefinition, RankingSortDirection } from "@/config/ranking.types";
import { getSeasonById } from "@/config/seasons.registry";
import { useRankingTable } from "@/features/rankings/hooks";
import type { RankingQueryFilters, RankingTableRow } from "@/features/rankings/types";
import { DataTable } from "@/shared/components/data-display/DataTable";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import {
  ProfileAlert,
  ProfileCoveragePill,
  ProfileKpi,
  ProfileMetricTile,
  ProfilePanel,
  ProfileShell,
  ProfileTag,
} from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import { ProfileRouteCard } from "@/shared/components/profile/ProfileRouteCard";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useResolvedCompetitionContext } from "@/shared/hooks/useResolvedCompetitionContext";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import type { CoverageState } from "@/shared/types/coverage.types";
import {
  buildCanonicalPlayerPath,
  buildCanonicalTeamPath,
  buildMatchesPath,
  buildPlayerResolverPath,
  buildPlayersPath,
  buildSeasonHubTabPath,
  buildTeamsPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";

type RankingTableProps = {
  rankingDefinition: RankingDefinition;
};

function parseMinSample(value: string): number | null {
  const normalizedValue = value.trim();

  if (normalizedValue.length === 0) {
    return null;
  }

  const parsedValue = Number.parseInt(normalizedValue, 10);

  if (!Number.isInteger(parsedValue) || parsedValue < 0) {
    return null;
  }

  return parsedValue;
}

function parseNullableQueryValue(value: string | null): string | null {
  if (!value) {
    return null;
  }

  const normalizedValue = value.trim();
  return normalizedValue.length > 0 ? normalizedValue : null;
}

function formatStageFormatLabel(stageFormat: string | null): string | null {
  switch (stageFormat) {
    case "league_table":
      return "Fase classificatória";
    case "group_table":
      return "Fase de grupos";
    case "knockout":
      return "Mata-mata";
    case "qualification_knockout":
      return "Eliminatória preliminar";
    case "placement_match":
      return "Disputa de colocação";
    default:
      return null;
  }
}

function resolveEntityHref(
  entity: RankingDefinition["entity"],
  entityId: string,
  context: CompetitionSeasonContext | null,
  competitionId: string | null,
  seasonId: string | null,
): string | null {
  if (entity === "player") {
    return context
      ? buildCanonicalPlayerPath(context, entityId)
      : buildPlayerResolverPath(entityId, { competitionId, seasonId });
  }

  if (entity === "team") {
    return context
      ? buildCanonicalTeamPath(context, entityId)
      : buildTeamResolverPath(entityId, { competitionId, seasonId });
  }

  if (entity === "coach") {
    return `/coaches/${entityId}`;
  }

  return null;
}

function resolveRowMetricValue(row: RankingTableRow, metricKey: string): number | null {
  if (typeof row.metricValue === "number" && Number.isFinite(row.metricValue)) {
    return row.metricValue;
  }

  const fallbackValue = row[metricKey];

  if (typeof fallbackValue === "number" && Number.isFinite(fallbackValue)) {
    return fallbackValue;
  }

  return null;
}

function resolveRowCaption(row: RankingTableRow, entity: RankingDefinition["entity"]): string {
  const teamName = typeof row.teamName === "string" ? row.teamName.trim() : "";
  const position = typeof row.position === "string" ? row.position.trim() : "";

  if (entity === "player" && teamName.length > 0) {
    return teamName;
  }

  if (position.length > 0) {
    return position;
  }

  return entity === "team" ? "Equipe" : entity === "player" ? "Jogador" : "Entidade";
}

function resolveTimeScopeLabel(filters: RankingQueryFilters): string {
  if (typeof filters.lastN === "number" && Number.isFinite(filters.lastN)) {
    return `Últimas ${filters.lastN}`;
  }

  if (filters.dateRangeStart || filters.dateRangeEnd) {
    return "Janela personalizada";
  }

  if (filters.roundId?.trim()) {
    return `Rodada ${filters.roundId}`;
  }

  return "Temporada";
}

function getEntityMonogram(entityName: string): string {
  const initials = entityName
    .split(/\s+/)
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((chunk) => chunk[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 3);

  return initials.length > 0 ? initials : "RK";
}

function resolveEntityMediaCategory(
  entity: RankingDefinition["entity"],
): "clubs" | "players" | null {
  if (entity === "player") {
    return "players";
  }

  if (entity === "team") {
    return "clubs";
  }

  return null;
}

function buildRankingHref(rankingId: string, currentSearch: string): string {
  return currentSearch.length > 0
    ? `/rankings/${rankingId}?${currentSearch}`
    : `/rankings/${rankingId}`;
}

function resolveMetricProgress(currentValue: number | null, leaderValue: number | null): number {
  if (
    typeof currentValue !== "number" ||
    !Number.isFinite(currentValue) ||
    typeof leaderValue !== "number" ||
    !Number.isFinite(leaderValue) ||
    leaderValue === 0
  ) {
    return 0;
  }

  return Math.max(0, Math.min(100, (currentValue / leaderValue) * 100));
}

function resolveGapLabel(
  currentValue: number | null,
  nextValue: number | null,
  metricKey: string,
): string {
  if (typeof currentValue !== "number" || typeof nextValue !== "number") {
    return "-";
  }

  return formatMetricValue(metricKey, currentValue - nextValue);
}

function shouldShowCoverageNotice(coverage: CoverageState): boolean {
  if (coverage.status !== "partial") {
    return false;
  }

  if (typeof coverage.percentage === "number") {
    return coverage.percentage < 95;
  }

  return true;
}

function resolveCoverageMessage(coverage: CoverageState): string {
  if (typeof coverage.percentage === "number") {
    return `Cobertura parcial nesta temporada (${coverage.percentage.toFixed(0)}% coberto). Use o ranking como referência, não como leitura exaustiva.`;
  }

  return "Cobertura parcial nesta temporada. Use o ranking como referência, não como leitura exaustiva.";
}

export function RankingTable({ rankingDefinition }: RankingTableProps) {
  const [search, setSearch] = useState("");
  const [sortDirection, setSortDirection] = useState<RankingSortDirection>(
    rankingDefinition.defaultSort,
  );
  const [minSampleInput, setMinSampleInput] = useState(
    rankingDefinition.minSample?.min?.toString() ?? "",
  );
  const minSampleValue = useMemo(() => parseMinSample(minSampleInput), [minSampleInput]);

  const searchParams = useSearchParams();
  const currentSearch = searchParams.toString();
  const selectedStageId = parseNullableQueryValue(searchParams.get("stageId"));
  const selectedStageFormat = parseNullableQueryValue(searchParams.get("stageFormat"));
  const { competitionId, seasonId } = useGlobalFiltersState();
  const resolvedContext = useResolvedCompetitionContext();
  const competitionName = getCompetitionById(competitionId)?.name ?? null;
  const seasonLabel = getSeasonById(seasonId)?.label ?? null;
  const contextLabel =
    competitionName && seasonLabel
      ? `${competitionName} · ${seasonLabel}`
      : (competitionName ?? seasonLabel ?? null);

  const metric = getMetric(rankingDefinition.metricKey);
  const rankingQuery = useRankingTable(rankingDefinition, {
    localFilters: {
      search,
      sortDirection,
      minSampleValue,
      stageId: selectedStageId,
      stageFormat: selectedStageFormat,
    },
  });

  const rows = rankingQuery.data?.rows ?? [];
  const resolvedStage = rankingQuery.data?.stage ?? null;
  const selectedStageLabel =
    typeof resolvedStage?.stageName === "string" && resolvedStage.stageName.trim().length > 0
      ? resolvedStage.stageName.trim()
      : null;
  const selectedStageFormatLabel = formatStageFormatLabel(
    resolvedStage?.stageFormat ?? selectedStageFormat,
  );
  const topRows = rows.slice(0, 3);
  const leaderRow = rows[0] ?? null;
  const leaderValue = leaderRow
    ? resolveRowMetricValue(leaderRow, rankingDefinition.metricKey)
    : null;
  const runnerUpValue = rows[1]
    ? resolveRowMetricValue(rows[1], rankingDefinition.metricKey)
    : null;
  const relatedRankings = useMemo(
    () =>
      listRankingsByEntity(rankingDefinition.entity)
        .filter((candidate) => candidate.id !== rankingDefinition.id)
        .slice(0, 4),
    [rankingDefinition.entity, rankingDefinition.id],
  );
  const seasonHubHref = resolvedContext
    ? buildSeasonHubTabPath(resolvedContext, "rankings", {
        competitionId,
        seasonId,
        roundId: rankingQuery.mergedFilters.roundId,
        stageId: selectedStageId,
        stageFormat: selectedStageFormat,
        venue: rankingQuery.mergedFilters.venue,
        lastN: rankingQuery.mergedFilters.lastN,
        dateRangeStart: rankingQuery.mergedFilters.dateRangeStart,
        dateRangeEnd: rankingQuery.mergedFilters.dateRangeEnd,
      })
    : "/competitions";
  const teamsHref = buildTeamsPath({
    competitionId,
    seasonId,
    roundId: rankingQuery.mergedFilters.roundId,
    stageId: selectedStageId,
    stageFormat: selectedStageFormat,
    venue: rankingQuery.mergedFilters.venue,
    lastN: rankingQuery.mergedFilters.lastN,
    dateRangeStart: rankingQuery.mergedFilters.dateRangeStart,
    dateRangeEnd: rankingQuery.mergedFilters.dateRangeEnd,
  });
  const playersHref = buildPlayersPath({
    competitionId,
    seasonId,
    roundId: rankingQuery.mergedFilters.roundId,
    stageId: selectedStageId,
    stageFormat: selectedStageFormat,
    venue: rankingQuery.mergedFilters.venue,
    lastN: rankingQuery.mergedFilters.lastN,
    dateRangeStart: rankingQuery.mergedFilters.dateRangeStart,
    dateRangeEnd: rankingQuery.mergedFilters.dateRangeEnd,
  });
  const matchesHref = buildMatchesPath({
    competitionId,
    seasonId,
    roundId: rankingQuery.mergedFilters.roundId,
    stageId: selectedStageId,
    stageFormat: selectedStageFormat,
    venue: rankingQuery.mergedFilters.venue,
    lastN: rankingQuery.mergedFilters.lastN,
    dateRangeStart: rankingQuery.mergedFilters.dateRangeStart,
    dateRangeEnd: rankingQuery.mergedFilters.dateRangeEnd,
  });

  const columns = useMemo<Array<ColumnDef<RankingTableRow, unknown>>>(
    () => [
      {
        accessorFn: (row) => row.rank ?? null,
        id: "rank",
        header: "#",
        cell: ({ row }) => (
          <span className="inline-flex min-w-10 items-center justify-center rounded-full bg-[rgba(216,227,251,0.72)] px-3 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-[#003526]">
            {row.original.rank ?? "-"}
          </span>
        ),
      },
      {
        accessorFn: (row) => row.entityName ?? row.entityId,
        id: "entity",
        header: rankingDefinition.entity === "player" ? "Jogador" : "Entidade",
        cell: ({ row }) => {
          const entityName = row.original.entityName ?? row.original.entityId;
          const entityHref = resolveEntityHref(
            rankingDefinition.entity,
            row.original.entityId,
            resolvedContext,
            competitionId,
            seasonId,
          );
          const caption = resolveRowCaption(row.original, rankingDefinition.entity);
          const mediaCategory = resolveEntityMediaCategory(rankingDefinition.entity);
          const content = (
            <div className="flex items-center gap-3">
              {mediaCategory ? (
                <ProfileMedia
                  alt={entityName}
                  assetId={row.original.entityId}
                  category={mediaCategory}
                  className="h-10 w-10 border-0 bg-[rgba(216,227,251,0.82)]"
                  fallback={getEntityMonogram(entityName)}
                  imageClassName="p-1.5"
                  shape="circle"
                />
              ) : null}
              <div className="space-y-1">
                <p className="font-semibold text-[#111c2d]">{entityName}</p>
                <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">{caption}</p>
              </div>
            </div>
          );

          if (!entityHref) {
            return content;
          }

          return (
            <Link className="block transition-colors hover:text-[#00513b]" href={entityHref}>
              {content}
            </Link>
          );
        },
      },
      {
        accessorFn: (row) => resolveRowMetricValue(row, rankingDefinition.metricKey),
        id: "metric",
        header: metric?.label ?? rankingDefinition.metricKey,
        cell: ({ row }) => {
          const metricValue = resolveRowMetricValue(row.original, rankingDefinition.metricKey);

          return (
            <div className="space-y-1 text-right">
              <p className="font-[family:var(--font-profile-headline)] text-lg font-extrabold text-[#111c2d]">
                {formatMetricValue(rankingDefinition.metricKey, metricValue)}
              </p>
              <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
                {row.original.rank === 1 ? "Líder" : "Na disputa"}
              </p>
            </div>
          );
        },
      },
    ],
    [
      competitionId,
      metric?.label,
      rankingDefinition.entity,
      rankingDefinition.metricKey,
      resolvedContext,
      seasonId,
    ],
  );

  if (!metric) {
    return (
      <ProfileShell className="space-y-6">
        <header className="space-y-2">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Rankings
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            {rankingDefinition.label}
          </h1>
        </header>
        <ProfileAlert title="Ranking inválido" tone="critical">
          A métrica configurada para este ranking não foi encontrada.
        </ProfileAlert>
      </ProfileShell>
    );
  }

  if (rankingQuery.isLoading) {
    return (
      <ProfileShell className="space-y-6">
        <header className="space-y-2">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Rankings
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            {rankingDefinition.label}
          </h1>
        </header>
        <LoadingSkeleton height={220} />
        <LoadingSkeleton height={160} />
        <LoadingSkeleton height={420} />
      </ProfileShell>
    );
  }

  if (rankingQuery.isError && rows.length === 0) {
    return (
      <ProfileShell className="space-y-6">
        <header className="space-y-2">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Rankings
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            {rankingDefinition.label}
          </h1>
        </header>
        <ProfileAlert title="Falha ao carregar ranking" tone="critical">
          <p>{rankingQuery.error?.message}</p>
        </ProfileAlert>
      </ProfileShell>
    );
  }

  return (
    <ProfileShell className="space-y-6">
      <div className="flex flex-wrap items-center gap-2 text-[0.7rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
        <Link className="transition-colors hover:text-[#003526]" href="/competitions">
          Competições
        </Link>
        {contextLabel ? (
          <>
            <span className="text-[#8fa097]">/</span>
            <span>{contextLabel}</span>
          </>
        ) : null}
        <span className="text-[#8fa097]">/</span>
        <span>Rankings</span>
      </div>

      <ProfilePanel className="space-y-5" tone="accent">
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.95fr)]">
          <div className="flex items-start gap-5">
            {leaderRow ? (
              (() => {
                const leaderName = leaderRow.entityName ?? leaderRow.entityId;
                const mediaCategory = resolveEntityMediaCategory(rankingDefinition.entity);

                if (!mediaCategory) {
                  return (
                    <div className="flex h-20 w-20 items-center justify-center rounded-full border border-white/12 bg-white/12 text-xl font-black tracking-[0.08em] text-white">
                      {getEntityMonogram(leaderName)}
                    </div>
                  );
                }

                return (
                  <ProfileMedia
                    alt={leaderName}
                    assetId={leaderRow.entityId}
                    category={mediaCategory}
                    className="h-20 w-20 border-white/12 bg-white/12"
                    fallback={getEntityMonogram(leaderName)}
                    fallbackClassName="text-xl tracking-[0.08em] text-white"
                    imageClassName="p-2.5"
                    shape="circle"
                    tone="contrast"
                  />
                );
              })()
            ) : (
              <div className="flex h-20 w-20 items-center justify-center rounded-full border border-white/12 bg-white/12 text-xl font-black tracking-[0.08em] text-white">
                RK
              </div>
            )}

            <div className="space-y-3">
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-white/65">
                Rankings
              </p>
              <div className="flex flex-wrap items-center gap-2">
                <ProfileCoveragePill coverage={rankingQuery.coverage} className="bg-white/16 text-white" />
                <ProfileTag className="bg-white/12 text-white/82">
                  {resolveTimeScopeLabel(rankingQuery.mergedFilters)}
                </ProfileTag>
                {contextLabel ? (
                  <ProfileTag className="bg-white/12 text-white/82">{contextLabel}</ProfileTag>
                ) : null}
                {selectedStageLabel ? (
                  <ProfileTag className="bg-white/12 text-white/82">{selectedStageLabel}</ProfileTag>
                ) : null}
                {selectedStageFormatLabel ? (
                  <ProfileTag className="bg-white/12 text-white/82">{selectedStageFormatLabel}</ProfileTag>
                ) : null}
                <ProfileTag className="bg-white/12 text-white/82">{metric.label}</ProfileTag>
              </div>
              <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-white md:text-5xl">
                {rankingDefinition.label}
              </h1>
              <p className="max-w-3xl text-sm leading-6 text-white/74">
                {rankingDefinition.description}
              </p>
              {leaderRow ? (
                <p className="text-sm font-semibold text-white/82">
                  Líder atual: {leaderRow.entityName ?? leaderRow.entityId}
                </p>
              ) : null}
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <ProfileKpi
              hint="Entidades com linha no ranking"
              invert
              label="Entidades"
              value={rows.length}
            />
            <ProfileKpi
              hint={metric.label}
              invert
              label="Topo"
              value={formatMetricValue(rankingDefinition.metricKey, leaderValue)}
            />
            <ProfileKpi
              hint="Sobre o vice-líder"
              invert
              label="Vantagem"
              value={resolveGapLabel(leaderValue, runnerUpValue, rankingDefinition.metricKey)}
            />
            <ProfileKpi
              hint="Filtro local"
              invert
              label="Amostra mínima"
              value={minSampleValue ?? rankingDefinition.minSample?.min ?? "-"}
            />
          </div>
        </div>
      </ProfilePanel>

      {rankingQuery.isError ? (
        <ProfileAlert title="Dados carregados com alerta" tone="warning">
          <p>{rankingQuery.error?.message}</p>
        </ProfileAlert>
      ) : null}

      {shouldShowCoverageNotice(rankingQuery.coverage) ? (
        <PartialDataBanner
          coverage={rankingQuery.coverage}
          message={resolveCoverageMessage(rankingQuery.coverage)}
        />
      ) : null}

      <section className="grid gap-4 xl:grid-cols-4">
        <ProfileRouteCard
          description="Volte para a temporada e releia a mesma competição antes de trocar de profundidade."
          href={seasonHubHref}
          label="Contexto canônico"
          title="Temporada"
        />
        <ProfileRouteCard
          description="Abra atletas ligados ao ranking sem perder competição, temporada e janela."
          href={playersHref}
          label="Saída canônica"
          title="Jogadores"
        />
        <ProfileRouteCard
          description="Cruze o ranking com a lista de times para aprofundar campanha, elenco e perfil."
          href={teamsHref}
          label="Saída canônica"
          title="Times"
        />
        <ProfileRouteCard
          description="Use a disputa do ranking como entrada para o calendário completo da temporada."
          href={matchesHref}
          label="Calendário"
          title="Partidas"
        />
      </section>

      <section className="grid gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,0.85fr)]">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {topRows.length > 0 ? (
            topRows.map((row, index) => {
              const metricValue = resolveRowMetricValue(row, rankingDefinition.metricKey);
              const metricProgress = resolveMetricProgress(metricValue, leaderValue);
              const entityHref = resolveEntityHref(
                rankingDefinition.entity,
                row.entityId,
                resolvedContext,
                competitionId,
                seasonId,
              );
              const entityName = row.entityName ?? row.entityId;
              const mediaCategory = resolveEntityMediaCategory(rankingDefinition.entity);
              const cardTone = index === 0 ? "accent" : "base";
              const cardMetaClassName = cardTone === "accent" ? "text-white/70" : "text-[#57657a]";
              const content = (
                <>
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex items-start gap-3">
                      {mediaCategory ? (
                        <ProfileMedia
                          alt={entityName}
                          assetId={row.entityId}
                          category={mediaCategory}
                          className={
                            cardTone === "accent"
                              ? "h-14 w-14 border-white/12 bg-white/12"
                              : "h-14 w-14 border-[rgba(191,201,195,0.45)] bg-white"
                          }
                          fallback={getEntityMonogram(entityName)}
                          fallbackClassName={cardTone === "accent" ? "text-sm text-white" : "text-sm"}
                          imageClassName="p-2"
                          shape="circle"
                          tone={cardTone === "accent" ? "contrast" : "base"}
                        />
                      ) : null}
                      <p
                        className={`text-[0.72rem] font-semibold uppercase tracking-[0.18em] ${cardMetaClassName}`}
                      >
                        #{row.rank ?? index + 1}
                      </p>
                      <div>
                        <p className="font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-inherit">
                          {entityName}
                        </p>
                        <p
                          className={`mt-1 text-xs uppercase tracking-[0.16em] ${cardMetaClassName}`}
                        >
                          {resolveRowCaption(row, rankingDefinition.entity)}
                        </p>
                      </div>
                    </div>
                    <div className="text-right">
                      <p className="font-[family:var(--font-profile-headline)] text-3xl font-extrabold text-inherit">
                        {formatMetricValue(rankingDefinition.metricKey, metricValue)}
                      </p>
                      <p
                        className={`mt-1 text-[0.68rem] uppercase tracking-[0.18em] ${cardMetaClassName}`}
                      >
                        {metric.label}
                      </p>
                    </div>
                  </div>
                  <div className="mt-5 h-1.5 overflow-hidden rounded-full bg-white/20">
                    <div
                      className={
                        cardTone === "accent"
                          ? "h-full rounded-full bg-white"
                          : "h-full rounded-full bg-[#003526]"
                      }
                      style={{ width: `${metricProgress}%` }}
                    />
                  </div>
                </>
              );

              return (
                <ProfilePanel className="space-y-4" key={row.entityId} tone={cardTone}>
                  {entityHref ? (
                    <Link className="block" href={entityHref}>
                      {content}
                    </Link>
                  ) : (
                    content
                  )}
                </ProfilePanel>
              );
            })
          ) : (
            <ProfilePanel className="md:col-span-2 xl:col-span-3">
              <EmptyState
                description="Não há entidades para os filtros globais e locais atuais."
                title="Ranking vazio"
              />
            </ProfilePanel>
          )}
        </div>

        <ProfilePanel className="space-y-5" tone="soft">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
                Leitura do ranking
              </p>
              <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
                Como está a disputa
              </h2>
            </div>
            <ProfileTag>{resolveTimeScopeLabel(rankingQuery.mergedFilters)}</ProfileTag>
            {selectedStageLabel ? <ProfileTag>{selectedStageLabel}</ProfileTag> : null}
            {selectedStageFormatLabel ? <ProfileTag>{selectedStageFormatLabel}</ProfileTag> : null}
          </div>

          <div className="grid gap-3 sm:grid-cols-2">
            <ProfileMetricTile label="Entidades avaliadas" value={rows.length} />
            <ProfileMetricTile
              label="Amostra mínima"
              value={minSampleValue ?? rankingDefinition.minSample?.min ?? "-"}
            />
            <ProfileMetricTile
              label="Líder"
              value={leaderRow ? (leaderRow.entityName ?? leaderRow.entityId) : "-"}
            />
            <ProfileMetricTile
              label="Vantagem sobre o vice"
              value={resolveGapLabel(leaderValue, runnerUpValue, rankingDefinition.metricKey)}
            />
          </div>

          {rankingDefinition.coverageWarning ? (
            <ProfileAlert title="Leitura importante" tone="info">
              {rankingDefinition.coverageWarning}
            </ProfileAlert>
          ) : null}

          <div className="space-y-3">
            <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
              Outros rankings relacionados
            </p>
            <div className="grid gap-2">
              {relatedRankings.map((relatedRanking) => (
                <Link
                  className="rounded-[1.2rem] border border-[rgba(191,201,195,0.55)] bg-white/80 px-4 py-3 transition-colors hover:border-[#8bd6b6] hover:bg-white"
                  href={buildRankingHref(relatedRanking.id, currentSearch)}
                  key={relatedRanking.id}
                >
                  <p className="font-semibold text-[#111c2d]">{relatedRanking.label}</p>
                  <p className="mt-1 text-xs uppercase tracking-[0.16em] text-[#57657a]">
                    {getMetric(relatedRanking.metricKey)?.label ?? relatedRanking.metricKey}
                  </p>
                </Link>
              ))}
            </div>
          </div>
        </ProfilePanel>
      </section>

      <ProfilePanel className="space-y-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div className="space-y-2">
            <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
              Ajustes da tabela
            </p>
            <h2 className="font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
              Refine o recorte sem sair da página
            </h2>
            <p className="text-sm text-[#57657a]">
              Use busca, ordenação e amostra mínima para comparar o ranking sem perder a mesma
              temporada.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <ProfileTag>{metric.label}</ProfileTag>
            <ProfileTag>{resolveTimeScopeLabel(rankingQuery.mergedFilters)}</ProfileTag>
            {selectedStageLabel ? <ProfileTag>{selectedStageLabel}</ProfileTag> : null}
            {selectedStageFormatLabel ? <ProfileTag>{selectedStageFormatLabel}</ProfileTag> : null}
          </div>
        </div>

        <section className="grid gap-3 md:grid-cols-3">
          <label className="flex flex-col gap-2 text-sm text-[#1f2d40]">
            Buscar{" "}
            {rankingDefinition.entity === "player"
              ? "jogador"
              : rankingDefinition.entity === "coach"
                ? "técnico"
                : "time"}
            <div className="flex items-center gap-3 rounded-[1.3rem] border border-[rgba(191,201,195,0.55)] bg-[#f9f9ff] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
              <span className="flex h-8 w-8 items-center justify-center rounded-full bg-[rgba(216,227,251,0.82)] text-xs font-semibold text-[#003526]">
                Q
              </span>
              <input
                className="w-full border-0 bg-transparent text-sm text-[#111c2d] outline-none placeholder:text-[#76859a]"
                onChange={(event) => {
                  setSearch(event.target.value);
                }}
                placeholder={`Buscar ${
                  rankingDefinition.entity === "player"
                    ? "jogador"
                    : rankingDefinition.entity === "coach"
                      ? "técnico"
                      : "time"
                }`}
                type="text"
                value={search}
              />
            </div>
          </label>

          <label className="flex flex-col gap-2 text-sm text-[#1f2d40]">
            Ordenar ranking
            <div className="flex items-center gap-3 rounded-[1.3rem] border border-[rgba(191,201,195,0.55)] bg-[#f9f9ff] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
              <span className="flex h-8 w-8 items-center justify-center rounded-full bg-[rgba(216,227,251,0.82)] text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
                Ord
              </span>
              <select
                className="w-full border-0 bg-transparent text-sm text-[#111c2d] outline-none ring-0"
                onChange={(event) => {
                  setSortDirection(event.target.value as RankingSortDirection);
                }}
                value={sortDirection}
              >
                <option value="desc">Descendente</option>
                <option value="asc">Ascendente</option>
              </select>
            </div>
          </label>

          <label className="flex flex-col gap-2 text-sm text-[#1f2d40]">
            Amostra mínima
            <div className="flex items-center gap-3 rounded-[1.3rem] border border-[rgba(191,201,195,0.55)] bg-[#f9f9ff] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
              <span className="flex h-8 w-8 items-center justify-center rounded-full bg-[rgba(216,227,251,0.82)] text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
                Min
              </span>
              <input
                className="w-full border-0 bg-transparent text-sm text-[#111c2d] outline-none ring-0 placeholder:text-[#76859a]"
                onChange={(event) => {
                  setMinSampleInput(event.target.value);
                }}
                placeholder={rankingDefinition.minSample?.min?.toString() ?? "Opcional"}
                type="number"
                value={minSampleInput}
              />
            </div>
          </label>

        </section>
      </ProfilePanel>

      <ProfilePanel className="space-y-5" tone="soft">
        <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
              Ranking detalhado
            </p>
            <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
              Tabela completa
            </h2>
          </div>
          {rankingQuery.data?.updatedAt ? (
            <ProfileTag>
              Atualizado em {new Date(rankingQuery.data.updatedAt).toLocaleDateString("pt-BR")}
            </ProfileTag>
          ) : null}
        </div>

        <DataTable<RankingTableRow>
          columns={columns}
          data={rows}
          emptyDescription="Não há linhas para os filtros atuais."
          emptyTitle="Ranking vazio"
          enableVirtualization
          initialPageSize={50}
          loading={false}
          pageSizeOptions={[25, 50, 100]}
          variant="profile"
          virtualizerMaxHeight={520}
        />
      </ProfilePanel>
    </ProfileShell>
  );
}
