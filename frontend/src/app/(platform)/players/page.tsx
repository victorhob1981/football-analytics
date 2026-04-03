"use client";

import { useCallback, useMemo, useState } from "react";

import Link from "next/link";

import { useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";

import { getCompetitionById } from "@/config/competitions.registry";
import { formatMetricValue } from "@/config/metrics.registry";
import { getSeasonById } from "@/config/seasons.registry";
import { usePlayersList } from "@/features/players/hooks";
import { playersQueryKeys } from "@/features/players/queryKeys";
import { fetchPlayerProfile } from "@/features/players/services/players.service";
import type { PlayerListItem, PlayerProfileFilters } from "@/features/players/types";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { DataTable } from "@/shared/components/data-display/DataTable";
import {
  ProfileAlert,
  ProfileKpi,
  ProfileTag,
  profileHeadlineVariableClassName,
  profileTypographyClassName,
} from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import { ProfileRouteCard } from "@/shared/components/profile/ProfileRouteCard";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useResolvedCompetitionContext } from "@/shared/hooks/useResolvedCompetitionContext";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useComparisonStore } from "@/shared/stores/comparison.store";
import {
  appendFilterQueryString,
  buildCanonicalPlayerPath,
  buildCanonicalTeamPath,
  buildMatchesPath,
  buildPlayerResolverPath,
  buildRankingPath,
  buildSeasonHubTabPath,
  buildTeamsPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";

function parseMinMinutes(value: string): number | null {
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

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", {
  maximumFractionDigits: 0,
});

const DECIMAL_FORMATTER = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatInteger(value: number): string {
  return INTEGER_FORMATTER.format(value);
}

function formatDecimal(value: number | null): string {
  if (value === null) {
    return "-";
  }

  return DECIMAL_FORMATTER.format(value);
}

function getInitials(name: string): string {
  const tokens = name
    .trim()
    .split(/\s+/)
    .filter((token) => token.length > 0)
    .slice(0, 2);

  if (tokens.length === 0) {
    return "PL";
  }

  return tokens.map((token) => token[0]?.toUpperCase() ?? "").join("");
}

function formatPosition(position: string | null | undefined): string {
  if (!position) {
    return "Sem posicao";
  }

  const normalizedPosition = position.trim();

  if (normalizedPosition.length <= 3) {
    return normalizedPosition.toUpperCase();
  }

  return normalizedPosition;
}

function describeVenue(venue: string): string {
  if (venue === "home") {
    return "Casa";
  }

  if (venue === "away") {
    return "Fora";
  }

  return "Todos os mandos";
}

function describeTimeWindow(params: {
  roundId: string | null;
  lastN: number | null;
  dateRangeStart: string | null;
  dateRangeEnd: string | null;
}): string {
  if (params.lastN !== null) {
    return `Ultimas ${params.lastN} partidas`;
  }

  if (params.dateRangeStart !== null || params.dateRangeEnd !== null) {
    const startLabel = params.dateRangeStart ?? "...";
    const endLabel = params.dateRangeEnd ?? "...";

    return `${startLabel} ate ${endLabel}`;
  }

  if (params.roundId !== null) {
    return `Rodada ${params.roundId}`;
  }

  return "Temporada inteira";
}

export default function PlayersPage() {
  const [search, setSearch] = useState("");
  const [minMinutesInput, setMinMinutesInput] = useState("");
  const queryClient = useQueryClient();
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const resolvedContext = useResolvedCompetitionContext();
  const { activeMode, params: timeRangeParams } = useTimeRange();
  const comparisonEntityType = useComparisonStore((state) => state.entityType);
  const selectedIds = useComparisonStore((state) => state.selectedIds);
  const addToComparison = useComparisonStore((state) => state.add);
  const removeFromComparison = useComparisonStore((state) => state.remove);
  const setComparisonEntityType = useComparisonStore((state) => state.setEntityType);

  const competitionName = getCompetitionById(competitionId)?.name;
  const seasonLabel = getSeasonById(seasonId)?.label;
  const pageTitle = competitionName
    ? `Jogadores - ${competitionName}${seasonLabel ? ` (${seasonLabel})` : ""}`
    : "Jogadores Gerais";

  const normalizedMinMinutes = useMemo(() => parseMinMinutes(minMinutesInput), [minMinutesInput]);
  const selectedIdsSet = useMemo(() => new Set(selectedIds), [selectedIds]);
  const activeWindowLabel = useMemo(() => describeTimeWindow(timeRangeParams), [timeRangeParams]);

  const playersQuery = usePlayersList({
    search,
    minMinutes: normalizedMinMinutes,
  });

  const detailPrefetchFilters = useMemo<PlayerProfileFilters>(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
      includeRecentMatches: true,
    }),
    [
      competitionId,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const prefetchPlayerDetail = useCallback(
    (playerId: string) => {
      const normalizedPlayerId = playerId.trim();

      if (normalizedPlayerId.length === 0) {
        return;
      }

      void queryClient.prefetchQuery({
        queryKey: playersQueryKeys.profile(normalizedPlayerId, detailPrefetchFilters),
        queryFn: () => fetchPlayerProfile(normalizedPlayerId, detailPrefetchFilters),
        staleTime: 5 * 60 * 1000,
      });
    },
    [detailPrefetchFilters, queryClient],
  );

  const getPlayerHref = useCallback(
    (playerId: string) =>
      resolvedContext
        ? appendFilterQueryString(
            buildCanonicalPlayerPath(resolvedContext, playerId),
            {
              competitionId,
              seasonId,
              roundId: timeRangeParams.roundId,
              venue,
              lastN: timeRangeParams.lastN,
              dateRangeStart: timeRangeParams.dateRangeStart,
              dateRangeEnd: timeRangeParams.dateRangeEnd,
            },
            ["competitionId", "seasonId"],
          )
        : buildPlayerResolverPath(playerId, {
            competitionId,
            seasonId,
            roundId: timeRangeParams.roundId,
            venue,
            lastN: timeRangeParams.lastN,
            dateRangeStart: timeRangeParams.dateRangeStart,
            dateRangeEnd: timeRangeParams.dateRangeEnd,
          }),
    [
      competitionId,
      resolvedContext,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const getTeamHref = useCallback(
    (teamId: string) =>
      resolvedContext
        ? appendFilterQueryString(
            buildCanonicalTeamPath(resolvedContext, teamId),
            {
              competitionId,
              seasonId,
              roundId: timeRangeParams.roundId,
              venue,
              lastN: timeRangeParams.lastN,
              dateRangeStart: timeRangeParams.dateRangeStart,
              dateRangeEnd: timeRangeParams.dateRangeEnd,
            },
            ["competitionId", "seasonId"],
          )
        : buildTeamResolverPath(teamId, {
            competitionId,
            seasonId,
            roundId: timeRangeParams.roundId,
            venue,
            lastN: timeRangeParams.lastN,
            dateRangeStart: timeRangeParams.dateRangeStart,
            dateRangeEnd: timeRangeParams.dateRangeEnd,
          }),
    [
      competitionId,
      resolvedContext,
      seasonId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const handleCompareAction = useCallback(
    (playerId: string) => {
      if (comparisonEntityType !== "player") {
        setComparisonEntityType("player");
      }

      if (selectedIdsSet.has(playerId)) {
        removeFromComparison(playerId);
        return;
      }

      addToComparison(playerId);
    },
    [
      addToComparison,
      comparisonEntityType,
      removeFromComparison,
      selectedIdsSet,
      setComparisonEntityType,
    ],
  );

  const tableData = useMemo(() => {
    const items = playersQuery.data?.items ?? [];
    const normalizedSearch = search.trim().toLowerCase();

    return items.filter((item) => {
      const playerName = item.playerName?.toLowerCase() ?? "";
      const passesSearchFilter =
        normalizedSearch.length === 0 || playerName.includes(normalizedSearch);
      const playerMinutes = item.minutesPlayed ?? 0;
      const passesMinMinutesFilter =
        normalizedMinMinutes === null || playerMinutes >= normalizedMinMinutes;

      return passesSearchFilter && passesMinMinutesFilter;
    });
  }, [normalizedMinMinutes, playersQuery.data?.items, search]);

  const derivedSummary = useMemo(() => {
    const totalGoals = tableData.reduce((sum, item) => sum + (item.goals ?? 0), 0);
    const totalAssists = tableData.reduce((sum, item) => sum + (item.assists ?? 0), 0);
    const totalMinutes = tableData.reduce((sum, item) => sum + (item.minutesPlayed ?? 0), 0);
    const ratingValues = tableData
      .map((item) => item.rating)
      .filter((rating): rating is number => typeof rating === "number");

    return {
      totalGoals,
      totalAssists,
      totalMinutes,
      goalInvolvements: totalGoals + totalAssists,
      ratedPlayers: ratingValues.length,
      averageRating:
        ratingValues.length > 0
          ? ratingValues.reduce((sum, rating) => sum + rating, 0) / ratingValues.length
          : null,
    };
  }, [tableData]);

  const featuredPlayers = useMemo(
    () =>
      [...tableData]
        .sort((left, right) => {
          const ratingDelta = (right.rating ?? -1) - (left.rating ?? -1);

          if (ratingDelta !== 0) {
            return ratingDelta;
          }

          const goalDelta = (right.goals ?? 0) - (left.goals ?? 0);

          if (goalDelta !== 0) {
            return goalDelta;
          }

          return (right.minutesPlayed ?? 0) - (left.minutesPlayed ?? 0);
        })
        .slice(0, 4),
    [tableData],
  );

  const selectedPlayersPreview = useMemo(
    () =>
      selectedIds.map((playerId) => {
        const player = tableData.find((candidate) => candidate.playerId === playerId);

        return {
          playerId,
          label: player?.playerName ?? playerId,
        };
      }),
    [selectedIds, tableData],
  );

  const canNavigateDirectly = resolvedContext !== null;
  const localFiltersDescription =
    normalizedMinMinutes === null
      ? "Sem piso local de minutos"
      : `Minimo local de ${formatInteger(normalizedMinMinutes)} minutos`;
  const discoverySummary =
    search.trim().length > 0
      ? `Busca refinada por "${search.trim()}"`
      : "Panorama de jogadores com os filtros atuais";
  const loadingSummaryValue = playersQuery.isLoading ? "..." : formatInteger(tableData.length);
  const averageRatingValue = playersQuery.isLoading
    ? "..."
    : formatDecimal(derivedSummary.averageRating);
  const goalContributionValue = playersQuery.isLoading
    ? "..."
    : formatInteger(derivedSummary.goalInvolvements);
  const compareStatusCopy =
    selectedPlayersPreview.length === 0
      ? "Selecione ate dois jogadores para comparar sem sair desta tela."
      : selectedPlayersPreview.length === 1
        ? "Um jogador selecionado. Escolha mais um para abrir a comparacao."
        : "Dois jogadores selecionados. A comparacao esta pronta.";
  const resultsEmptyTitle = playersQuery.isError ? "Lista indisponivel" : "Sem jogadores";
  const resultsEmptyDescription = playersQuery.isError
    ? "Nao foi possivel consultar a lista de jogadores agora."
    : "Nenhum jogador encontrado com os filtros atuais.";
  const seasonHubHref = resolvedContext
    ? buildSeasonHubTabPath(resolvedContext, "rankings", {
        competitionId,
        seasonId,
        roundId: timeRangeParams.roundId,
        venue,
        lastN: timeRangeParams.lastN,
        dateRangeStart: timeRangeParams.dateRangeStart,
        dateRangeEnd: timeRangeParams.dateRangeEnd,
      })
    : "/competitions";

  const columns = useMemo<Array<ColumnDef<PlayerListItem, unknown>>>(
    () => [
      {
        accessorKey: "playerName",
        header: "Jogador",
        cell: ({ row }) => {
          const player = row.original;

          return (
            <div className="flex items-center gap-3">
              <ProfileMedia
                alt={player.playerName}
                assetId={player.playerId}
                category="players"
                className="h-10 w-10 border-0 bg-[rgba(216,227,251,0.82)]"
                fallback={getInitials(player.playerName)}
                imageClassName="p-1.5"
                shape="circle"
              />
              <div className="min-w-0 space-y-1">
                <Link
                  className="block truncate font-semibold text-[#111c2d] transition-colors hover:text-[#003526]"
                  href={getPlayerHref(player.playerId)}
                  onFocus={() => {
                    prefetchPlayerDetail(player.playerId);
                  }}
                  onMouseEnter={() => {
                    prefetchPlayerDetail(player.playerId);
                  }}
                >
                  {player.playerName}
                </Link>
                <div className="flex flex-wrap items-center gap-2 text-xs text-[#57657a]">
                  <span>{player.nationality ?? "Nacionalidade nao informada"}</span>
                  {typeof player.matchesPlayed === "number" ? (
                    <span className="rounded-full bg-[rgba(240,243,255,0.96)] px-2 py-0.5 font-medium text-[#57657a]">
                      {formatInteger(player.matchesPlayed)} jogos
                    </span>
                  ) : null}
                </div>
              </div>
            </div>
          );
        },
      },
      {
        accessorKey: "teamName",
        header: "Time",
        cell: ({ row }) => {
          const player = row.original;
          const teamName = player.teamName ?? "-";

          if (!player.teamId || teamName === "-") {
            return <span className="text-sm text-[#57657a]">{teamName}</span>;
          }

          return (
            <Link
              className="inline-flex items-center gap-2 rounded-full bg-[rgba(240,243,255,0.96)] px-3 py-1 text-xs font-semibold text-[#1f2d40] transition-colors hover:bg-[rgba(216,227,251,0.96)]"
              href={getTeamHref(player.teamId)}
            >
              <ProfileMedia
                alt={teamName}
                assetId={player.teamId}
                category="clubs"
                className="h-6 w-6 border-0 bg-white"
                fallback={getInitials(teamName)}
                fallbackClassName="text-[0.6rem]"
                imageClassName="p-1"
                shape="circle"
              />
              <span className="truncate">{teamName}</span>
            </Link>
          );
        },
      },
      {
        accessorKey: "position",
        header: "Posicao",
        cell: ({ row }) => (
          <span className="inline-flex rounded-full bg-[rgba(240,243,255,0.96)] px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
            {formatPosition(row.original.position)}
          </span>
        ),
      },
      {
        accessorKey: "minutesPlayed",
        header: "Minutos",
        cell: ({ row }) => (
          <span className="font-medium tabular-nums text-[#1f2d40]">
            {formatMetricValue("minutes_played", row.original.minutesPlayed)}
          </span>
        ),
      },
      {
        accessorKey: "goals",
        header: "Gols",
        cell: ({ row }) => (
          <span className="font-medium tabular-nums text-[#1f2d40]">
            {formatMetricValue("goals", row.original.goals)}
          </span>
        ),
      },
      {
        accessorKey: "assists",
        header: "Assistencias",
        cell: ({ row }) => (
          <span className="font-medium tabular-nums text-[#1f2d40]">
            {formatMetricValue("assists", row.original.assists)}
          </span>
        ),
      },
      {
        accessorKey: "rating",
        header: "Rating",
        cell: ({ row }) => {
          const rating = row.original.rating;

          return (
            <span
              className={`inline-flex min-w-16 justify-center rounded-full px-3 py-1 text-xs font-semibold ${
                typeof rating === "number"
                  ? "bg-[#003526] text-white"
                  : "bg-[rgba(240,243,255,0.96)] text-[#57657a]"
              }`}
            >
              {formatMetricValue("player_rating", rating)}
            </span>
          );
        },
      },
      {
        id: "compare",
        header: "Comparar",
        enableSorting: false,
        cell: ({ row }) => {
          const player = row.original;
          const isSelected = selectedIdsSet.has(player.playerId);
          const canAddMore = selectedIds.length < 2;
          const isDisabled = !isSelected && !canAddMore;

          return (
            <button
              aria-pressed={isSelected}
              className={`rounded-full px-3 py-1.5 text-xs font-semibold transition-colors ${
                isSelected
                  ? "bg-[#003526] text-white"
                  : "border border-[rgba(112,121,116,0.28)] bg-white/90 text-[#1f2d40]"
              } disabled:cursor-not-allowed disabled:opacity-50`}
              disabled={isDisabled}
              onClick={() => {
                handleCompareAction(player.playerId);
              }}
              type="button"
            >
              {isSelected ? "Remover" : "Comparar"}
            </button>
          );
        },
      },
    ],
    [
      getPlayerHref,
      getTeamHref,
      handleCompareAction,
      prefetchPlayerDetail,
      selectedIds.length,
      selectedIdsSet,
    ],
  );

  return (
    <main
      className={`${profileTypographyClassName} ${profileHeadlineVariableClassName} space-y-6 text-[#111c2d]`}
    >
      <section className="relative isolate overflow-hidden rounded-[2rem] bg-[linear-gradient(180deg,#eef5ff_0%,#f9f9ff_44%,#f4faf7_100%)] p-4 shadow-[0_30px_90px_-55px_rgba(17,28,45,0.42)] md:p-6 xl:p-8">
        <div className="pointer-events-none absolute inset-x-0 top-0 h-48 bg-[radial-gradient(circle_at_top_left,rgba(216,227,251,0.95),transparent_52%),radial-gradient(circle_at_top_right,rgba(139,214,182,0.42),transparent_46%)]" />
        <div className="relative grid gap-6 xl:grid-cols-[minmax(0,1.6fr)_minmax(300px,0.95fr)]">
          <section className="overflow-hidden rounded-[1.85rem] bg-[linear-gradient(135deg,#003526_0%,#004e39_100%)] p-6 text-white shadow-[0_34px_90px_-58px_rgba(0,53,38,0.9)]">
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-full bg-white/10 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.18em] text-white/78">
                Catalogo
              </span>
              <span className="rounded-full bg-white/10 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.18em] text-white/78">
                {activeWindowLabel}
              </span>
              <span className="rounded-full bg-white/10 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.18em] text-white/78">
                {localFiltersDescription}
              </span>
            </div>

            <div className="mt-6 max-w-3xl">
              <p className="text-[0.72rem] uppercase tracking-[0.18em] text-white/62">
                Lista de jogadores
              </p>
              <h1 className="mt-3 font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-[-0.04em] md:text-5xl">
                Jogadores
              </h1>
              <p className="mt-3 max-w-2xl text-sm/6 text-white/74">
                {pageTitle}. Encontre atletas, compare desempenho recente e abra cada perfil na
                mesma leitura competitiva da tela.
              </p>
            </div>

            <div className="mt-6 grid gap-3 md:grid-cols-3">
              <ProfileKpi
                hint="Apos filtros globais e locais"
                invert
                label="Jogadores visiveis"
                value={loadingSummaryValue}
              />
              <ProfileKpi
                hint={
                  derivedSummary.ratedPlayers > 0
                    ? `${formatInteger(derivedSummary.ratedPlayers)} com nota`
                    : "Sem nota disponivel"
                }
                invert
                label="Media de rating"
                value={averageRatingValue}
              />
              <ProfileKpi
                hint={`${formatInteger(derivedSummary.totalGoals)} gols e ${formatInteger(derivedSummary.totalAssists)} assist.`}
                invert
                label="Gols + assist."
                value={goalContributionValue}
              />
            </div>
          </section>

          <div className="grid gap-4">
            <section className="rounded-[1.6rem] border border-white/60 bg-[rgba(255,255,255,0.84)] p-5 shadow-[0_22px_60px_-48px_rgba(17,28,45,0.32)] backdrop-blur-xl">
              <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
                Leitura atual
              </p>
              <dl className="mt-4 space-y-3 text-sm text-[#1f2d40]">
                <div className="flex items-start justify-between gap-4">
                  <dt className="text-[#57657a]">Competicao</dt>
                  <dd className="text-right font-medium">{competitionName ?? "Todas"}</dd>
                </div>
                <div className="flex items-start justify-between gap-4">
                  <dt className="text-[#57657a]">Temporada</dt>
                  <dd className="text-right font-medium">{seasonLabel ?? "Todas"}</dd>
                </div>
                <div className="flex items-start justify-between gap-4">
                  <dt className="text-[#57657a]">Mando</dt>
                  <dd className="text-right font-medium">{describeVenue(venue)}</dd>
                </div>
                <div className="flex items-start justify-between gap-4">
                  <dt className="text-[#57657a]">Janela</dt>
                  <dd className="text-right font-medium">{activeWindowLabel}</dd>
                </div>
                <div className="flex items-start justify-between gap-4">
                  <dt className="text-[#57657a]">Navegacao</dt>
                  <dd className="max-w-[16rem] text-right font-medium">
                    {canNavigateDirectly
                      ? "Os links mantêm esta competição e temporada ao abrir jogador e time."
                      : "Quando faltar contexto, o produto abre o melhor caminho disponível."}
                  </dd>
                </div>
              </dl>
            </section>

            <section className="rounded-[1.6rem] border border-[rgba(191,201,195,0.55)] bg-[rgba(240,243,255,0.88)] p-5 shadow-[0_20px_56px_-48px_rgba(17,28,45,0.28)]">
              <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
                Comparacao
              </p>
              <p className="mt-3 text-sm/6 text-[#1f2d40]">{compareStatusCopy}</p>
              {selectedPlayersPreview.length > 0 ? (
                <div className="mt-4 flex flex-wrap gap-2">
                  {selectedPlayersPreview.map((player) => (
                    <ProfileTag key={player.playerId}>{player.label}</ProfileTag>
                  ))}
                </div>
              ) : null}
            </section>
          </div>
        </div>
      </section>

      {playersQuery.isError ? (
        <ProfileAlert
          title={
            tableData.length === 0 ? "Falha ao carregar a lista" : "Lista carregada com alerta"
          }
          tone={tableData.length === 0 ? "critical" : "warning"}
        >
          <p>{playersQuery.error?.message}</p>
        </ProfileAlert>
      ) : null}

      {playersQuery.isPartial ? (
        <PartialDataBanner
          className="rounded-[1.35rem] border-[#ffdcc3] bg-[#fff3e8] px-4 py-3 text-[#6e3900]"
          coverage={playersQuery.coverage}
          message="Algumas metricas ainda estao incompletas neste contexto. Use a lista como referencia, nao como leitura exaustiva."
        />
      ) : null}

      <section className="grid gap-4 xl:grid-cols-4">
        <ProfileRouteCard
          description="Volte para a temporada para cruzar elenco, calendário e destaques no mesmo recorte."
          href={seasonHubHref}
          label="Contexto canônico"
          title="Temporada"
        />
        <ProfileRouteCard
          description="Entre na família de rankings mantendo a mesma competição, temporada e janela ativa."
          href={buildRankingPath("player-goals", {
            competitionId,
            seasonId,
            roundId: timeRangeParams.roundId,
            venue,
            lastN: timeRangeParams.lastN,
            dateRangeStart: timeRangeParams.dateRangeStart,
            dateRangeEnd: timeRangeParams.dateRangeEnd,
          })}
          label="Leitura comparativa"
          title="Rankings"
        />
        <ProfileRouteCard
          description="Cruze atletas com perfis de time e abra os elencos presos ao mesmo contexto."
          href={buildTeamsPath({
            competitionId,
            seasonId,
            roundId: timeRangeParams.roundId,
            venue,
            lastN: timeRangeParams.lastN,
            dateRangeStart: timeRangeParams.dateRangeStart,
            dateRangeEnd: timeRangeParams.dateRangeEnd,
          })}
          label="Saída canônica"
          title="Times"
        />
        <ProfileRouteCard
          description="Use a lista de jogadores como entrada para o calendário completo da temporada."
          href={buildMatchesPath({
            competitionId,
            seasonId,
            roundId: timeRangeParams.roundId,
            venue,
            lastN: timeRangeParams.lastN,
            dateRangeStart: timeRangeParams.dateRangeStart,
            dateRangeEnd: timeRangeParams.dateRangeEnd,
          })}
          label="Saída canônica"
          title="Partidas"
        />
      </section>

      <section className="rounded-[1.75rem] border border-white/60 bg-[rgba(255,255,255,0.84)] p-5 shadow-[0_24px_60px_-48px_rgba(17,28,45,0.32)] backdrop-blur-xl">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
              Busca e refinamento local
            </p>
            <p className="mt-2 max-w-3xl text-sm/6 text-[#57657a]">
              Combine busca por nome e piso de minutos para encontrar o jogador certo nesta
              temporada.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <ProfileTag>{competitionName ?? "Todas as competicoes"}</ProfileTag>
            <ProfileTag>{seasonLabel ?? "Todas as temporadas"}</ProfileTag>
            <ProfileTag>
              {activeMode === "none" ? "Sem janela extra" : activeWindowLabel}
            </ProfileTag>
            <ProfileTag>{localFiltersDescription}</ProfileTag>
          </div>
        </div>

        <div className="mt-5 grid gap-3 lg:grid-cols-[minmax(0,1.4fr)_minmax(220px,0.85fr)]">
          <label className="flex flex-col gap-2 text-sm text-[#1f2d40]">
            Buscar jogador
            <div className="flex items-center gap-3 rounded-[1.3rem] border border-[rgba(191,201,195,0.55)] bg-[#f9f9ff] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
              <span className="flex h-8 w-8 items-center justify-center rounded-full bg-[rgba(216,227,251,0.82)] text-xs font-semibold text-[#003526]">
                Q
              </span>
              <input
                className="w-full border-0 bg-transparent text-sm text-[#111c2d] outline-none placeholder:text-[#707974]"
                onChange={(event) => {
                  setSearch(event.target.value);
                }}
                placeholder="Ex.: Arrascaeta"
                type="text"
                value={search}
              />
            </div>
          </label>

          <label className="flex flex-col gap-2 text-sm text-[#1f2d40]">
            Minimo de minutos
            <div className="flex items-center gap-3 rounded-[1.3rem] border border-[rgba(191,201,195,0.55)] bg-[#f9f9ff] px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)]">
              <span className="flex h-8 w-8 items-center justify-center rounded-full bg-[rgba(216,227,251,0.82)] text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
                Min
              </span>
              <input
                className="w-full border-0 bg-transparent text-sm text-[#111c2d] outline-none placeholder:text-[#707974]"
                min={0}
                onChange={(event) => {
                  setMinMinutesInput(event.target.value);
                }}
                placeholder="Ex.: 300"
                type="number"
                value={minMinutesInput}
              />
            </div>
          </label>
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px]">
        <section className="rounded-[1.75rem] border border-white/60 bg-[rgba(255,255,255,0.84)] p-5 shadow-[0_24px_60px_-48px_rgba(17,28,45,0.32)] backdrop-blur-xl">
          <div className="flex flex-col gap-4 border-b border-[rgba(191,201,195,0.55)] pb-5 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
                Resultados
              </p>
              <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.04em] text-[#111c2d]">
                Descoberta de jogadores
              </h2>
              <p className="mt-2 max-w-2xl text-sm/6 text-[#57657a]">
                {discoverySummary}. Clique em um nome para abrir o perfil do jogador no melhor
                contexto disponivel.
              </p>
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-[1.3rem] bg-[rgba(240,243,255,0.96)] px-4 py-3">
                <p className="text-[0.68rem] uppercase tracking-[0.16em] text-[#57657a]">
                  Registros
                </p>
                <p className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
                  {loadingSummaryValue}
                </p>
              </div>
              <div className="rounded-[1.3rem] bg-[rgba(240,243,255,0.96)] px-4 py-3">
                <p className="text-[0.68rem] uppercase tracking-[0.16em] text-[#57657a]">
                  Minutos somados
                </p>
                <p className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
                  {playersQuery.isLoading ? "..." : formatInteger(derivedSummary.totalMinutes)}
                </p>
              </div>
            </div>
          </div>

          <div className="mt-5">
            <DataTable<PlayerListItem>
              columns={columns}
              data={tableData}
              emptyDescription={resultsEmptyDescription}
              emptyTitle={resultsEmptyTitle}
              initialPageSize={12}
              loading={playersQuery.isLoading}
              pageSizeOptions={[12, 24, 48]}
              variant="profile"
            />
          </div>
        </section>

        <aside className="space-y-4">
          <section className="rounded-[1.75rem] border border-[rgba(191,201,195,0.55)] bg-[rgba(240,243,255,0.88)] p-5 shadow-[0_22px_56px_-48px_rgba(17,28,45,0.28)]">
            <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">Em destaque</p>
            <div className="mt-4 space-y-3">
              {playersQuery.isLoading
                ? Array.from({ length: 3 }, (_, index) => (
                    <div
                      className="h-20 animate-pulse rounded-[1.35rem] bg-white/75"
                      key={`players-list-loading-${index}`}
                    />
                  ))
                : featuredPlayers.map((player) => (
                    <Link
                      className="group flex items-center justify-between rounded-[1.35rem] bg-white/92 p-4 transition-colors hover:bg-white"
                      href={getPlayerHref(player.playerId)}
                      key={player.playerId}
                      onFocus={() => {
                        prefetchPlayerDetail(player.playerId);
                      }}
                      onMouseEnter={() => {
                        prefetchPlayerDetail(player.playerId);
                      }}
                    >
                      <div className="flex min-w-0 items-center gap-3">
                        <ProfileMedia
                          alt={player.playerName}
                          assetId={player.playerId}
                          category="players"
                          className="h-11 w-11 border-0 bg-[rgba(216,227,251,0.82)]"
                          fallback={getInitials(player.playerName)}
                          imageClassName="p-1.5"
                          shape="circle"
                        />
                        <div className="min-w-0">
                          <p className="truncate font-semibold text-[#111c2d] transition-colors group-hover:text-[#003526]">
                            {player.playerName}
                          </p>
                          <p className="mt-1 truncate text-xs text-[#57657a]">
                            {player.teamName ?? "Sem time"} • {formatPosition(player.position)}
                          </p>
                        </div>
                      </div>
                      <div className="pl-3 text-right">
                        <p className="text-sm font-semibold text-[#111c2d]">
                          {formatMetricValue("player_rating", player.rating)}
                        </p>
                        <p className="mt-1 text-[0.65rem] uppercase tracking-[0.16em] text-[#57657a]">
                          rating
                        </p>
                      </div>
                    </Link>
                  ))}

              {!playersQuery.isLoading && featuredPlayers.length === 0 ? (
                <p className="rounded-[1.35rem] bg-white/90 px-4 py-5 text-sm text-[#57657a]">
                  Sem destaques com os filtros atuais.
                </p>
              ) : null}
            </div>
          </section>

          <section className="rounded-[1.75rem] border border-white/60 bg-[rgba(255,255,255,0.84)] p-5 shadow-[0_22px_56px_-48px_rgba(17,28,45,0.28)] backdrop-blur-xl">
            <p className="text-[0.72rem] uppercase tracking-[0.16em] text-[#57657a]">
              Como usar esta lista
            </p>
            <div className="mt-4 space-y-3 text-sm/6 text-[#57657a]">
              <p>
                Abra o jogador para acompanhar historico, forma recente e tendencia mensal sem sair
                da mesma temporada.
              </p>
              <p>
                O link do time leva direto para elenco, resultados e desempenho ligados a cada
                atleta.
              </p>
              <p>
                Use a comparacao para separar dois nomes e seguir para a leitura lado a lado.
              </p>
            </div>
          </section>
        </aside>
      </section>
    </main>
  );
}
