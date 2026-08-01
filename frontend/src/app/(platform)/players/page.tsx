"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import Link from "next/link";
import { useSearchParams } from "next/navigation";

import { useHomePage } from "@/features/home/hooks/useHomePage";
import { usePlayersList } from "@/features/players/hooks";
import type {
  PlayerListItem,
  PlayersSortBy,
  PlayersSortDirection,
} from "@/features/players/types";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { ProfileAlert, ProfilePanel, ProfileShell } from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import { useDebouncedValue } from "@/shared/hooks/useDebouncedValue";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useResolvedCompetitionContext } from "@/shared/hooks/useResolvedCompetitionContext";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import { useComparisonStore } from "@/shared/stores/comparison.store";
import {
  appendFilterQueryString,
  buildCanonicalPlayerPath,
  buildClubsPath,
  buildPlayerResolverPath,
  buildRankingPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });
const DECIMAL_FORMATTER = new Intl.NumberFormat("pt-BR", {
  maximumFractionDigits: 2,
  minimumFractionDigits: 2,
});
const COMPACT_FORMATTER = new Intl.NumberFormat("pt-BR", {
  maximumFractionDigits: 1,
  notation: "compact",
});

const SORT_OPTIONS: Array<{ label: string; value: PlayersSortBy }> = [
  { label: "Relevância", value: "relevance" },
  { label: "Nome", value: "playerName" },
  { label: "Minutos", value: "minutesPlayed" },
  { label: "Gols", value: "goals" },
  { label: "Assistências", value: "assists" },
  { label: "Nota", value: "rating" },
];

const POSITION_LABELS: Record<string, string> = {
  attacker: "Atacante",
  attackingmidfield: "Meia atacante",
  attackingmidfielder: "Meia atacante",
  cam: "Meia atacante",
  cb: "Zagueiro",
  centerback: "Zagueiro",
  centerforward: "Centroavante",
  centreback: "Zagueiro",
  centreforward: "Centroavante",
  cf: "Centroavante",
  cm: "Meia central",
  cdm: "Volante",
  defender: "Defensor",
  defensivemidfield: "Volante",
  defensivemidfielder: "Volante",
  dm: "Volante",
  forward: "Atacante",
  fullback: "Lateral",
  gk: "Goleiro",
  goalkeeper: "Goleiro",
  keeper: "Goleiro",
  lb: "Lateral esquerdo",
  leftback: "Lateral esquerdo",
  leftmidfield: "Meia esquerda",
  leftmidfielder: "Meia esquerda",
  leftwing: "Ponta esquerda",
  leftwingback: "Ala esquerdo",
  leftwinger: "Ponta esquerda",
  lm: "Meia esquerda",
  lw: "Ponta esquerda",
  midfielder: "Meia",
  ram: "Meia direita",
  rb: "Lateral direito",
  rightback: "Lateral direito",
  rightmidfield: "Meia direita",
  rightmidfielder: "Meia direita",
  rightwing: "Ponta direita",
  rightwingback: "Ala direito",
  rightwinger: "Ponta direita",
  rm: "Meia direita",
  rw: "Ponta direita",
  secondstriker: "Segundo atacante",
  st: "Centroavante",
  striker: "Centroavante",
  wingback: "Ala",
  winger: "Ponta",
};

function formatInteger(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? INTEGER_FORMATTER.format(value) : "—";
}

function formatCompact(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? COMPACT_FORMATTER.format(value) : "—";
}

function formatDecimal(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? DECIMAL_FORMATTER.format(value) : "—";
}

function parseMinMinutes(value: string): number | null {
  if (value.trim().length === 0) {
    return null;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

function parseQueryValue(value: string | null): string | null {
  const normalized = value?.trim() ?? "";
  return normalized.length > 0 ? normalized : null;
}

function getInitials(name: string): string {
  const initials = name
    .trim()
    .split(/\s+/)
    .slice(0, 2)
    .map((token) => token[0]?.toUpperCase() ?? "")
    .join("");

  return initials || "JG";
}

function formatPosition(position: string | null | undefined): string {
  if (!position) {
    return "Posição não informada";
  }

  const normalized = position.trim();
  const key = normalized
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z]/g, "")
    .toLowerCase();

  return POSITION_LABELS[key] ?? normalized;
}

function formatYearSpan(start: string | null | undefined, end: string | null | undefined): string {
  const startYear = start?.slice(0, 4);
  const endYear = end?.slice(0, 4);

  if (startYear && endYear) {
    return startYear === endYear ? startYear : `${startYear}–${endYear}`;
  }

  return startYear ?? endYear ?? "Período não informado";
}

function describeTimeWindow(params: {
  roundId: string | null;
  lastN: number | null;
  dateRangeStart: string | null;
  dateRangeEnd: string | null;
}): string {
  if (params.lastN !== null) {
    return `últimas ${params.lastN} partidas`;
  }

  if (params.dateRangeStart || params.dateRangeEnd) {
    return `${params.dateRangeStart ?? "início"} a ${params.dateRangeEnd ?? "hoje"}`;
  }

  if (params.roundId) {
    return `rodada ${params.roundId}`;
  }

  return "todo o período publicado";
}

function activeMetric(player: PlayerListItem, sortBy: PlayersSortBy) {
  if (sortBy === "goals") {
    return { label: "Gols", value: formatInteger(player.goals) };
  }

  if (sortBy === "assists") {
    return { label: "Assistências", value: formatInteger(player.assists) };
  }

  if (sortBy === "minutesPlayed") {
    return { label: "Minutos", value: formatInteger(player.minutesPlayed) };
  }

  if (sortBy === "rating") {
    return { label: "Nota", value: formatDecimal(player.rating) };
  }

  return null;
}

function SearchIcon({ className = "h-5 w-5" }: { className?: string }) {
  return (
    <svg aria-hidden="true" className={className} fill="none" viewBox="0 0 24 24">
      <circle cx="10.8" cy="10.8" r="5.8" stroke="currentColor" strokeWidth="1.8" />
      <path d="m15.4 15.4 4.1 4.1" stroke="currentColor" strokeLinecap="round" strokeWidth="1.8" />
    </svg>
  );
}

function ArrowIcon() {
  return (
    <svg aria-hidden="true" className="h-4 w-4" fill="none" viewBox="0 0 24 24">
      <path d="M5 12h14m-5-5 5 5-5 5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
    </svg>
  );
}

export default function PlayersPage() {
  const searchParams = useSearchParams();
  const selectedStageId = parseQueryValue(searchParams.get("stageId"));
  const selectedStageFormat = parseQueryValue(searchParams.get("stageFormat"));
  const [search, setSearch] = useState("");
  const [minMinutesInput, setMinMinutesInput] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [sortBy, setSortBy] = useState<PlayersSortBy>("relevance");
  const [sortDirection, setSortDirection] = useState<PlayersSortDirection>("desc");
  const debouncedSearch = useDebouncedValue(search);
  const minMinutes = useMemo(() => parseMinMinutes(minMinutesInput), [minMinutesInput]);
  const archiveQuery = useHomePage();
  const { competitionId, seasonId, venue } = useGlobalFiltersState();
  const resolvedContext = useResolvedCompetitionContext();
  const { params: timeRangeParams } = useTimeRange();
  const comparisonEntityType = useComparisonStore((state) => state.entityType);
  const comparisonIds = useComparisonStore((state) => state.selectedIds);
  const addToComparison = useComparisonStore((state) => state.add);
  const removeFromComparison = useComparisonStore((state) => state.remove);
  const setComparisonEntityType = useComparisonStore((state) => state.setEntityType);
  const selectedIds = comparisonEntityType === "player" ? comparisonIds : [];
  const selectedIdsSet = useMemo(() => new Set(selectedIds), [selectedIds]);

  const playersQuery = usePlayersList({
    search: debouncedSearch,
    minMinutes,
    stageId: selectedStageId,
    stageFormat: selectedStageFormat,
    page,
    pageSize,
    sortBy,
    sortDirection,
  });

  useEffect(() => {
    setPage(1);
  }, [
    competitionId,
    debouncedSearch,
    minMinutes,
    pageSize,
    seasonId,
    selectedStageFormat,
    selectedStageId,
    sortBy,
    sortDirection,
    timeRangeParams.dateRangeEnd,
    timeRangeParams.dateRangeStart,
    timeRangeParams.lastN,
    timeRangeParams.roundId,
    venue,
  ]);

  const sharedFilters = useMemo(
    () => ({
      competitionId,
      seasonId,
      roundId: timeRangeParams.roundId,
      stageId: selectedStageId,
      stageFormat: selectedStageFormat,
      venue,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
    }),
    [
      competitionId,
      seasonId,
      selectedStageFormat,
      selectedStageId,
      timeRangeParams.dateRangeEnd,
      timeRangeParams.dateRangeStart,
      timeRangeParams.lastN,
      timeRangeParams.roundId,
      venue,
    ],
  );

  const getPlayerHref = useCallback(
    (playerId: string) =>
      resolvedContext
        ? appendFilterQueryString(
            buildCanonicalPlayerPath(resolvedContext, playerId),
            sharedFilters,
            ["competitionId", "seasonId"],
          )
        : buildPlayerResolverPath(playerId, sharedFilters),
    [resolvedContext, sharedFilters],
  );

  const getTeamHref = useCallback(
    (teamId: string) => buildTeamResolverPath(teamId, sharedFilters),
    [sharedFilters],
  );

  const handleCompare = useCallback(
    (playerId: string) => {
      if (comparisonEntityType !== "player") {
        setComparisonEntityType("player");
      }

      if (selectedIdsSet.has(playerId)) {
        removeFromComparison(playerId);
      } else {
        addToComparison(playerId);
      }
    },
    [
      addToComparison,
      comparisonEntityType,
      removeFromComparison,
      selectedIdsSet,
      setComparisonEntityType,
    ],
  );

  const rows = playersQuery.data?.items ?? [];
  const pagination = playersQuery.meta?.pagination;
  const totalCount = pagination?.totalCount ?? rows.length;
  const currentPage = pagination?.page ?? page;
  const resolvedPageSize = pagination?.pageSize ?? pageSize;
  const totalPages = Math.max(pagination?.totalPages ?? Math.ceil(totalCount / resolvedPageSize), 1);
  const rangeStart = totalCount === 0 ? 0 : (currentPage - 1) * resolvedPageSize + 1;
  const rangeEnd = totalCount === 0 ? 0 : rangeStart + rows.length - 1;
  const archiveSummary = archiveQuery.data?.archiveSummary;
  const archivePlayerCount = archiveSummary?.players ?? totalCount;
  const scopeLabel = playersQuery.data?.scope.label ?? "Acervo publicado";
  const contextLabel = resolvedContext
    ? `${resolvedContext.competitionName} · ${resolvedContext.seasonLabel}`
    : "Todas as competições e temporadas";
  const windowLabel = describeTimeWindow(timeRangeParams);
  const isFiltered = playersQuery.data?.scope.kind === "filtered";
  const rankingsHref = buildRankingPath("player-goals", sharedFilters);
  const clubsHref = buildClubsPath(sharedFilters);

  if (playersQuery.isLoading && !playersQuery.data) {
    return (
      <ProfileShell className="space-y-5" variant="plain">
        <LoadingSkeleton className="motion-reduce:animate-none" height={330} rounded="md" />
        <LoadingSkeleton className="motion-reduce:animate-none" height={118} rounded="md" />
        <div className="grid gap-4 md:grid-cols-2">
          {[0, 1, 2, 3].map((item) => (
            <LoadingSkeleton className="motion-reduce:animate-none" height={210} key={item} rounded="md" />
          ))}
        </div>
      </ProfileShell>
    );
  }

  if (playersQuery.isError && rows.length === 0) {
    return (
      <ProfileShell className="space-y-5" variant="plain">
        <EmptyState
          actionLabel="Tentar novamente"
          description={playersQuery.error?.message ?? "Não foi possível consultar o acervo agora."}
          onAction={() => void playersQuery.refetch()}
          title="Falha ao carregar jogadores"
        />
      </ProfileShell>
    );
  }

  return (
    <ProfileShell className="space-y-6" variant="plain">
      <nav aria-label="Caminho da página" className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#66756e]">
        <Link className="hover:text-[#00513b]" href="/competitions">Competições</Link>
        <span aria-hidden="true">/</span>
        <span aria-current="page">Jogadores</span>
      </nav>

      <section className="overflow-hidden rounded-[1.75rem] border border-[#1c4136] bg-[#08231a] text-white shadow-[0_30px_70px_-50px_rgba(0,31,22,0.72)]">
        <div className="grid lg:grid-cols-[minmax(0,1.25fr)_minmax(20rem,0.75fr)]">
          <div className="p-5 sm:p-7 lg:p-9">
            <p className="text-[0.68rem] font-bold uppercase tracking-[0.24em] text-[#9ddfc2]">
              Acervo de jogadores
            </p>
            <h1 className="mt-4 max-w-3xl font-[family:var(--font-profile-headline)] text-4xl font-extrabold leading-[0.98] tracking-[-0.05em] sm:text-5xl lg:text-6xl">
              Carreiras para explorar, nomes para descobrir.
            </h1>
            <p className="mt-5 max-w-2xl text-sm/6 text-white/72 sm:text-base/7">
              Navegue pelas trajetórias registradas na plataforma, encontre um jogador e siga por clubes, temporadas e partidas.
            </p>

            <dl className="mt-8 grid grid-cols-3 gap-4 border-t border-white/12 pt-5">
              <div>
                <dt className="text-[0.62rem] uppercase tracking-[0.15em] text-white/48">Competições</dt>
                <dd className="mt-1 text-xl font-bold">{formatInteger(archiveSummary?.competitions)}</dd>
              </div>
              <div>
                <dt className="text-[0.62rem] uppercase tracking-[0.15em] text-white/48">Temporadas</dt>
                <dd className="mt-1 text-xl font-bold">{formatInteger(archiveSummary?.seasons)}</dd>
              </div>
              <div>
                <dt className="text-[0.62rem] uppercase tracking-[0.15em] text-white/48">Partidas</dt>
                <dd className="mt-1 text-xl font-bold">{formatCompact(archiveSummary?.matches)}</dd>
              </div>
            </dl>
          </div>

          <div className="border-t border-white/12 bg-white/[0.055] p-5 sm:p-7 lg:border-l lg:border-t-0 lg:p-9">
            <p className="font-[family:var(--font-profile-headline)] text-5xl font-extrabold tracking-[-0.055em] sm:text-6xl">
              {formatInteger(archivePlayerCount)}
            </p>
            <p className="mt-2 text-sm font-semibold text-white/86">
              identidades de jogadores no acervo bruto publicado
            </p>
            <p className="mt-2 text-sm/6 text-white/56">
              {isFiltered
                ? `${formatInteger(totalCount)} ${totalCount === 1 ? "carreira documentada atende" : "carreiras documentadas atendem"} aos filtros atuais.`
                : `${formatInteger(totalCount)} ${totalCount === 1 ? "carreira documentada disponível" : "carreiras documentadas disponíveis"} para explorar.`}{" "}
              São camadas de cobertura diferentes; nenhuma representa toda a história do futebol.
            </p>

            <label className="mt-7 block" htmlFor="player-search">
              <span className="text-[0.68rem] font-bold uppercase tracking-[0.17em] text-white/62">
                Buscar por nome
              </span>
              <span className="mt-2 flex min-h-14 items-center gap-3 rounded-[1rem] bg-white px-4 text-[#111c2d] shadow-[0_14px_35px_-24px_rgba(0,0,0,0.55)]">
                <SearchIcon className="h-5 w-5 shrink-0 text-[#35624f]" />
                <input
                  className="min-w-0 flex-1 border-0 bg-transparent py-3 text-base font-medium outline-none placeholder:text-[#7c8882]"
                  id="player-search"
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder="Digite um jogador"
                  type="search"
                  value={search}
                />
              </span>
            </label>
          </div>
        </div>
      </section>

      <ProfilePanel className="border-[#dfe7e2] bg-white/90 p-4 md:p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <p className="text-[0.66rem] font-bold uppercase tracking-[0.18em] text-[#65736d]">{scopeLabel}</p>
            <h2 className="mt-1 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.035em] text-[#111c2d]">
              Explorar o catálogo
            </h2>
            <p className="mt-1 text-sm text-[#66756e]">{contextLabel} · {windowLabel}</p>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <label className="text-xs font-semibold text-[#586861]">
              Ordem
              <select
                className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                onChange={(event) => {
                  const nextSort = event.target.value as PlayersSortBy;
                  setSortBy(nextSort);
                  if (nextSort === "relevance") setSortDirection("desc");
                }}
                value={sortBy}
              >
                {SORT_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>

            {sortBy !== "relevance" ? (
              <label className="text-xs font-semibold text-[#586861]">
                Direção
                <select
                  className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                  onChange={(event) => setSortDirection(event.target.value as PlayersSortDirection)}
                  value={sortDirection}
                >
                  <option value="desc">Maior para menor</option>
                  <option value="asc">Menor para maior</option>
                </select>
              </label>
            ) : null}

            <label className="text-xs font-semibold text-[#586861]">
              Mínimo de minutos
              <input
                className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                min={0}
                onChange={(event) => setMinMinutesInput(event.target.value)}
                placeholder="Sem mínimo"
                type="number"
                value={minMinutesInput}
              />
            </label>

            <label className="text-xs font-semibold text-[#586861]">
              Por página
              <select
                className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                onChange={(event) => setPageSize(Number(event.target.value))}
                value={pageSize}
              >
                {[20, 40, 60].map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
            </label>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-[#e4ebe7] pt-4 text-sm">
          <Link className="font-semibold text-[#00513b] hover:underline" href={clubsHref}>Explorar clubes</Link>
          <Link className="font-semibold text-[#52635b] hover:text-[#00513b]" href={rankingsHref}>Abrir ranking de gols</Link>
          <span className="min-h-5 text-[#65736d]" role="status">
            {playersQuery.isFetching && playersQuery.data ? "Atualizando o acervo…" : ""}
          </span>
        </div>
      </ProfilePanel>

      {playersQuery.isError ? (
        <ProfileAlert title="O catálogo pode estar desatualizado" tone="warning">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p>{playersQuery.error?.message}</p>
            <button className="button-pill button-pill-secondary" onClick={() => void playersQuery.refetch()} type="button">Tentar novamente</button>
          </div>
        </ProfileAlert>
      ) : null}

      {playersQuery.isPartial ? (
        <p className="rounded-xl border border-[#dce5e0] bg-white/72 px-4 py-3 text-sm text-[#5d6d66]">
          Parte dos registros possui cobertura incompleta; os campos disponíveis continuam navegáveis.
        </p>
      ) : null}

      <section aria-labelledby="players-results-title" className="space-y-4">
        <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-[0.67rem] font-bold uppercase tracking-[0.18em] text-[#687870]">
              {isFiltered ? "Resultado do recorte" : "Carreiras documentadas"}
            </p>
            <h2 className="mt-1 font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.04em] text-[#111c2d]" id="players-results-title">
              {formatInteger(totalCount)} {totalCount === 1 ? "jogador" : "jogadores"}
            </h2>
          </div>
          <p className="text-sm text-[#64736d]">Exibindo {formatInteger(rangeStart)}–{formatInteger(rangeEnd)}</p>
        </header>

        {rows.length === 0 ? (
          <EmptyState
            actionLabel={search || minMinutesInput ? "Limpar busca e filtros" : undefined}
            description={search ? `Nenhum jogador encontrado para “${search}” neste recorte.` : "Não há jogadores para os filtros atuais."}
            onAction={search || minMinutesInput ? () => { setSearch(""); setMinMinutesInput(""); } : undefined}
            title="Nenhuma carreira encontrada"
          />
        ) : (
          <div className="grid gap-4 md:grid-cols-2">
            {rows.map((player) => {
              const metric = activeMetric(player, sortBy);
              const recentTeams = player.recentTeams?.slice(0, 3) ?? [];
              const isSelected = selectedIdsSet.has(player.playerId);
              const isCompareDisabled = !isSelected && selectedIds.length >= 2;

              return (
                <article className="flex min-w-0 flex-col rounded-[1.4rem] border border-[#dce5e0] bg-white/92 p-4 shadow-[0_22px_55px_-48px_rgba(17,39,30,0.4)] transition-colors hover:border-[#a9cbbb] sm:p-5" key={player.playerId}>
                  <div className="flex min-w-0 items-start gap-3.5">
                    <ProfileMedia
                      alt={player.playerName}
                      assetId={player.playerId}
                      category="players"
                      className="h-14 w-14 border-[#dce6e1] bg-[#edf3f0] sm:h-16 sm:w-16"
                      fallback={getInitials(player.playerName)}
                      linkBehavior="none"
                      shape="circle"
                    />
                    <div className="min-w-0 flex-1">
                      <Link className="break-words font-[family:var(--font-profile-headline)] text-xl font-extrabold tracking-[-0.025em] text-[#15231e] hover:text-[#00513b]" href={getPlayerHref(player.playerId)}>
                        {player.playerName}
                      </Link>
                      <p className="mt-1 text-sm text-[#66756e]">{formatPosition(player.position)}</p>
                    </div>
                    {metric ? (
                      <div className="shrink-0 text-right">
                        <p className="text-lg font-extrabold tabular-nums text-[#00513b]">{metric.value}</p>
                        <p className="text-[0.6rem] uppercase tracking-[0.13em] text-[#718079]">{metric.label}</p>
                      </div>
                    ) : null}
                  </div>

                  <div className="mt-5 min-h-[3rem] border-y border-[#e5ebe8] py-3">
                    <div className="flex items-center gap-3">
                      {recentTeams.length > 0 ? (
                        <div className="flex shrink-0 items-center pl-1">
                          {recentTeams.map((team, index) => (
                            <Link
                              aria-label={`Abrir ${team.teamName ?? "equipe"}`}
                              className={`relative inline-flex rounded-full focus-visible:z-10 ${index > 0 ? "-ml-2" : ""}`}
                              href={getTeamHref(team.teamId)}
                              key={team.teamId}
                            >
                              <ProfileMedia
                                alt={team.teamName ?? "Equipe"}
                                assetId={team.teamId}
                                category="clubs"
                                className="h-9 w-9 border-2 border-white bg-[#f2f5f3]"
                                fallback={getInitials(team.teamName ?? "Equipe")}
                                fallbackClassName="text-[0.55rem]"
                                linkBehavior="none"
                                shape="circle"
                              />
                            </Link>
                          ))}
                        </div>
                      ) : null}
                      <div className="min-w-0">
                        <p className="text-[0.62rem] font-bold uppercase tracking-[0.14em] text-[#718079]">Passagens recentes</p>
                        <p className="mt-0.5 truncate text-sm font-semibold text-[#33443d]">
                          {player.teamContextLabel ?? player.teamName ?? "Equipes ainda não documentadas"}
                        </p>
                      </div>
                    </div>
                  </div>

                  <dl className="mt-4 grid grid-cols-3 gap-3 text-sm">
                    <div>
                      <dt className="text-[0.62rem] uppercase tracking-[0.12em] text-[#718079]">Temporadas</dt>
                      <dd className="mt-1 font-bold tabular-nums text-[#24332d]">{formatInteger(player.seasonCount)}</dd>
                    </div>
                    <div>
                      <dt className="text-[0.62rem] uppercase tracking-[0.12em] text-[#718079]">Competições</dt>
                      <dd className="mt-1 font-bold tabular-nums text-[#24332d]">{formatInteger(player.competitionCount)}</dd>
                    </div>
                    <div>
                      <dt className="text-[0.62rem] uppercase tracking-[0.12em] text-[#718079]">Partidas</dt>
                      <dd className="mt-1 font-bold tabular-nums text-[#24332d]">{formatInteger(player.matchesPlayed)}</dd>
                    </div>
                  </dl>

                  <div className="mt-auto flex flex-wrap items-center justify-between gap-3 pt-5">
                    <p className="text-sm font-medium text-[#5f7068]">{formatYearSpan(player.careerStartAt, player.careerEndAt)}</p>
                    <div className="flex items-center gap-2">
                      <button
                        aria-pressed={isSelected}
                        className={`button-pill ${isSelected ? "button-pill-primary" : "button-pill-ghost"}`}
                        disabled={isCompareDisabled}
                        onClick={() => handleCompare(player.playerId)}
                        type="button"
                      >
                        {isSelected ? "Selecionado" : "Comparar"}
                      </button>
                      <Link aria-label={`Abrir perfil de ${player.playerName}`} className="button-pill button-pill-soft px-4" href={getPlayerHref(player.playerId)}>
                        <span className="sr-only">Abrir perfil</span><ArrowIcon />
                      </Link>
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        )}
      </section>

      {rows.length > 0 ? (
        <ProfilePanel className="flex flex-col gap-3 border-[#dfe7e2] bg-white/88 p-4 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-sm text-[#617169]">Página {formatInteger(currentPage)} de {formatInteger(totalPages)}</p>
          <div className="grid grid-cols-2 gap-2 sm:flex">
            <button className="button-pill button-pill-secondary" disabled={playersQuery.isFetching || currentPage <= 1} onClick={() => setPage((value) => Math.max(value - 1, 1))} type="button">Anterior</button>
            <button className="button-pill button-pill-primary" disabled={playersQuery.isFetching || currentPage >= totalPages} onClick={() => setPage((value) => Math.min(value + 1, totalPages))} type="button">Próxima</button>
          </div>
        </ProfilePanel>
      ) : null}
    </ProfileShell>
  );
}
