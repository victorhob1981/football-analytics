"use client";

import { useEffect, useMemo, useState } from "react";

import Link from "next/link";
import { useSearchParams } from "next/navigation";

import { useHomePage } from "@/features/home/hooks/useHomePage";
import { useTeamsList } from "@/features/teams/hooks/useTeamsList";
import type {
  TeamListItem,
  TeamType,
  TeamsListSortBy,
  TeamsListSortDirection,
} from "@/features/teams/types";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { ProfileAlert, ProfilePanel, ProfileShell } from "@/shared/components/profile/ProfilePrimitives";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import { useDebouncedValue } from "@/shared/hooks/useDebouncedValue";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useResolvedCompetitionContext } from "@/shared/hooks/useResolvedCompetitionContext";
import {
  buildCanonicalClubPath,
  buildCanonicalTeamPath,
  buildClubResolverPath,
  buildFilterQueryString,
  buildHeadToHeadPath,
  buildPlayersPath,
  buildRankingPath,
  buildTeamResolverPath,
  resolveCompetitionSeasonContextFromSearchParams,
} from "@/shared/utils/context-routing";

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });
const COMPACT_FORMATTER = new Intl.NumberFormat("pt-BR", {
  maximumFractionDigits: 1,
  notation: "compact",
});

const BASE_SORT_OPTIONS: Array<{ label: string; value: TeamsListSortBy }> = [
  { label: "Relevância", value: "relevance" },
  { label: "Nome", value: "teamName" },
  { label: "Pontos", value: "points" },
  { label: "Vitórias", value: "wins" },
  { label: "Saldo", value: "goalDiff" },
];

function formatInteger(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? INTEGER_FORMATTER.format(value) : "—";
}

function formatCompact(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? COMPACT_FORMATTER.format(value) : "—";
}

function formatGoalDiff(value: number | null | undefined): string {
  if (typeof value !== "number") return "—";
  return value > 0 ? `+${formatInteger(value)}` : formatInteger(value);
}

function formatYearSpan(start: string | null | undefined, end: string | null | undefined): string {
  const startYear = start?.slice(0, 4);
  const endYear = end?.slice(0, 4);

  if (startYear && endYear) return startYear === endYear ? startYear : `${startYear}–${endYear}`;
  return startYear ?? endYear ?? "Período não informado";
}

function getMonogram(name: string): string {
  return name
    .trim()
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() ?? "")
    .join("") || "CL";
}

function describeVenue(venue: string | null | undefined): string {
  if (venue === "home") return "jogos como mandante";
  if (venue === "away") return "jogos como visitante";
  return "todos os mandos";
}

function describeTimeWindow(params: {
  roundId: string | null;
  lastN: number | null;
  dateRangeStart: string | null;
  dateRangeEnd: string | null;
}): string {
  if (params.lastN) return `últimas ${params.lastN} partidas`;
  if (params.dateRangeStart || params.dateRangeEnd) return `${params.dateRangeStart ?? "início"} a ${params.dateRangeEnd ?? "hoje"}`;
  if (params.roundId) return `rodada ${params.roundId}`;
  return "todo o período publicado";
}

function activeMetric(team: TeamListItem, sortBy: TeamsListSortBy) {
  if (sortBy === "points") return { label: "Pontos", value: formatInteger(team.points) };
  if (sortBy === "wins") return { label: "Vitórias", value: formatInteger(team.wins) };
  if (sortBy === "goalDiff") return { label: "Saldo", value: formatGoalDiff(team.goalDiff) };
  if (sortBy === "position") return { label: "Posição", value: team.position ? `${team.position}º` : "—" };
  return null;
}

function SearchIcon() {
  return (
    <svg aria-hidden="true" className="h-5 w-5" fill="none" viewBox="0 0 24 24">
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

export function TeamsPageContent({ entityType }: { entityType?: TeamType | null } = {}) {
  const isClubCatalog = entityType === "club";
  const entityLabel = isClubCatalog ? "Clubes" : "Equipes";
  const entityPlural = isClubCatalog ? "clubes" : "equipes";
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(24);
  const [sortBy, setSortBy] = useState<TeamsListSortBy>("relevance");
  const [sortDirection, setSortDirection] = useState<TeamsListSortDirection>("desc");
  const debouncedSearch = useDebouncedValue(search);
  const searchParams = useSearchParams();
  const globalContext = useResolvedCompetitionContext();
  const resolvedContext = useMemo(
    () => globalContext ?? resolveCompetitionSeasonContextFromSearchParams(searchParams),
    [globalContext, searchParams],
  );
  const { competitionId, seasonId, roundId, venue, lastN, dateRangeStart, dateRangeEnd } =
    useGlobalFiltersState();
  const archiveQuery = useHomePage();
  const teamsQuery = useTeamsList(
    {
      page,
      pageSize,
      search: debouncedSearch,
      entityType,
      sortBy,
      sortDirection,
    },
    resolvedContext,
  );
  const sharedFilters = useMemo(
    () => ({ competitionId, seasonId, roundId, venue, lastN, dateRangeStart, dateRangeEnd }),
    [competitionId, dateRangeEnd, dateRangeStart, lastN, roundId, seasonId, venue],
  );
  const canonicalExtraQuery = useMemo(
    () => buildFilterQueryString(sharedFilters, ["competitionId", "seasonId"]),
    [sharedFilters],
  );
  const sortOptions = useMemo(
    () => resolvedContext ? [...BASE_SORT_OPTIONS, { label: "Posição", value: "position" as const }] : BASE_SORT_OPTIONS,
    [resolvedContext],
  );

  useEffect(() => {
    setPage(1);
  }, [
    dateRangeEnd,
    dateRangeStart,
    debouncedSearch,
    lastN,
    pageSize,
    resolvedContext?.competitionId,
    resolvedContext?.seasonId,
    roundId,
    sortBy,
    sortDirection,
    venue,
  ]);

  const buildProfileHref = (teamId: string) =>
    resolvedContext
      ? `${isClubCatalog ? buildCanonicalClubPath(resolvedContext, teamId) : buildCanonicalTeamPath(resolvedContext, teamId)}${canonicalExtraQuery}`
      : isClubCatalog
        ? buildClubResolverPath(teamId, sharedFilters)
        : buildTeamResolverPath(teamId, sharedFilters);

  const items = teamsQuery.data?.items ?? [];
  const pagination = teamsQuery.meta?.pagination;
  const totalCount = pagination?.totalCount ?? items.length;
  const currentPage = pagination?.page ?? page;
  const resolvedPageSize = pagination?.pageSize ?? pageSize;
  const totalPages = Math.max(pagination?.totalPages ?? Math.ceil(totalCount / resolvedPageSize), 1);
  const rangeStart = totalCount === 0 ? 0 : (currentPage - 1) * resolvedPageSize + 1;
  const rangeEnd = totalCount === 0 ? 0 : rangeStart + items.length - 1;
  const archiveSummary = archiveQuery.data?.archiveSummary;
  const archiveEntityCount = isClubCatalog ? archiveSummary?.clubs ?? totalCount : totalCount;
  const scopeLabel = teamsQuery.data?.scope.label ?? "Acervo publicado";
  const contextLabel = resolvedContext
    ? `${resolvedContext.competitionName} · ${resolvedContext.seasonLabel}`
    : "Todas as competições e temporadas";
  const isFiltered = teamsQuery.data?.scope.kind === "filtered";
  const playersHref = buildPlayersPath(sharedFilters);
  const rankingsHref = buildRankingPath("team-possession", sharedFilters);

  if (teamsQuery.isLoading && !teamsQuery.data) {
    return (
      <ProfileShell className="space-y-5" variant="plain">
        <LoadingSkeleton className="motion-reduce:animate-none" height={330} rounded="md" />
        <LoadingSkeleton className="motion-reduce:animate-none" height={118} rounded="md" />
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {[0, 1, 2, 3, 4, 5].map((item) => (
            <LoadingSkeleton className="motion-reduce:animate-none" height={210} key={item} rounded="md" />
          ))}
        </div>
      </ProfileShell>
    );
  }

  if (teamsQuery.isError && items.length === 0) {
    return (
      <ProfileShell variant="plain">
        <EmptyState
          actionLabel="Tentar novamente"
          description={teamsQuery.error?.message ?? `Não foi possível consultar ${entityPlural} agora.`}
          onAction={() => void teamsQuery.refetch()}
          title={`Falha ao carregar ${entityPlural}`}
        />
      </ProfileShell>
    );
  }

  return (
    <ProfileShell className="space-y-6" variant="plain">
      <nav aria-label="Caminho da página" className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#66756e]">
        <Link className="hover:text-[#00513b]" href="/competitions">Competições</Link>
        <span aria-hidden="true">/</span>
        <span aria-current="page">{entityLabel}</span>
      </nav>

      <section className="overflow-hidden rounded-[1.75rem] border border-[#1c4136] bg-[#08231a] text-white shadow-[0_30px_70px_-50px_rgba(0,31,22,0.72)]">
        <div className="grid lg:grid-cols-[minmax(0,1.25fr)_minmax(20rem,0.75fr)]">
          <div className="p-5 sm:p-7 lg:p-9">
            <p className="text-[0.68rem] font-bold uppercase tracking-[0.24em] text-[#9ddfc2]">Acervo de {entityPlural}</p>
            <h1 className="mt-4 max-w-3xl font-[family:var(--font-profile-headline)] text-4xl font-extrabold leading-[0.98] tracking-[-0.05em] sm:text-5xl lg:text-6xl">
              Identidades e trajetórias reunidas em um só arquivo.
            </h1>
            <p className="mt-5 max-w-2xl text-sm/6 text-white/72 sm:text-base/7">
              Encontre um clube e percorra as competições, temporadas, partidas e conquistas documentadas pela plataforma.
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
              {formatInteger(archiveEntityCount)}
            </p>
            <p className="mt-2 text-sm font-semibold text-white/86">{entityPlural} no acervo publicado</p>
            <p className="mt-2 text-sm/6 text-white/56">
              {formatInteger(totalCount)} {totalCount === 1 ? "resultado" : "resultados"} neste recorte. A cobertura publicada não representa tudo que existe no futebol.
            </p>

            <label className="mt-7 block" htmlFor="club-search">
              <span className="text-[0.68rem] font-bold uppercase tracking-[0.17em] text-white/62">Buscar clube</span>
              <span className="mt-2 flex min-h-14 items-center gap-3 rounded-[1rem] bg-white px-4 text-[#111c2d] shadow-[0_14px_35px_-24px_rgba(0,0,0,0.55)]">
                <span className="shrink-0 text-[#35624f]"><SearchIcon /></span>
                <input
                  className="min-w-0 flex-1 border-0 bg-transparent py-3 text-base font-medium outline-none placeholder:text-[#7c8882]"
                  id="club-search"
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder="Digite um clube"
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
            <h2 className="mt-1 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.035em] text-[#111c2d]">Explorar o catálogo</h2>
            <p className="mt-1 text-sm text-[#66756e]">{contextLabel} · {describeVenue(venue)} · {describeTimeWindow({ roundId, lastN, dateRangeStart, dateRangeEnd })}</p>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            <label className="text-xs font-semibold text-[#586861]">
              Ordem
              <select
                className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                onChange={(event) => {
                  const nextSort = event.target.value as TeamsListSortBy;
                  setSortBy(nextSort);
                  if (nextSort === "relevance") setSortDirection("desc");
                }}
                value={sortBy}
              >
                {sortOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>

            {sortBy !== "relevance" ? (
              <label className="text-xs font-semibold text-[#586861]">
                Direção
                <select
                  className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                  onChange={(event) => setSortDirection(event.target.value as TeamsListSortDirection)}
                  value={sortDirection}
                >
                  <option value="desc">Maior para menor</option>
                  <option value="asc">Menor para maior</option>
                </select>
              </label>
            ) : null}

            <label className="text-xs font-semibold text-[#586861]">
              Por página
              <select
                className="mt-1 min-h-11 w-full rounded-xl border border-[#ced9d3] bg-white px-3 text-base text-[#17231f] sm:text-sm"
                onChange={(event) => setPageSize(Number(event.target.value))}
                value={pageSize}
              >
                {[12, 24, 48].map((option) => <option key={option} value={option}>{option}</option>)}
              </select>
            </label>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-[#e4ebe7] pt-4 text-sm">
          <Link className="font-semibold text-[#00513b] hover:underline" href={playersHref}>Explorar jogadores</Link>
          <Link className="font-semibold text-[#52635b] hover:text-[#00513b]" href={rankingsHref}>Abrir rankings de clubes</Link>
          <span className="min-h-5 text-[#65736d]" role="status">{teamsQuery.isFetching && teamsQuery.data ? `Atualizando ${entityPlural}…` : ""}</span>
        </div>
      </ProfilePanel>

      {teamsQuery.isError ? (
        <ProfileAlert title="O catálogo pode estar desatualizado" tone="warning">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p>{teamsQuery.error?.message}</p>
            <button className="button-pill button-pill-secondary" onClick={() => void teamsQuery.refetch()} type="button">Tentar novamente</button>
          </div>
        </ProfileAlert>
      ) : null}

      {teamsQuery.isPartial ? (
        <p className="rounded-xl border border-[#dce5e0] bg-white/72 px-4 py-3 text-sm text-[#5d6d66]">
          Alguns clubes possuem identidade ou trajetória parcial; apenas os dados documentados são exibidos.
        </p>
      ) : null}

      <section aria-labelledby="clubs-results-title" className="space-y-4">
        <header className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-[0.67rem] font-bold uppercase tracking-[0.18em] text-[#687870]">{isFiltered ? "Resultado do recorte" : "Trajetórias documentadas"}</p>
            <h2 className="mt-1 font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.04em] text-[#111c2d]" id="clubs-results-title">
              {formatInteger(totalCount)} {totalCount === 1 ? "clube" : "clubes"}
            </h2>
          </div>
          <p className="text-sm text-[#64736d]">Exibindo {formatInteger(rangeStart)}–{formatInteger(rangeEnd)}</p>
        </header>

        {items.length === 0 ? (
          <EmptyState
            actionLabel={search ? "Limpar busca" : undefined}
            description={search ? `Nenhum clube encontrado para “${search}” neste recorte.` : "Não há clubes para os filtros atuais."}
            onAction={search ? () => setSearch("") : undefined}
            title="Nenhuma trajetória encontrada"
          />
        ) : (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {items.map((team) => {
              const metric = activeMetric(team, sortBy);
              const profileHref = buildProfileHref(team.teamId);
              const compareHref = buildHeadToHeadPath({ ...sharedFilters, teamA: team.teamId });
              const origin = [team.countryOrTerritory, team.stadiumName].filter(Boolean).join(" · ");

              return (
                <article className="flex min-w-0 flex-col rounded-[1.4rem] border border-[#dce5e0] bg-white/92 p-4 shadow-[0_22px_55px_-48px_rgba(17,39,30,0.4)] transition-colors hover:border-[#a9cbbb] sm:p-5" key={team.teamId}>
                  <div className="flex min-w-0 items-start gap-3.5">
                    <ProfileMedia
                      alt={`Escudo de ${team.teamName}`}
                      assetId={team.visualAssetId ?? team.teamId}
                      category="clubs"
                      className="h-16 w-16 border-[#dce6e1] bg-[#f3f6f4]"
                      fallback={getMonogram(team.teamName)}
                      imageClassName="p-2"
                      linkBehavior="none"
                    />
                    <div className="min-w-0 flex-1">
                      <Link className="break-words font-[family:var(--font-profile-headline)] text-xl font-extrabold tracking-[-0.025em] text-[#15231e] hover:text-[#00513b]" href={profileHref}>{team.teamName}</Link>
                      <p className="mt-1 text-sm text-[#66756e]">{origin || "Origem ainda não documentada"}</p>
                    </div>
                    {metric ? (
                      <div className="shrink-0 text-right">
                        <p className="text-lg font-extrabold tabular-nums text-[#00513b]">{metric.value}</p>
                        <p className="text-[0.6rem] uppercase tracking-[0.13em] text-[#718079]">{metric.label}</p>
                      </div>
                    ) : null}
                  </div>

                  <p className="mt-5 border-y border-[#e5ebe8] py-3 text-sm leading-6 text-[#4d5f57]">
                    Presença documentada em {formatInteger(team.seasonCount)} temporadas e {formatInteger(team.competitionCount)} {team.competitionCount === 1 ? "competição" : "competições"}.
                  </p>

                  <dl className="mt-4 grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <dt className="text-[0.62rem] uppercase tracking-[0.12em] text-[#718079]">Período</dt>
                      <dd className="mt-1 font-bold text-[#24332d]">{formatYearSpan(team.firstMatchAt, team.lastMatchAt)}</dd>
                    </div>
                    <div>
                      <dt className="text-[0.62rem] uppercase tracking-[0.12em] text-[#718079]">Partidas</dt>
                      <dd className="mt-1 font-bold tabular-nums text-[#24332d]">{formatInteger(team.matchesPlayed)}</dd>
                    </div>
                  </dl>

                  <div className="mt-auto flex flex-wrap items-center justify-between gap-3 pt-5">
                    <Link className="text-sm font-semibold text-[#52635b] hover:text-[#00513b] hover:underline" href={compareHref}>Comparar</Link>
                    <Link aria-label={`Abrir perfil de ${team.teamName}`} className="button-pill button-pill-soft px-4" href={profileHref}>
                      <span className="sr-only">Abrir perfil</span><ArrowIcon />
                    </Link>
                  </div>
                </article>
              );
            })}
          </div>
        )}
      </section>

      {items.length > 0 ? (
        <ProfilePanel className="flex flex-col gap-3 border-[#dfe7e2] bg-white/88 p-4 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-sm text-[#617169]">Página {formatInteger(currentPage)} de {formatInteger(totalPages)}</p>
          <div className="grid grid-cols-2 gap-2 sm:flex">
            <button className="button-pill button-pill-secondary" disabled={teamsQuery.isFetching || currentPage <= 1} onClick={() => setPage((value) => Math.max(value - 1, 1))} type="button">Anterior</button>
            <button className="button-pill button-pill-primary" disabled={teamsQuery.isFetching || currentPage >= totalPages} onClick={() => setPage((value) => Math.min(value + 1, totalPages))} type="button">Próxima</button>
          </div>
        </ProfilePanel>
      ) : null}
    </ProfileShell>
  );
}
