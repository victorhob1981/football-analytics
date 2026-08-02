"use client";

import Link from "next/link";

import { TeamHonorsSection } from "@/features/teams/components/TeamHonorsSection";
import { useTeamContexts } from "@/features/teams/hooks/useTeamContexts";
import { useTeamProfile } from "@/features/teams/hooks/useTeamProfile";
import type { TeamHonorsPreview } from "@/features/teams/types";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import {
  ProfileAlert,
  ProfileCoveragePill,
  ProfilePanel,
  ProfileShell,
} from "@/shared/components/profile/ProfilePrimitives";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import {
  buildCanonicalClubPath,
  buildClubResolverPath,
  buildClubsPath,
  buildHeadToHeadPath,
} from "@/shared/utils/context-routing";
import { formatDate } from "@/shared/utils/formatters";

type TeamAggregateProfileContentProps = {
  teamId: string;
  honorsPreview?: TeamHonorsPreview | null;
};

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });

function formatInteger(value: number | null | undefined): string {
  return typeof value === "number" && !Number.isNaN(value) ? INTEGER_FORMATTER.format(value) : "—";
}

function getMonogram(name: string): string {
  const initials = name
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part[0]?.toUpperCase() ?? "")
    .join("")
    .slice(0, 3);
  return initials || "CLB";
}

function getArchiveSpan(firstMatchAt?: string | null, lastMatchAt?: string | null): string {
  if (firstMatchAt && lastMatchAt) {
    return `${formatDate(firstMatchAt)} — ${formatDate(lastMatchAt)}`;
  }

  return firstMatchAt
    ? `Desde ${formatDate(firstMatchAt)}`
    : lastMatchAt
      ? `Até ${formatDate(lastMatchAt)}`
      : "Período ainda não informado";
}

function groupContexts(contexts: CompetitionSeasonContext[]) {
  return Array.from(
    contexts.reduce((groups, context) => {
      const key = `${context.competitionId}-${context.competitionKey}`;
      const current = groups.get(key) ?? {
        competitionId: context.competitionId,
        competitionKey: context.competitionKey,
        competitionName: context.competitionName,
        contexts: [] as CompetitionSeasonContext[],
      };
      current.contexts.push(context);
      groups.set(key, current);
      return groups;
    }, new Map<string, { competitionId: string; competitionKey: string; competitionName: string; contexts: CompetitionSeasonContext[] }>()),
  ).map(([, group]) => group);
}

export function TeamAggregateProfileContent({ teamId }: TeamAggregateProfileContentProps) {
  const { competitionId, seasonId, roundId, venue, lastN, dateRangeStart, dateRangeEnd } =
    useGlobalFiltersState();
  const contextsQuery = useTeamContexts(teamId);
  const defaultContext = contextsQuery.data?.defaultContext ?? null;
  const profileQuery = useTeamProfile(
    teamId,
    { includeRecentMatches: false, includeSquad: false, includeStats: false },
    defaultContext,
  );
  const sharedFilters = {
    competitionId,
    seasonId,
    roundId,
    venue,
    lastN,
    dateRangeStart,
    dateRangeEnd,
  };
  const clubsHref = buildClubsPath(sharedFilters);
  const headToHeadHref = buildHeadToHeadPath({ ...sharedFilters, teamA: teamId });

  if (contextsQuery.isLoading || (defaultContext && profileQuery.isLoading)) {
    return (
      <ProfileShell className="space-y-6" aria-busy="true">
        <span className="sr-only" role="status">Carregando perfil do clube</span>
        <LoadingSkeleton height={240} />
        <LoadingSkeleton height={300} />
        <LoadingSkeleton height={220} />
      </ProfileShell>
    );
  }

  if ((contextsQuery.isError && !contextsQuery.data) || (profileQuery.isError && !profileQuery.data)) {
    const error = profileQuery.error ?? contextsQuery.error;
    const isNotFound = error?.status === 404;
    return (
      <ProfileShell className="space-y-6">
        <ProfileAlert title={isNotFound ? "Clube não encontrado" : "Não foi possível carregar o clube"} tone="critical">
          <p>{isNotFound ? "Este clube não está disponível no acervo publicado." : error?.message}</p>
          {!isNotFound ? (
            <button
              className="button-pill button-pill-secondary mt-3"
              onClick={() => {
                void contextsQuery.refetch();
                void profileQuery.refetch();
              }}
              type="button"
            >
              Tentar novamente
            </button>
          ) : null}
        </ProfileAlert>
        <Link className="button-pill button-pill-primary w-fit" href={clubsHref}>Voltar para clubes</Link>
      </ProfileShell>
    );
  }

  if (!defaultContext || contextsQuery.isEmpty || !profileQuery.data) {
    return (
      <ProfileShell className="space-y-6">
        <EmptyState
          title="Perfil de clube indisponível"
          description="O clube foi identificado, mas ainda não possui uma participação publicada que permita montar o perfil."
        />
        <Link className="button-pill button-pill-primary w-fit" href={clubsHref}>Voltar para clubes</Link>
      </ProfileShell>
    );
  }

  const { archive, honors, identity, team } = profileQuery.data;
  const availableContexts = contextsQuery.data?.availableContexts ?? [];
  const contextGroups = groupContexts(availableContexts);
  const identityFacts = [
    identity.city && identity.countryOrTerritory
      ? `${identity.city}, ${identity.countryOrTerritory}`
      : identity.city ?? identity.countryOrTerritory,
    identity.foundedYear ? `Fundado em ${identity.foundedYear}` : null,
    identity.stadiumName ? `Estádio: ${identity.stadiumName}` : null,
  ].filter((value): value is string => Boolean(value));
  const profileIsPartial =
    profileQuery.isPartial ||
    contextsQuery.isPartial ||
    profileQuery.data.sectionCoverage?.archive?.status === "partial" ||
    profileQuery.data.sectionCoverage?.identity?.status === "partial";

  return (
    <ProfileShell className="space-y-6">
      <nav aria-label="Navegação estrutural" className="flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#57657a]">
        <Link className="hover:text-[#00513b]" href={clubsHref}>Clubes</Link>
        <span aria-hidden="true" className="text-[#9aa6a0]">/</span>
        <span aria-current="page">{identity.officialName || team.teamName}</span>
      </nav>

      <ProfilePanel className="relative overflow-hidden bg-[#06271d] p-0" tone="accent">
        <div aria-hidden="true" className="pointer-events-none absolute inset-0 overflow-hidden">
          <div className="absolute -right-24 -top-28 h-80 w-80 rounded-full border border-[#a6f2d1]/15" />
          <div className="absolute right-10 top-12 h-48 w-48 rounded-full border border-dashed border-white/12" />
          <div className="absolute inset-x-0 bottom-0 h-40 bg-gradient-to-t from-[#001c14]/45 to-transparent" />
          <div className="absolute bottom-0 left-0 h-1.5 w-32 bg-[#a6f2d1]" />
        </div>

        <div data-testid="club-identity-hero" className="relative z-10 grid gap-8 p-5 sm:p-7 lg:grid-cols-[minmax(0,1fr)_minmax(20rem,0.48fr)] lg:p-9">
          <div className="flex min-w-0 flex-col justify-between gap-8">
            <div className="flex min-w-0 items-start gap-4 sm:items-center sm:gap-6">
              <div className="relative shrink-0">
                <ProfileMedia
                  alt={`Escudo de ${team.teamName}`}
                  assetId={team.visualAssetId ?? team.teamId}
                  category="clubs"
                  className="h-28 w-28 border border-white/18 bg-white/10 shadow-[0_20px_45px_-28px_rgba(0,0,0,0.9)] sm:h-36 sm:w-36"
                  fallback={getMonogram(team.teamName)}
                  href={buildClubResolverPath(team.teamId, sharedFilters)}
                  imageClassName="p-4 sm:p-5"
                  tone="contrast"
                />
                <span className="absolute -bottom-2 -right-2 rounded-full border border-[#06271d] bg-[#a6f2d1] px-2.5 py-1 text-[0.56rem] font-black uppercase tracking-[0.14em] text-[#003526]">
                  {team.visualAssetId ? "Escudo" : "Texto"}
                </span>
              </div>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="text-[0.68rem] font-bold uppercase tracking-[0.22em] text-[#a6f2d1]/75">Clube · arquivo histórico</p>
                  <span className="rounded-full border border-white/12 bg-white/8 px-2 py-1 text-[0.56rem] font-bold uppercase tracking-[0.14em] text-white/62">
                    Identidade documentada
                  </span>
                </div>
                <h1 className="mt-3 break-words font-[family:var(--font-profile-headline)] text-4xl font-extrabold leading-[0.92] tracking-[-0.06em] text-white sm:text-6xl">
                  {identity.officialName || team.teamName}
                </h1>
                <p className="mt-4 max-w-3xl text-sm leading-6 text-white/68">
                  {identityFacts.length > 0 ? identityFacts.join(" · ") : "Dados de origem ainda não documentados no acervo."}
                </p>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              {defaultContext ? (
                <Link className="button-pill button-pill-on-dark" href={buildCanonicalClubPath(defaultContext, teamId)}>
                  Abrir temporada mais recente
                </Link>
              ) : null}
              <Link className="button-pill button-pill-on-dark" href={headToHeadHref}>Comparar clube</Link>
              <Link className="button-pill button-pill-on-dark" href={clubsHref}>Ver clubes no recorte</Link>
            </div>
          </div>

          <aside data-testid="club-archive-timeline" className="rounded-[1.6rem] border border-white/12 bg-black/10 p-5 sm:p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-[0.66rem] font-bold uppercase tracking-[0.2em] text-white/55">Arquivo publicado</p>
                <p className="mt-2 text-sm text-white/55">Amostra disponível na plataforma</p>
              </div>
              <span className="rounded-full bg-[#a6f2d1]/12 px-2.5 py-1 text-[0.58rem] font-bold uppercase tracking-[0.14em] text-[#a6f2d1]">
                {formatInteger(availableContexts.length)} recortes
              </span>
            </div>

            <div className="mt-5 grid grid-cols-3 gap-2">
              <div className="rounded-xl bg-white/8 p-3">
                <p className="text-[0.58rem] font-bold uppercase tracking-[0.12em] text-white/48">Partidas</p>
                <p className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-white">{formatInteger(archive.matchesPlayed)}</p>
              </div>
              <div className="rounded-xl bg-white/8 p-3">
                <p className="text-[0.58rem] font-bold uppercase tracking-[0.12em] text-white/48">Temporadas</p>
                <p className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-white">{formatInteger(archive.seasonCount)}</p>
              </div>
              <div className="rounded-xl bg-white/8 p-3">
                <p className="text-[0.58rem] font-bold uppercase tracking-[0.12em] text-white/48">Competições</p>
                <p className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold tracking-[-0.04em] text-white">{formatInteger(archive.competitionCount)}</p>
              </div>
            </div>

            <div className="mt-6 rounded-xl border border-white/10 bg-white/5 p-4">
              <div className="flex items-center justify-between gap-3 text-[0.58rem] font-bold uppercase tracking-[0.13em] text-white/48">
                <span>Primeiro registro</span>
                <span>Último registro</span>
              </div>
              <div className="mt-3 flex items-center gap-2">
                <span className="h-2.5 w-2.5 shrink-0 rounded-full bg-[#a6f2d1] shadow-[0_0_0_4px_rgba(166,242,209,0.12)]" />
                <span className="h-px flex-1 border-t border-dashed border-[#a6f2d1]/35" />
                <span className="h-2.5 w-2.5 shrink-0 rounded-full border-2 border-[#a6f2d1] bg-[#06271d]" />
              </div>
              <div className="mt-2 flex justify-between gap-3 text-xs font-semibold text-white/72">
                <span>{archive.firstMatchAt ? formatDate(archive.firstMatchAt) : "—"}</span>
                <span>{archive.lastMatchAt ? formatDate(archive.lastMatchAt) : "—"}</span>
              </div>
            </div>

            <p className="mt-4 text-xs leading-5 text-white/52">{getArchiveSpan(archive.firstMatchAt, archive.lastMatchAt)} · o recorte não representa o histórico totalizante do clube.</p>
          </aside>
        </div>
      </ProfilePanel>

      {contextsQuery.isFetching || profileQuery.isFetching ? (
        <p aria-live="polite" className="text-xs font-semibold text-[#57657a]">Atualizando arquivo do clube…</p>
      ) : null}

      {contextsQuery.isError || profileQuery.isError ? (
        <ProfileAlert title="Perfil carregado com ressalvas" tone="warning">
          <p>{profileQuery.error?.message ?? contextsQuery.error?.message}</p>
          <button
            className="button-pill button-pill-secondary mt-3"
            onClick={() => {
              void contextsQuery.refetch();
              void profileQuery.refetch();
            }}
            type="button"
          >
            Tentar novamente
          </button>
        </ProfileAlert>
      ) : null}

      {profileIsPartial ? (
        <ProfileAlert title="Arquivo parcial" tone="warning">
          <p>Algumas temporadas, dados de identidade ou seções do clube ainda não estão cobertos pela publicação atual.</p>
        </ProfileAlert>
      ) : null}

      {honors ? (
        <TeamHonorsSection honors={honors} />
      ) : (
        <ProfilePanel className="space-y-3">
          <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Conquistas documentadas</p>
          <h2 className="font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.045em] text-[#111c2d]">Ainda sem registros publicados</h2>
          <p className="max-w-3xl text-sm leading-6 text-[#57657a]">Isso indica ausência de documentação nesta API, não ausência de conquistas na história do clube.</p>
        </ProfilePanel>
      )}

      <ProfilePanel className="space-y-6">
        <header className="grid gap-4 md:grid-cols-[minmax(0,1fr)_auto] md:items-end">
          <div>
            <p className="text-[0.68rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">Participações no acervo</p>
            <h2 className="mt-2 max-w-3xl font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.045em] text-[#111c2d] md:text-4xl">Competições e temporadas disponíveis</h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-[#57657a]">Escolha um recorte para abrir jornada, elenco, partidas e estatísticas com contexto explícito.</p>
          </div>
          <ProfileCoveragePill
            coverage={{ ...contextsQuery.coverage, label: "Cobertura dos recortes" }}
          />
        </header>

        {contextGroups.length > 0 ? (
          <div className="divide-y divide-[rgba(191,201,195,0.42)] border-y border-[rgba(191,201,195,0.42)]">
            {contextGroups.map((group, index) => (
              <details className="group py-4" key={`${group.competitionId}-${group.competitionKey}`} open={index === 0}>
                <summary className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-4 rounded-lg px-1 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#00513b] [&::-webkit-details-marker]:hidden">
                  <span>
                    <span className="block font-bold text-[#111c2d]">{group.competitionName}</span>
                    <span className="mt-1 block text-sm text-[#69778d]">{formatInteger(group.contexts.length)} temporadas publicadas</span>
                  </span>
                  <svg aria-hidden="true" className="h-5 w-5 shrink-0 text-[#57657a] transition-transform duration-200 group-open:rotate-180 motion-reduce:transition-none" fill="none" viewBox="0 0 20 20">
                    <path d="m6 8 4 4 4-4" stroke="currentColor" strokeLinecap="round" strokeWidth="1.7" />
                  </svg>
                </summary>
                <div className="grid gap-2 pb-1 pt-3 sm:grid-cols-2 lg:grid-cols-3">
                  {group.contexts.map((context) => (
                    <Link
                      className="flex min-h-12 items-center justify-between gap-3 rounded-xl border border-[rgba(191,201,195,0.48)] bg-white/74 px-4 py-3 text-sm font-semibold text-[#111c2d] hover:border-[#8bd6b6] hover:text-[#00513b]"
                      href={buildCanonicalClubPath(context, teamId)}
                      key={`${context.competitionId}-${context.seasonId}`}
                    >
                      {context.seasonLabel}
                      <span aria-hidden="true">→</span>
                    </Link>
                  ))}
                </div>
              </details>
            ))}
          </div>
        ) : (
          <EmptyState title="Sem temporadas detalhadas" description="O clube está identificado, mas os recortes de competição ainda não foram publicados." />
        )}
      </ProfilePanel>
    </ProfileShell>
  );
}
