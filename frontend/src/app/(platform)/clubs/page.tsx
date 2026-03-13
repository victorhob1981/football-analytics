"use client";

import { useMemo, useState } from "react";

import { Shield, Swords, TrendingUp } from "lucide-react";
import Link from "next/link";

import { useClubsList } from "@/features/clubs/hooks";
import type { ClubListItem, ClubsListLocalFilters } from "@/features/clubs/types";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useComparisonStore } from "@/shared/stores/comparison.store";

type RecentResult = "W" | "D" | "L";

const FORM_STYLES: Record<RecentResult, string> = {
  W: "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40",
  D: "bg-amber-500/20 text-amber-300 border border-amber-500/40",
  L: "bg-red-500/20 text-red-400 border border-red-500/40",
};

const FORM_LABEL: Record<RecentResult, string> = { W: "V", D: "E", L: "D" };

function isRecentResult(value: string): value is RecentResult {
  return value === "W" || value === "D" || value === "L";
}

function toRecentForm(form: string[] | null | undefined): RecentResult[] {
  if (!Array.isArray(form)) {
    return [];
  }

  return form.filter(isRecentResult).slice(0, 5);
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

function FormBadge({ result }: { result: RecentResult }) {
  return (
    <span className={`inline-flex h-6 w-6 items-center justify-center rounded text-[11px] font-bold ${FORM_STYLES[result]}`}>
      {FORM_LABEL[result]}
    </span>
  );
}

function ZoneBadge({ position }: { position: number | null | undefined }) {
  if (typeof position !== "number") {
    return null;
  }

  if (position <= 1) {
    return <span className="rounded-full bg-yellow-500/20 px-2 py-0.5 text-[10px] font-bold text-yellow-300">Titulo</span>;
  }
  if (position <= 6) {
    return <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-[10px] font-bold text-emerald-300">Libertadores</span>;
  }
  if (position >= 17) {
    return <span className="rounded-full bg-red-500/20 px-2 py-0.5 text-[10px] font-bold text-red-400">Rebaixamento</span>;
  }

  return null;
}

function TeamBadge({ teamName, logoUrl }: { teamName: string; logoUrl?: string | null }) {
  const initials = toTeamInitials(teamName);
  const hasLogo = typeof logoUrl === "string" && logoUrl.trim().length > 0;

  return (
    <span
      aria-label={`Logo ${teamName}`}
      className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-slate-600 bg-slate-800 text-[11px] font-semibold text-slate-300"
      style={hasLogo ? { backgroundImage: `url(${logoUrl})`, backgroundSize: "cover", backgroundPosition: "center" } : undefined}
      title={teamName}
    >
      {hasLogo ? null : initials}
    </span>
  );
}

type ClubCardProps = {
  club: ClubListItem;
  isSelected: boolean;
  isDisabled: boolean;
  onCompare: (id: string) => void;
};

function ClubCard({ club, isSelected, isDisabled, onCompare }: ClubCardProps) {
  const position = club.position ?? null;
  const points = club.points ?? 0;
  const matchesPlayed = club.matchesPlayed ?? 0;
  const wins = club.wins ?? 0;
  const draws = club.draws ?? 0;
  const losses = club.losses ?? 0;
  const goalsFor = club.goalsFor ?? 0;
  const goalsAgainst = club.goalsAgainst ?? 0;
  const goalDifference = club.goalDifference ?? goalsFor - goalsAgainst;
  const form = toRecentForm(club.recentForm);

  return (
    <article
      className={`group rounded-xl border p-5 shadow-lg shadow-slate-950/30 transition-all ${
        isSelected
          ? "border-emerald-500/60 bg-emerald-900/20 ring-1 ring-emerald-500/40"
          : "border-slate-700/80 bg-slate-900/80 hover:border-slate-600 hover:bg-slate-900"
      }`}
    >
      <div className="mb-4 flex items-start justify-between gap-2">
        <div className="flex items-center gap-2">
          <span
            className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg text-sm font-bold ${
              position === 1
                ? "bg-yellow-500/20 text-yellow-300"
                : typeof position === "number" && position <= 6
                  ? "bg-emerald-500/20 text-emerald-300"
                  : typeof position === "number" && position >= 17
                    ? "bg-red-500/20 text-red-400"
                    : "bg-slate-800 text-slate-300"
            }`}
          >
            {position ?? "-"}
          </span>

          <TeamBadge logoUrl={club.logoUrl} teamName={club.teamName} />

          <div>
            <Link
              className="text-sm font-semibold text-slate-100 no-underline transition-colors group-hover:text-emerald-300"
              href={`/clubs/${club.teamId}`}
            >
              {club.teamName}
            </Link>
            <div className="mt-0.5">
              <ZoneBadge position={position} />
            </div>
          </div>
        </div>
        <div className="shrink-0 text-right">
          <p className="text-2xl font-bold text-slate-100">{points}</p>
          <p className="text-xs text-slate-500">pts</p>
        </div>
      </div>

      <div className="mb-4 grid grid-cols-3 gap-2">
        <div className="rounded-lg bg-slate-800/60 px-2 py-2 text-center">
          <Swords className="mx-auto mb-1 h-3.5 w-3.5 text-emerald-400" />
          <p className="text-base font-semibold text-slate-100">{goalsFor}</p>
          <p className="text-[10px] text-slate-500">Gols pro</p>
        </div>
        <div className="rounded-lg bg-slate-800/60 px-2 py-2 text-center">
          <Shield className="mx-auto mb-1 h-3.5 w-3.5 text-sky-400" />
          <p className="text-base font-semibold text-slate-100">{goalsAgainst}</p>
          <p className="text-[10px] text-slate-500">Gols sofridos</p>
        </div>
        <div className="rounded-lg bg-slate-800/60 px-2 py-2 text-center">
          <TrendingUp className="mx-auto mb-1 h-3.5 w-3.5 text-amber-400" />
          <p className={`text-base font-semibold ${goalDifference >= 0 ? "text-emerald-400" : "text-red-400"}`}>
            {goalDifference > 0 ? "+" : ""}
            {goalDifference}
          </p>
          <p className="text-[10px] text-slate-500">Saldo</p>
        </div>
      </div>

      <div className="mb-4">
        <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500">Ultimos 5</p>
        {form.length > 0 ? (
          <div className="flex gap-1">
            {form.map((result, index) => (
              // eslint-disable-next-line react/no-array-index-key
              <FormBadge key={`${club.teamId}-${index}`} result={result} />
            ))}
          </div>
        ) : (
          <p className="text-xs text-slate-500">Sem forma recente.</p>
        )}
      </div>

      <div className="flex items-center justify-between border-t border-slate-700/50 pt-3">
        <div className="flex gap-3 text-xs text-slate-500">
          <span>
            J <strong className="text-slate-300">{matchesPlayed}</strong>
          </span>
          <span>
            V <strong className="text-emerald-400">{wins}</strong>
          </span>
          <span>
            E <strong className="text-amber-400">{draws}</strong>
          </span>
          <span>
            D <strong className="text-red-400">{losses}</strong>
          </span>
        </div>
        <button
          className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-40 ${
            isSelected
              ? "bg-emerald-500/20 text-emerald-300 hover:bg-emerald-500/30"
              : "border border-slate-600 bg-slate-800 text-slate-300 hover:border-emerald-500/60 hover:text-emerald-300"
          }`}
          disabled={isDisabled}
          onClick={() => {
            onCompare(club.teamId);
          }}
          type="button"
        >
          {isSelected ? "Selecionado" : "Comparar"}
        </button>
      </div>
    </article>
  );
}

export default function ClubsPage() {
  const [search, setSearch] = useState("");
  const [orderBy, setOrderBy] = useState<NonNullable<ClubsListLocalFilters["orderBy"]>>("points");

  const comparisonEntityType = useComparisonStore((state) => state.entityType);
  const selectedIds = useComparisonStore((state) => state.selectedIds);
  const addToComparison = useComparisonStore((state) => state.add);
  const removeFromComparison = useComparisonStore((state) => state.remove);
  const setEntityType = useComparisonStore((state) => state.setEntityType);

  const clubsQuery = useClubsList({
    search,
    orderBy,
    page: 1,
    pageSize: 50,
  });

  const selectedIdsSet = useMemo(() => {
    if (comparisonEntityType !== "team") {
      return new Set<string>();
    }

    return new Set(selectedIds);
  }, [comparisonEntityType, selectedIds]);
  const selectedCount = comparisonEntityType === "team" ? selectedIds.length : 0;

  function handleCompareAction(teamId: string) {
    if (comparisonEntityType !== "team") {
      setEntityType("team");
      addToComparison(teamId);
      return;
    }

    if (selectedIdsSet.has(teamId)) {
      removeFromComparison(teamId);
    } else {
      addToComparison(teamId);
    }
  }

  const clubs = clubsQuery.data?.items ?? [];

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-6">
        <header className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-slate-100">Clubes</h1>
            <p className="mt-1 text-sm text-slate-400">
              Explore os clubes da competicao, abra o perfil de cada um e selecione dois para comparar
            </p>
          </div>
        </header>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-4 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <div className="flex flex-wrap items-end gap-4">
            <label className="flex flex-1 flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              Buscar clube
              <input
                className="w-full rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(event) => {
                  setSearch(event.target.value);
                }}
                placeholder="Ex.: Flamengo"
                type="text"
                value={search}
              />
            </label>

            <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
              Ordenar por
              <select
                className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(event) => {
                  setOrderBy(event.target.value as NonNullable<ClubsListLocalFilters["orderBy"]>);
                }}
                value={orderBy}
              >
                <option value="points">Pontos</option>
                <option value="goalsFor">Gols marcados</option>
                <option value="goalsAgainst">Gols sofridos</option>
              </select>
            </label>
          </div>
        </section>

        {clubsQuery.isLoading ? (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {[0, 1, 2, 3, 4, 5, 6, 7].map((index) => (
              <LoadingSkeleton height={260} key={index} />
            ))}
          </div>
        ) : clubsQuery.isError && !clubsQuery.data ? (
          <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
            Falha ao carregar clubes: {clubsQuery.error?.message}
          </section>
        ) : clubs.length === 0 ? (
          <EmptyState
            description={search.trim().length > 0 ? `Nenhum clube encontrado para "${search}".` : "Sem clubes para os filtros atuais."}
            title="Lista vazia"
          />
        ) : (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            {clubs.map((club) => (
              <ClubCard
                club={club}
                isDisabled={!selectedIdsSet.has(club.teamId) && selectedCount >= 2}
                isSelected={selectedIdsSet.has(club.teamId)}
                key={club.teamId}
                onCompare={handleCompareAction}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
