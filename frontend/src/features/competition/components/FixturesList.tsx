"use client";

import Link from "next/link";

interface FixtureCardProps {
    fixtureId: string;
    homeTeamName: string;
    awayTeamName: string;
    homeGoals?: number | null;
    awayGoals?: number | null;
    playedAt?: string | null;
    status?: string | null;
}

function ScoreBadge({
    homeGoals,
    awayGoals,
    status,
}: {
    homeGoals?: number | null;
    awayGoals?: number | null;
    status?: string | null;
}) {
    const hasScore = homeGoals !== null && homeGoals !== undefined && awayGoals !== null && awayGoals !== undefined;

    if (!hasScore) {
        return (
            <span className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1 text-xs font-medium text-slate-400">
                {status === "scheduled" ? "Ag." : "—"}
            </span>
        );
    }

    const homeWon = homeGoals > awayGoals;
    const awayWon = awayGoals > homeGoals;

    return (
        <div className="flex items-center gap-1 font-mono text-base font-bold">
            <span className={homeWon ? "text-emerald-300" : awayWon ? "text-slate-500" : "text-amber-300"}>
                {homeGoals}
            </span>
            <span className="text-slate-600">–</span>
            <span className={awayWon ? "text-emerald-300" : homeWon ? "text-slate-500" : "text-amber-300"}>
                {awayGoals}
            </span>
        </div>
    );
}

function formatDate(iso?: string | null): string {
    if (!iso) return "—";
    return new Date(iso).toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

export function FixtureRow({
    fixtureId,
    homeTeamName,
    awayTeamName,
    homeGoals,
    awayGoals,
    playedAt,
    status,
}: FixtureCardProps) {
    return (
        <Link
            className="flex items-center gap-3 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4 py-3 no-underline transition-colors hover:border-emerald-500/30 hover:bg-slate-800/70"
            href={`/matches/${fixtureId}`}
        >
            {/* Time da casa */}
            <span className="flex-1 truncate text-right text-sm font-medium text-slate-100">
                {homeTeamName}
            </span>

            {/* Placar */}
            <ScoreBadge awayGoals={awayGoals} homeGoals={homeGoals} status={status} />

            {/* Time visitante */}
            <span className="flex-1 truncate text-left text-sm font-medium text-slate-100">
                {awayTeamName}
            </span>

            {/* Data */}
            <span className="hidden shrink-0 text-[10px] text-slate-500 sm:block">
                {formatDate(playedAt)}
            </span>
        </Link>
    );
}

interface FixturesListProps {
    fixtures: FixtureCardProps[];
    loading?: boolean;
}

function SkeletonFixture() {
    return (
        <div className="flex h-12 items-center gap-3 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4 animate-pulse">
            <div className="h-4 flex-1 rounded bg-slate-700" />
            <div className="h-6 w-12 rounded bg-slate-700" />
            <div className="h-4 flex-1 rounded bg-slate-700" />
        </div>
    );
}

export function FixturesList({ fixtures, loading }: FixturesListProps) {
    if (loading) {
        return (
            <div className="space-y-2">
                {Array.from({ length: 5 }).map((_, i) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <SkeletonFixture key={i} />
                ))}
            </div>
        );
    }

    if (fixtures.length === 0) {
        return (
            <div className="rounded-xl border border-slate-700 bg-slate-900 p-6 text-center">
                <p className="text-sm text-slate-400">Nenhuma partida encontrada para esta rodada.</p>
            </div>
        );
    }

    return (
        <div className="space-y-2">
            {fixtures.map((f) => (
                <FixtureRow key={f.fixtureId} {...f} />
            ))}
        </div>
    );
}
