"use client";

import { Shield, Swords, TrendingUp } from "lucide-react";
import Link from "next/link";

// ── Tipos ─────────────────────────────────────────────────────────────────────

type RecentResult = "W" | "D" | "L";

export interface StandingRow {
    teamId: string;
    teamName: string;
    position: number;
    points: number;
    matchesPlayed: number;
    wins: number;
    draws: number;
    losses: number;
    goalsFor: number;
    goalsAgainst: number;
    goalDifference: number;
    recentForm?: RecentResult[];
    zone?: "title" | "libertadores" | "sulamericana" | "relegation" | null;
}

// ── Sub-componentes ───────────────────────────────────────────────────────────

const FORM_STYLES: Record<RecentResult, string> = {
    W: "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40",
    D: "bg-amber-500/20 text-amber-300 border border-amber-500/40",
    L: "bg-red-500/20 text-red-400 border border-red-500/40",
};
const FORM_LABEL: Record<RecentResult, string> = { W: "V", D: "E", L: "D" };

function FormBadge({ result }: { result: RecentResult }) {
    return (
        <span className={`inline-flex h-5 w-5 items-center justify-center rounded text-[10px] font-bold ${FORM_STYLES[result]}`}>
            {FORM_LABEL[result]}
        </span>
    );
}

const ZONE_STYLES: Record<string, string> = {
    title: "border-l-yellow-400",
    libertadores: "border-l-emerald-500",
    sulamericana: "border-l-sky-500",
    relegation: "border-l-red-500",
};

const ZONE_DOT: Record<string, string> = {
    title: "bg-yellow-400",
    libertadores: "bg-emerald-500",
    sulamericana: "bg-sky-500",
    relegation: "bg-red-500",
};

const POS_STYLES: Record<string, string> = {
    title: "bg-yellow-500/20 text-yellow-300",
    libertadores: "bg-emerald-500/20 text-emerald-300",
    sulamericana: "bg-sky-500/20 text-sky-300",
    relegation: "bg-red-500/20 text-red-400",
};

// ── Componente principal ──────────────────────────────────────────────────────

interface StandingsTableProps {
    rows: StandingRow[];
    loading?: boolean;
}

function SkeletonRow() {
    return (
        <li className="flex h-12 items-center gap-3 rounded-lg border border-slate-700/50 bg-slate-800/40 px-3 animate-pulse">
            <div className="h-5 w-5 rounded bg-slate-700" />
            <div className="h-4 w-32 rounded bg-slate-700" />
            <div className="ml-auto h-4 w-24 rounded bg-slate-700" />
        </li>
    );
}

export function StandingsTable({ rows, loading }: StandingsTableProps) {
    if (loading) {
        return (
            <ul className="space-y-1.5">
                {Array.from({ length: 10 }).map((_, i) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <SkeletonRow key={i} />
                ))}
            </ul>
        );
    }

    if (rows.length === 0) {
        return (
            <div className="rounded-xl border border-slate-700 bg-slate-900 p-8 text-center">
                <p className="text-sm text-slate-400">Dados de classificação indisponíveis para este recorte.</p>
            </div>
        );
    }

    return (
        <div className="overflow-x-auto">
            {/* Header */}
            <div className="mb-2 grid min-w-[640px] grid-cols-[2rem_1fr_2.5rem_2rem_2rem_2rem_2rem_2.5rem_2.5rem_2.5rem_7rem] items-center gap-2 px-3 text-[10px] font-semibold uppercase tracking-wider text-slate-500">
                <span className="text-center">#</span>
                <span>Time</span>
                <span className="text-center">Pts</span>
                <span className="text-center">J</span>
                <span className="text-center">V</span>
                <span className="text-center">E</span>
                <span className="text-center">D</span>
                <span className="text-center">GP</span>
                <span className="text-center">GC</span>
                <span className="text-center">SG</span>
                <span className="text-center">Últimos 5</span>
            </div>

            <ul className="space-y-1 min-w-[640px]">
                {rows.map((row) => {
                    const zone = row.zone ?? null;
                    return (
                        <li
                            className={`grid grid-cols-[2rem_1fr_2.5rem_2rem_2rem_2rem_2rem_2.5rem_2.5rem_2.5rem_7rem] items-center gap-2 rounded-lg border border-l-4 border-slate-700/50 px-3 py-2.5 transition-colors hover:bg-slate-800/60 ${zone ? (ZONE_STYLES[zone] ?? "border-l-slate-700") : "border-l-slate-700/50"
                                } bg-slate-800/40`}
                            key={row.teamId}
                        >
                            {/* Posição */}
                            <span
                                className={`flex h-6 w-6 items-center justify-center rounded text-xs font-bold ${zone ? (POS_STYLES[zone] ?? "text-slate-400") : "text-slate-400"
                                    }`}
                            >
                                {row.position}
                            </span>

                            {/* Nome do time */}
                            <div className="flex min-w-0 items-center gap-2">
                                {zone && <span className={`hidden h-2 w-2 shrink-0 rounded-full sm:inline-block ${ZONE_DOT[zone]}`} />}
                                <Link
                                    className="truncate text-sm font-medium text-slate-100 no-underline hover:text-emerald-300"
                                    href={`/clubs/${row.teamId}`}
                                >
                                    {row.teamName}
                                </Link>
                            </div>

                            {/* Estatísticas */}
                            <span className="text-center text-sm font-bold text-slate-100">{row.points}</span>
                            <span className="text-center text-xs text-slate-400">{row.matchesPlayed}</span>
                            <span className="text-center text-xs text-emerald-400">{row.wins}</span>
                            <span className="text-center text-xs text-amber-400">{row.draws}</span>
                            <span className="text-center text-xs text-red-400">{row.losses}</span>
                            <span className="text-center text-xs text-slate-300">{row.goalsFor}</span>
                            <span className="text-center text-xs text-slate-300">{row.goalsAgainst}</span>
                            <span className={`text-center text-xs font-semibold ${row.goalDifference >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                                {row.goalDifference > 0 ? "+" : ""}{row.goalDifference}
                            </span>

                            {/* Forma */}
                            <div className="flex justify-center gap-0.5">
                                {(row.recentForm ?? []).map((r, i) => (
                                    // eslint-disable-next-line react/no-array-index-key
                                    <FormBadge key={i} result={r} />
                                ))}
                            </div>
                        </li>
                    );
                })}
            </ul>

            {/* Legenda de zonas */}
            <div className="mt-3 flex flex-wrap gap-4 px-1 text-[10px] text-slate-500">
                <span className="flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-yellow-400" />Título</span>
                <span className="flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-emerald-500" />Libertadores</span>
                <span className="flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-sky-500" />Sul-Americana</span>
                <span className="flex items-center gap-1.5"><span className="h-2 w-2 rounded-full bg-red-500" />Rebaixamento</span>
            </div>
        </div>
    );
}

// Export icons para usar na página
export { Shield, Swords, TrendingUp };
