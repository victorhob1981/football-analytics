"use client";

import { useState } from "react";

import { ArrowLeftRight, BarChart3, GitCompare, Link2, Swords, TrendingUp } from "lucide-react";
import Link from "next/link";

import {
    CartesianGrid,
    Legend,
    Line,
    LineChart,
    PolarAngleAxis,
    PolarGrid,
    Radar,
    RadarChart,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from "recharts";

import { useComparisonStore } from "@/shared/stores/comparison.store";

// ── Tipos e dados mock ────────────────────────────────────────────────────────

interface ClubStats {
    id: string;
    name: string;
    points: number;
    winRate: number;
    goalsFor: number;
    goalsAgainst: number;
    goalDifference: number;
    wins: number;
    draws: number;
    losses: number;
    topScorers: Array<{ playerId: string; name: string; goals: number; assists: number }>;
    radarScores: {
        ofensivo: number;
        defensivo: number;
        disciplina: number;
        consistencia: number;
        forma: number;
    };
    pointsEvolution: number[];
}

const MOCK_CLUBS: Record<string, ClubStats> = {
    "1": {
        id: "1", name: "Flamengo",
        points: 54, winRate: 66.7, goalsFor: 48, goalsAgainst: 22, goalDifference: 26, wins: 16, draws: 6, losses: 2,
        radarScores: { ofensivo: 92, defensivo: 85, disciplina: 70, consistencia: 88, forma: 95 },
        topScorers: [
            { playerId: "p1", name: "Pedro", goals: 14, assists: 5 },
            { playerId: "p2", name: "Arrascaeta", goals: 6, assists: 9 },
            { playerId: "p3", name: "De la Cruz", goals: 4, assists: 6 },
        ],
        pointsEvolution: [3, 6, 7, 10, 13, 16, 19, 22, 22, 25, 28, 31, 34, 34, 37, 40, 43, 43, 46, 49, 49, 52, 53, 54],
    },
    "2": {
        id: "2", name: "Palmeiras",
        points: 49, winRate: 58.3, goalsFor: 41, goalsAgainst: 26, goalDifference: 15, wins: 14, draws: 7, losses: 3,
        radarScores: { ofensivo: 80, defensivo: 88, disciplina: 82, consistencia: 84, forma: 78 },
        topScorers: [
            { playerId: "p10", name: "Estêvão", goals: 10, assists: 6 },
            { playerId: "p11", name: "Raphael Veiga", goals: 7, assists: 5 },
            { playerId: "p12", name: "Flaco López", goals: 6, assists: 3 },
        ],
        pointsEvolution: [3, 3, 6, 9, 9, 12, 15, 18, 21, 21, 24, 27, 27, 30, 33, 33, 36, 39, 39, 42, 42, 43, 46, 49],
    },
    "3": {
        id: "3", name: "Atlético-MG",
        points: 46, winRate: 54.2, goalsFor: 38, goalsAgainst: 28, goalDifference: 10, wins: 13, draws: 7, losses: 4,
        radarScores: { ofensivo: 76, defensivo: 72, disciplina: 65, consistencia: 76, forma: 80 },
        topScorers: [
            { playerId: "p20", name: "Hulk", goals: 9, assists: 3 },
            { playerId: "p21", name: "Paulinho", goals: 7, assists: 4 },
            { playerId: "p22", name: "Gustavo Scarpa", goals: 4, assists: 7 },
        ],
        pointsEvolution: [0, 3, 6, 9, 12, 12, 15, 18, 18, 21, 24, 24, 27, 27, 30, 33, 33, 36, 36, 39, 42, 43, 44, 46],
    },
};

const AVAILABLE_CLUBS = [
    { id: "1", name: "Flamengo" },
    { id: "2", name: "Palmeiras" },
    { id: "3", name: "Atlético-MG" },
    { id: "4", name: "Fluminense" },
    { id: "5", name: "Botafogo" },
    { id: "6", name: "São Paulo" },
];

const CHART_COLORS = { left: "#10B981", right: "#06B6D4" } as const;

const ROUND_LABELS = Array.from({ length: 24 }, (_, i) => `R${i + 1}`);

// ── Helpers ───────────────────────────────────────────────────────────────────

function getClub(id: string): ClubStats {
    return MOCK_CLUBS[id] ?? {
        id, name: `Clube ${id}`,
        points: 40, winRate: 50, goalsFor: 35, goalsAgainst: 30, goalDifference: 5, wins: 12, draws: 4, losses: 8,
        radarScores: { ofensivo: 70, defensivo: 70, disciplina: 70, consistencia: 70, forma: 70 },
        topScorers: [],
        pointsEvolution: Array.from({ length: 24 }, (_, i) => (i + 1) * (40 / 24)),
    };
}

// ── Componentes utilitários ───────────────────────────────────────────────────

function SectionShell({ title, subtitle, icon: Icon, children }: {
    title: string; subtitle?: string; icon?: React.ElementType; children: React.ReactNode;
}) {
    return (
        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
            <header className="mb-4 flex items-start gap-3">
                {Icon && <div className="mt-0.5 rounded-lg bg-emerald-500/10 p-1.5"><Icon className="h-4 w-4 text-emerald-400" /></div>}
                <div className="space-y-0.5">
                    <h2 className="text-base font-semibold text-slate-100">{title}</h2>
                    {subtitle && <p className="text-xs text-slate-400">{subtitle}</p>}
                </div>
            </header>
            {children}
        </section>
    );
}

// ── Seção 2: KPI side-by-side com delta ──────────────────────────────────────

function KpiRow({ label, leftVal, rightVal, isLowerBetter = false, unit = "" }: {
    label: string; leftVal: number; rightVal: number; isLowerBetter?: boolean; unit?: string;
}) {
    const diff = rightVal - leftVal;
    const leftWins = isLowerBetter ? leftVal < rightVal : leftVal > rightVal;
    const rightWins = isLowerBetter ? rightVal < leftVal : rightVal > leftVal;
    const sign = diff > 0 ? "+" : "";
    const deltaColor = diff === 0 ? "text-slate-500" : rightWins ? "text-emerald-400" : "text-red-400";

    return (
        <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-3 rounded-lg border border-slate-700/40 bg-slate-800/30 px-4 py-2.5">
            <span className={`text-sm font-semibold ${leftWins ? "text-emerald-300" : "text-slate-300"}`}>
                {leftVal}{unit}
            </span>
            <div className="flex flex-col items-center gap-0.5">
                <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">{label}</span>
                <span className={`text-[10px] font-bold ${deltaColor}`}>{sign}{diff}{unit}</span>
            </div>
            <span className={`text-right text-sm font-semibold ${rightWins ? "text-emerald-300" : "text-slate-300"}`}>
                {rightVal}{unit}
            </span>
        </div>
    );
}

function ComparisonKpis({ left, right }: { left: ClubStats; right: ClubStats }) {
    return (
        <div className="space-y-2">
            <div className="grid grid-cols-[1fr_auto_1fr] px-4 pb-1 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                <span className="flex items-center gap-1.5">
                    <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: CHART_COLORS.left }} />
                    {left.name}
                </span>
                <span />
                <span className="flex items-center justify-end gap-1.5">
                    {right.name}
                    <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: CHART_COLORS.right }} />
                </span>
            </div>
            <KpiRow label="Pontos" leftVal={left.points} rightVal={right.points} />
            <KpiRow label="Aproveit. %" leftVal={Math.round(left.winRate)} rightVal={Math.round(right.winRate)} unit="%" />
            <KpiRow label="Gols marcados" leftVal={left.goalsFor} rightVal={right.goalsFor} />
            <KpiRow label="Gols sofridos" leftVal={left.goalsAgainst} rightVal={right.goalsAgainst} isLowerBetter />
            <KpiRow label="Saldo de gols" leftVal={left.goalDifference} rightVal={right.goalDifference} />
            <KpiRow label="Vitórias" leftVal={left.wins} rightVal={right.wins} />
            <KpiRow label="Empates" leftVal={left.draws} rightVal={right.draws} isLowerBetter />
            <KpiRow label="Derrotas" leftVal={left.losses} rightVal={right.losses} isLowerBetter />
        </div>
    );
}

// ── Seção 3: LineChart de pontos acumulados ───────────────────────────────────

function PointsEvolutionChart({ left, right }: { left: ClubStats; right: ClubStats }) {
    const data = ROUND_LABELS.map((round, i) => ({
        round,
        [left.name]: left.pointsEvolution[i] ?? null,
        [right.name]: right.pointsEvolution[i] ?? null,
    }));

    return (
        <div className="h-[260px] w-full">
            <ResponsiveContainer height="100%" width="100%">
                <LineChart data={data} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
                    <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                    <XAxis dataKey="round" stroke="#94A3B8" tick={{ fontSize: 10 }} />
                    <YAxis stroke="#94A3B8" tick={{ fontSize: 11 }} />
                    <Tooltip
                        contentStyle={{ backgroundColor: "#0F172A", border: "1px solid #334155", borderRadius: "8px", color: "#F1F5F9" }}
                        itemStyle={{ color: "#F1F5F9" }}
                        labelStyle={{ color: "#CBD5E1", fontWeight: 600 }}
                    />
                    <Legend wrapperStyle={{ color: "#CBD5E1", fontSize: 12 }} />
                    <Line dataKey={left.name} dot={false} stroke={CHART_COLORS.left} strokeWidth={2.5} type="monotone" />
                    <Line dataKey={right.name} dot={false} stroke={CHART_COLORS.right} strokeWidth={2.5} type="monotone" />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

// ── Seção 4: RadarChart multidimensional ──────────────────────────────────────

function RadarMetricsChart({ left, right }: { left: ClubStats; right: ClubStats }) {
    const dimensions = [
        { key: "ofensivo", label: "Ofensivo" },
        { key: "defensivo", label: "Defensivo" },
        { key: "disciplina", label: "Disciplina" },
        { key: "consistencia", label: "Consistência" },
        { key: "forma", label: "Forma" },
    ] as const;

    const data = dimensions.map(({ key, label }) => ({
        metric: label,
        [left.name]: left.radarScores[key],
        [right.name]: right.radarScores[key],
    }));

    return (
        <div className="h-[280px] w-full">
            <ResponsiveContainer height="100%" width="100%">
                <RadarChart data={data} margin={{ top: 8, right: 24, bottom: 8, left: 24 }}>
                    <PolarGrid stroke="#334155" />
                    <PolarAngleAxis dataKey="metric" tick={{ fill: "#94A3B8", fontSize: 11 }} />
                    <Radar dataKey={left.name} fill={CHART_COLORS.left} fillOpacity={0.15} name={left.name} stroke={CHART_COLORS.left} strokeWidth={2} />
                    <Radar dataKey={right.name} fill={CHART_COLORS.right} fillOpacity={0.15} name={right.name} stroke={CHART_COLORS.right} strokeWidth={2} />
                    <Legend wrapperStyle={{ color: "#CBD5E1", fontSize: 12 }} />
                    <Tooltip
                        contentStyle={{ backgroundColor: "#0F172A", border: "1px solid #334155", borderRadius: "8px", color: "#F1F5F9" }}
                        itemStyle={{ color: "#F1F5F9" }}
                    />
                </RadarChart>
            </ResponsiveContainer>
        </div>
    );
}

// ── Seção 5: Contribuidores de cada clube ────────────────────────────────────

function ContributorsPanel({ club, color }: { club: ClubStats; color: string }) {
    return (
        <div>
            <div className="mb-2 flex items-center gap-2">
                <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color }} />
                <h3 className="text-sm font-semibold text-slate-200">{club.name}</h3>
            </div>
            <ul className="space-y-1.5">
                {club.topScorers.map((s, i) => (
                    <li className="flex items-center gap-2 rounded-md border border-slate-700/40 bg-slate-800/30 px-3 py-1.5 text-xs" key={s.playerId}>
                        <span className="w-4 text-center text-slate-600">{i + 1}</span>
                        <Link className="flex-1 truncate font-medium text-slate-200 no-underline hover:text-emerald-300" href={`/players/${s.playerId}`}>{s.name}</Link>
                        <span className="flex items-center gap-1 text-emerald-400"><Swords className="h-3 w-3" />{s.goals}</span>
                        <span className="flex items-center gap-1 text-sky-400"><TrendingUp className="h-3 w-3" />{s.assists}</span>
                    </li>
                ))}
                {club.topScorers.length === 0 && (
                    <li className="py-2 text-center text-xs text-slate-500">Dados indisponíveis</li>
                )}
            </ul>
        </div>
    );
}

// ── Seção 6: H2H mini-resumo ──────────────────────────────────────────────────

function H2HSection({ left, right }: { left: ClubStats; right: ClubStats }) {
    // Mock H2H
    const h2h = { leftWins: 8, draws: 5, rightWins: 6, goalsLeft: 24, goalsRight: 20, total: 19 };
    const leftPct = Math.round((h2h.leftWins / h2h.total) * 100);
    const rightPct = Math.round((h2h.rightWins / h2h.total) * 100);
    const drawPct = 100 - leftPct - rightPct;

    return (
        <div className="space-y-4">
            <div className="grid grid-cols-3 gap-3 text-center">
                <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 py-3">
                    <p className="text-2xl font-black text-emerald-300">{h2h.leftWins}</p>
                    <p className="text-xs font-semibold text-emerald-400">Vitórias {left.name}</p>
                </div>
                <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 py-3">
                    <p className="text-2xl font-black text-amber-300">{h2h.draws}</p>
                    <p className="text-xs font-semibold text-amber-400">Empates</p>
                </div>
                <div className="rounded-xl border border-cyan-500/20 bg-cyan-500/10 py-3">
                    <p className="text-2xl font-black text-cyan-300">{h2h.rightWins}</p>
                    <p className="text-xs font-semibold text-cyan-400">Vitórias {right.name}</p>
                </div>
            </div>

            {/* Barra de dominância */}
            <div>
                <div className="mb-1.5 flex justify-between text-[10px] text-slate-500">
                    <span>{left.name}</span>
                    <span>Empates</span>
                    <span>{right.name}</span>
                </div>
                <div className="flex h-3 w-full overflow-hidden rounded-full">
                    <div className="bg-emerald-500" style={{ width: `${leftPct}%` }} />
                    <div className="bg-amber-500/60" style={{ width: `${drawPct}%` }} />
                    <div className="bg-cyan-500" style={{ width: `${rightPct}%` }} />
                </div>
                <div className="mt-1 flex justify-between text-[10px]">
                    <span className="text-emerald-400">{leftPct}%</span>
                    <span className="font-medium text-slate-400">Gols: {h2h.goalsLeft}–{h2h.goalsRight}</span>
                    <span className="text-cyan-400">{rightPct}%</span>
                </div>
            </div>

            <div className="flex justify-center pt-1">
                <Link
                    className="flex items-center gap-2 rounded-xl border border-slate-600 bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 no-underline transition-all hover:border-emerald-500/40 hover:text-emerald-300"
                    href={`/head-to-head?teamA=${left.id}&teamB=${right.id}`}
                >
                    <Link2 className="h-4 w-4" />
                    Ver H2H completo
                </Link>
            </div>
        </div>
    );
}

// ── Seletor de clubes ─────────────────────────────────────────────────────────

function ClubSelector({ label, value, color, onChange }: {
    label: string; value: string; color: string; onChange: (id: string) => void;
}) {
    return (
        <label className="flex flex-col gap-1 flex-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
            <span className="flex items-center gap-1.5">
                <span className="inline-block h-2 w-2 rounded-full" style={{ backgroundColor: color }} />
                {label}
            </span>
            <select
                className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2.5 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
                onChange={(e) => { onChange(e.target.value); }}
                value={value}
            >
                {AVAILABLE_CLUBS.map((c) => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                ))}
            </select>
        </label>
    );
}

// ── Página principal ──────────────────────────────────────────────────────────

export default function ClubsComparePage() {
    const compEntityType = useComparisonStore((s) => s.entityType);
    const storeIds = useComparisonStore((s) => s.selectedIds);
    const preselected = compEntityType === "team" && storeIds.length >= 2 ? storeIds : [];

    const [leftId, setLeftId] = useState(preselected[0] ?? "1");
    const [rightId, setRightId] = useState(preselected[1] ?? "2");

    const leftClub = getClub(leftId);
    const rightClub = getClub(rightId);

    return (
        <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
            <div className="mx-auto max-w-7xl space-y-5">

                {/* Header */}
                <header className="flex flex-wrap items-end justify-between gap-3">
                    <div>
                        <h1 className="text-2xl font-semibold tracking-tight text-slate-100">Comparativo de Clubes</h1>
                        <p className="mt-1 text-sm text-slate-400">Análise lado a lado com KPIs, tendência temporal, radar e H2H</p>
                    </div>
                    <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs font-medium text-amber-300">
                        ⚠️ Dados ilustrativos
                    </span>
                </header>

                {/* S1 — Seletor */}
                <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
                    <div className="mb-3 flex items-center gap-2">
                        <div className="rounded-lg bg-emerald-500/10 p-1.5"><GitCompare className="h-4 w-4 text-emerald-400" /></div>
                        <h2 className="text-base font-semibold text-slate-100">Selecionar Clubes</h2>
                    </div>
                    <div className="flex flex-wrap gap-4">
                        <ClubSelector color={CHART_COLORS.left} label="Clube A" onChange={setLeftId} value={leftId} />
                        <div className="flex items-end pb-2.5 text-slate-600"><ArrowLeftRight className="h-5 w-5" /></div>
                        <ClubSelector color={CHART_COLORS.right} label="Clube B" onChange={setRightId} value={rightId} />
                    </div>
                </section>

                {/* S2 — KPI side-by-side */}
                <SectionShell icon={BarChart3} subtitle="Diferença absoluta entre os dois clubes" title="Resultados Agregados">
                    <ComparisonKpis left={leftClub} right={rightClub} />
                </SectionShell>

                {/* S3 — Evolução de pontos */}
                <SectionShell icon={TrendingUp} subtitle="Pontos acumulados rodada a rodada" title="Evolução de Pontos">
                    <PointsEvolutionChart left={leftClub} right={rightClub} />
                </SectionShell>

                {/* S4 — RadarChart */}
                <SectionShell
                    icon={BarChart3}
                    subtitle="Índices calculados: ofensivo, defensivo, disciplina, consistência e forma (0–100)"
                    title="Radar de Métricas"
                >
                    <RadarMetricsChart left={leftClub} right={rightClub} />
                </SectionShell>

                {/* S5 — Contribuidores */}
                <SectionShell icon={Swords} subtitle="Top contribuidores de cada clube no período" title="Jogadores em Destaque">
                    <div className="grid gap-6 sm:grid-cols-2">
                        <ContributorsPanel club={leftClub} color={CHART_COLORS.left} />
                        <ContributorsPanel club={rightClub} color={CHART_COLORS.right} />
                    </div>
                </SectionShell>

                {/* S6 — H2H mini-resumo */}
                <SectionShell
                    icon={ArrowLeftRight}
                    subtitle={`Histórico de confrontos diretos entre ${leftClub.name} e ${rightClub.name}`}
                    title="Confronto Direto (H2H)"
                >
                    <H2HSection left={leftClub} right={rightClub} />
                </SectionShell>

            </div>
        </div>
    );
}
