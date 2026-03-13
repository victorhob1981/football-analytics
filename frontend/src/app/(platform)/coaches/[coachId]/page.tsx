"use client";

import { use } from "react";

import {
  ArrowLeft,
  Award,
  BarChart3,
  ChevronLeft,
  GitCompare,
  Percent,
  Shield,
  Swords,
  Timer,
  Trophy,
  Users,
} from "lucide-react";
import Link from "next/link";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

// ── Dados mock ────────────────────────────────────────────────────────────────

interface CoachProfile {
  id: string;
  name: string;
  nationality: string;
  age: number;
  currentClub: string;
  currentSince: string;
  totalMatches: number;
  wins: number;
  draws: number;
  losses: number;
  goalsFor: number;
  goalsAgainst: number;
  trophies: string[];
  clubHistory: Array<{
    club: string;
    from: string;
    to: string;
    matches: number;
    wins: number;
    draws: number;
    losses: number;
  }>;
  rankingPosition: number;
  rankingTotal: number;
}

const MOCK_DATA: Record<string, CoachProfile> = {
  c1: {
    id: "c1", name: "Filipe Luís", nationality: "Brasil", age: 39,
    currentClub: "Flamengo", currentSince: "2024-07",
    totalMatches: 38, wins: 26, draws: 7, losses: 5,
    goalsFor: 72, goalsAgainst: 32,
    trophies: ["Brasileirão 2024", "Copa do Brasil 2024"],
    rankingPosition: 1, rankingTotal: 10,
    clubHistory: [
      { club: "Flamengo Sub-17", from: "2023-01", to: "2024-06", matches: 30, wins: 22, draws: 5, losses: 3 },
      { club: "Flamengo", from: "2024-07", to: "Atual", matches: 38, wins: 26, draws: 7, losses: 5 },
    ],
  },
  c2: {
    id: "c2", name: "Abel Ferreira", nationality: "Portugal", age: 46,
    currentClub: "Palmeiras", currentSince: "2020-10",
    totalMatches: 250, wins: 158, draws: 48, losses: 44,
    goalsFor: 460, goalsAgainst: 210,
    trophies: ["Libertadores 2020", "Libertadores 2021", "Brasileirão 2022", "Brasileirão 2023"],
    rankingPosition: 2, rankingTotal: 10,
    clubHistory: [
      { club: "Braga", from: "2017-10", to: "2020-10", matches: 120, wins: 68, draws: 28, losses: 24 },
      { club: "Palmeiras", from: "2020-10", to: "Atual", matches: 250, wins: 158, draws: 48, losses: 44 },
    ],
  },
};

function getMockCoach(id: string): CoachProfile {
  return MOCK_DATA[id] ?? {
    id, name: `Técnico ${id}`, nationality: "—", age: 45,
    currentClub: "—", currentSince: "—",
    totalMatches: 30, wins: 15, draws: 8, losses: 7,
    goalsFor: 40, goalsAgainst: 30,
    trophies: [],
    rankingPosition: 5, rankingTotal: 10,
    clubHistory: [],
  };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function calcWinRate(wins: number, matches: number) {
  return matches > 0 ? Math.round(((wins * 3) / (matches * 3)) * 100) : 0;
}

// ── SectionShell ──────────────────────────────────────────────────────────────

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

// ── KPI Card ──────────────────────────────────────────────────────────────────

function KpiCard({ label, value, accent, icon: Icon }: {
  label: string; value: string | number; accent: string; icon?: React.ElementType;
}) {
  return (
    <article className="stat-card hover-lift" style={{ borderLeftColor: accent }}>
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="text-xs font-medium text-slate-400">{label}</p>
          <p className="mt-2 text-2xl font-bold text-slate-100">{value}</p>
        </div>
        {Icon && (
          <div className="rounded-lg p-1.5" style={{ backgroundColor: `${accent}1f` }}>
            <Icon className="h-4 w-4" color={accent} />
          </div>
        )}
      </div>
    </article>
  );
}

// ── Seção 3: Mini-ranking comparativo ─────────────────────────────────────────

const RANKING_DATA = [
  { name: "Filipe Luís", winRate: 74, isHighlight: false },
  { name: "Abel Ferreira", winRate: 69, isHighlight: false },
  { name: "Artur Jorge", winRate: 66, isHighlight: false },
  { name: "Rogério Ceni", winRate: 54, isHighlight: false },
  { name: "Fernando Diniz", winRate: 52, isHighlight: false },
];

function MiniRankingChart({ coachName }: { coachName: string }) {
  const data = RANKING_DATA.map((r) => ({ ...r, isHighlight: r.name === coachName }));

  return (
    <div className="h-[200px] w-full">
      <ResponsiveContainer height="100%" width="100%">
        <BarChart data={data} layout="vertical" margin={{ top: 0, right: 36, left: 8, bottom: 0 }}>
          <CartesianGrid horizontal={false} stroke="#334155" strokeDasharray="3 3" />
          <XAxis domain={[0, 100]} tick={{ fontSize: 10, fill: "#94A3B8" }} type="number" unit="%" />
          <YAxis dataKey="name" tick={{ fontSize: 10, fill: "#94A3B8" }} type="category" width={90} />
          <Tooltip
            contentStyle={{ backgroundColor: "#0F172A", border: "1px solid #334155", borderRadius: "8px", color: "#F1F5F9" }}
            formatter={(v: number) => [`${v}%`, "Aproveitamento"]}
          />
          <Bar dataKey="winRate" name="Aproveitamento" radius={[0, 3, 3, 0]}>
            {data.map((entry, i) => (
              <Cell fill={entry.isHighlight ? "#10B981" : "#334155"} key={i} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Seção 4: Timeline de clubes ───────────────────────────────────────────────

function ClubTimeline({ history, coachName }: { history: CoachProfile["clubHistory"]; coachName: string }) {
  if (history.length === 0) {
    return <p className="py-4 text-center text-xs text-slate-500">Histórico de clubes indisponível.</p>;
  }

  return (
    <div className="relative space-y-0">
      {/* Linha vertical */}
      <div className="absolute left-[22px] top-4 h-[calc(100%-2rem)] w-px bg-slate-700/60" />
      {history.map((h, i) => {
        const wr = calcWinRate(h.wins, h.matches);
        return (
          <div className="relative flex items-start gap-4 pb-6" key={i}>
            {/* Dot */}
            <div className="relative z-10 mt-1 flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-emerald-500/40 bg-slate-900">
              <div className="h-2 w-2 rounded-full bg-emerald-400" />
            </div>
            {/* Card */}
            <div className="flex-1 rounded-lg border border-slate-700/40 bg-slate-800/30 px-4 py-3">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div>
                  <p className="font-semibold text-slate-200">{h.club}</p>
                  <p className="text-[10px] text-slate-500">{h.from} → {h.to}</p>
                </div>
                <span className={`rounded-full px-2.5 py-0.5 text-[10px] font-bold ${wr >= 60 ? "bg-emerald-500/20 text-emerald-300" : wr >= 45 ? "bg-amber-500/20 text-amber-300" : "bg-red-500/20 text-red-400"}`}>
                  {wr}% aprov.
                </span>
              </div>
              <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[10px]">
                <span className="text-slate-500">{h.matches} jogos</span>
                <span className="text-emerald-400">{h.wins}V</span>
                <span className="text-amber-400">{h.draws}E</span>
                <span className="text-red-400">{h.losses}D</span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── Página ────────────────────────────────────────────────────────────────────

type Props = { params: Promise<{ coachId: string }> };

export default function CoachProfilePage({ params }: Props) {
  const { coachId } = use(params);
  const coach = getMockCoach(coachId);
  const wr = calcWinRate(coach.wins, coach.totalMatches);
  const gpg = coach.totalMatches > 0 ? (coach.goalsFor / coach.totalMatches).toFixed(2) : "—";

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-5xl space-y-5">

        {/* S1 — Header */}
        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-6 shadow-lg shadow-slate-950/40 backdrop-blur-sm">
          {/* Breadcrumb */}
          <div className="mb-4 flex items-center gap-2 text-xs text-slate-500">
            <Link className="no-underline hover:text-emerald-300" href="/coaches">
              <ChevronLeft className="inline h-3 w-3" /> Técnicos
            </Link>
            <span>/</span>
            <span className="text-slate-400">{coach.name}</span>
          </div>

          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="flex items-center gap-4">
              {/* Avatar */}
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-emerald-500/20 bg-emerald-900/20 text-2xl font-black text-emerald-300">
                {coach.name.charAt(0)}
              </div>
              <div className="space-y-1.5">
                <h1 className="text-2xl font-bold text-slate-100">{coach.name}</h1>
                <div className="flex flex-wrap items-center gap-2 text-sm">
                  <span className="text-slate-400">{coach.nationality}</span>
                  <span className="text-slate-600">·</span>
                  <span className="text-slate-400">{coach.age} anos</span>
                  <span className="text-slate-600">·</span>
                  <span className="font-medium text-emerald-300">{coach.currentClub}</span>
                  <span className="text-xs text-slate-500">desde {coach.currentSince}</span>
                </div>
                {/* Troféus */}
                {coach.trophies.length > 0 && (
                  <div className="flex flex-wrap gap-1.5">
                    {coach.trophies.map((t) => (
                      <span className="flex items-center gap-1 rounded-full border border-yellow-500/30 bg-yellow-500/10 px-2 py-0.5 text-[9px] font-semibold text-yellow-300" key={t}>
                        <Trophy className="h-2.5 w-2.5" />{t}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Posição no ranking */}
            <div className="flex flex-col items-center rounded-xl border border-slate-700/50 bg-slate-800/40 px-5 py-3 text-center">
              <p className="text-3xl font-black text-emerald-300">#{coach.rankingPosition}</p>
              <p className="text-[10px] text-slate-500">de {coach.rankingTotal} técnicos</p>
              <Link className="mt-1.5 text-[10px] text-emerald-400 no-underline hover:underline" href="/coaches">
                ver ranking
              </Link>
            </div>
          </div>
        </section>

        {/* S2 — KPI cards de performance */}
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <KpiCard accent="#10B981" icon={Users} label="Jogos comandados" value={coach.totalMatches} />
          <KpiCard accent="#F59E0B" icon={Percent} label="Aproveitamento" value={`${wr}%`} />
          <KpiCard accent="#06B6D4" icon={Swords} label="Gols/Jogo" value={gpg} />
          <KpiCard accent="#8B5CF6" icon={Trophy} label="Vitórias" value={coach.wins} />
        </div>

        {/* V/E/D breakdown */}
        <div className="grid grid-cols-3 gap-3">
          <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 py-3 text-center">
            <p className="text-2xl font-black text-emerald-300">{coach.wins}</p>
            <p className="text-xs font-semibold text-emerald-400">Vitórias</p>
          </div>
          <div className="rounded-xl border border-amber-500/20 bg-amber-500/10 py-3 text-center">
            <p className="text-2xl font-black text-amber-300">{coach.draws}</p>
            <p className="text-xs font-semibold text-amber-400">Empates</p>
          </div>
          <div className="rounded-xl border border-red-500/20 bg-red-500/10 py-3 text-center">
            <p className="text-2xl font-black text-red-400">{coach.losses}</p>
            <p className="text-xs font-semibold text-red-500">Derrotas</p>
          </div>
        </div>

        {/* Gols for/against */}
        <div className="grid gap-3 sm:grid-cols-2">
          <div className="flex items-center gap-3 rounded-xl border border-slate-700/50 bg-slate-900/60 px-4 py-3">
            <Shield className="h-5 w-5 text-emerald-400" />
            <div>
              <p className="text-lg font-bold text-emerald-300">{coach.goalsFor}</p>
              <p className="text-[10px] text-slate-500">Gols marcados</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border border-slate-700/50 bg-slate-900/60 px-4 py-3">
            <Shield className="h-5 w-5 text-red-400" />
            <div>
              <p className="text-lg font-bold text-red-400">{coach.goalsAgainst}</p>
              <p className="text-[10px] text-slate-500">Gols sofridos</p>
            </div>
          </div>
        </div>

        {/* S3 — Mini-ranking comparativo */}
        <SectionShell
          icon={BarChart3}
          subtitle="Aproveitamento em relação aos outros técnicos da temporada (destacado em emerald)"
          title="Posição no Ranking"
        >
          <MiniRankingChart coachName={coach.name} />
          <div className="mt-3 flex justify-center">
            <Link
              className="flex items-center gap-2 rounded-xl border border-slate-600 bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 no-underline transition-all hover:border-emerald-500/40 hover:text-emerald-300"
              href="/coaches"
            >
              <Users className="h-4 w-4" />
              Ver ranking completo
            </Link>
          </div>
        </SectionShell>

        {/* S4 — Histórico de clubes */}
        <SectionShell icon={Award} subtitle="Períodos, resultados e aproveitamento por clube" title="Histórico de Clubes">
          <ClubTimeline coachName={coach.name} history={coach.clubHistory} />
        </SectionShell>

      </div>
    </div>
  );
}
