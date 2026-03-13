"use client";

import { use, useState } from "react";

import {
  Activity,
  AlertCircle,
  ArrowLeftRight,
  BarChart3,
  ChevronLeft,
  GitCompare,
  Shield,
  Star,
  Swords,
  Trophy,
  TrendingUp,
  Users,
} from "lucide-react";
import Link from "next/link";

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { useComparisonStore } from "@/shared/stores/comparison.store";

// ── Tipos e dados mock ────────────────────────────────────────────────────────

type RecentResult = "W" | "D" | "L";

const FORM_STYLES: Record<RecentResult, string> = {
  W: "bg-emerald-500/20 text-emerald-300 border border-emerald-500/40",
  D: "bg-amber-500/20 text-amber-300 border border-amber-500/40",
  L: "bg-red-500/20 text-red-400 border border-red-500/40",
};
const FORM_LABEL: Record<RecentResult, string> = { W: "V", D: "E", L: "D" };

interface ClubProfile {
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
  winRate: number;
  recentForm: Array<{ result: RecentResult; opponent: string; homeGoals: number; awayGoals: number; isHome: boolean }>;
  monthlyGoals: Array<{ month: string; goalsFor: number; goalsAgainst: number }>;
  homeRecord: { wins: number; draws: number; losses: number; goalsFor: number; goalsAgainst: number };
  awayRecord: { wins: number; draws: number; losses: number; goalsFor: number; goalsAgainst: number };
  topScorers: Array<{ playerId: string; name: string; goals: number; assists: number }>;
  squad: Array<{ playerId?: string; name: string; appearances: number; minutesPlayed: number; position?: string }>;
  yellowCards: number;
  redCards: number;
  fouls: number;
}

function getMockProfile(clubId: string): ClubProfile {
  return {
    teamId: clubId,
    teamName: clubId === "1" ? "Flamengo" : clubId === "2" ? "Palmeiras" : `Clube ${clubId}`,
    position: 1,
    points: 54,
    matchesPlayed: 24,
    wins: 16,
    draws: 6,
    losses: 2,
    goalsFor: 48,
    goalsAgainst: 22,
    goalDifference: 26,
    winRate: 66.7,
    recentForm: [
      { result: "W", opponent: "Palmeiras", homeGoals: 2, awayGoals: 1, isHome: true },
      { result: "W", opponent: "Botafogo", homeGoals: 3, awayGoals: 0, isHome: false },
      { result: "D", opponent: "São Paulo", homeGoals: 1, awayGoals: 1, isHome: true },
      { result: "W", opponent: "Fluminense", homeGoals: 2, awayGoals: 0, isHome: false },
      { result: "W", opponent: "Internacional", homeGoals: 4, awayGoals: 1, isHome: true },
    ],
    monthlyGoals: [
      { month: "Set", goalsFor: 8, goalsAgainst: 3 },
      { month: "Out", goalsFor: 10, goalsAgainst: 4 },
      { month: "Nov", goalsFor: 12, goalsAgainst: 5 },
      { month: "Dez", goalsFor: 9, goalsAgainst: 3 },
      { month: "Jan", goalsFor: 6, goalsAgainst: 4 },
      { month: "Fev", goalsFor: 3, goalsAgainst: 3 },
    ],
    homeRecord: { wins: 10, draws: 3, losses: 1, goalsFor: 28, goalsAgainst: 12 },
    awayRecord: { wins: 6, draws: 3, losses: 1, goalsFor: 20, goalsAgainst: 10 },
    topScorers: [
      { playerId: "p1", name: "Pedro", goals: 14, assists: 5 },
      { playerId: "p2", name: "Arrascaeta", goals: 6, assists: 9 },
      { playerId: "p3", name: "De la Cruz", goals: 4, assists: 6 },
      { playerId: "p4", name: "Gerson", goals: 3, assists: 4 },
      { playerId: "p5", name: "Everton Cebolinha", goals: 3, assists: 3 },
    ],
    squad: [
      { playerId: "gk1", name: "Agustín Rossi", appearances: 22, minutesPlayed: 1980, position: "GK" },
      { playerId: "d1", name: "Varela", appearances: 20, minutesPlayed: 1720, position: "RB" },
      { playerId: "d2", name: "Léo Pereira", appearances: 21, minutesPlayed: 1800, position: "CB" },
      { playerId: "d3", name: "Fabrício Bruno", appearances: 19, minutesPlayed: 1650, position: "CB" },
      { playerId: "d4", name: "Ayrton Lucas", appearances: 22, minutesPlayed: 1910, position: "LB" },
      { playerId: "m1", name: "Gerson", appearances: 20, minutesPlayed: 1700, position: "CM" },
      { playerId: "m2", name: "De la Cruz", appearances: 21, minutesPlayed: 1780, position: "CM" },
      { playerId: "m3", name: "Arrascaeta", appearances: 18, minutesPlayed: 1500, position: "CAM" },
      { playerId: "f1", name: "Everton Cebolinha", appearances: 17, minutesPlayed: 1200, position: "LW" },
      { playerId: "f2", name: "Lorran", appearances: 15, minutesPlayed: 900, position: "AM" },
      { playerId: "f3", name: "Pedro", appearances: 22, minutesPlayed: 1900, position: "ST" },
    ],
    yellowCards: 38,
    redCards: 2,
    fouls: 210,
  };
}

// ── Componentes utilitários ───────────────────────────────────────────────────

function SectionShell({
  title,
  subtitle,
  icon: Icon,
  children,
  action,
}: {
  title: string;
  subtitle?: string;
  icon?: React.ElementType;
  children: React.ReactNode;
  action?: React.ReactNode;
}) {
  return (
    <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
      <header className="mb-4 flex items-start justify-between gap-3">
        <div className="flex items-start gap-3">
          {Icon && (
            <div className="mt-0.5 rounded-lg bg-emerald-500/10 p-1.5">
              <Icon className="h-4 w-4 text-emerald-400" />
            </div>
          )}
          <div className="space-y-0.5">
            <h2 className="text-base font-semibold text-slate-100">{title}</h2>
            {subtitle && <p className="text-xs text-slate-400">{subtitle}</p>}
          </div>
        </div>
        {action && <div className="shrink-0">{action}</div>}
      </header>
      {children}
    </section>
  );
}

function StatMini({ label, value, accent = "#10B981" }: { label: string; value: string | number; accent?: string }) {
  return (
    <div className="stat-card hover-lift" style={{ borderLeftColor: accent }}>
      <p className="text-xs font-medium text-slate-400">{label}</p>
      <p className="mt-1.5 text-2xl font-bold text-slate-100">{value}</p>
    </div>
  );
}

// ── Seção 1: Header ───────────────────────────────────────────────────────────

function ClubHeader({ profile, onCompare, isSelected }: {
  profile: ClubProfile;
  onCompare: () => void;
  isSelected: boolean;
}) {
  const gd = profile.goalDifference;
  return (
    <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-6 shadow-lg shadow-slate-950/40 backdrop-blur-sm">
      {/* Breadcrumb */}
      <div className="mb-4 flex items-center gap-2 text-xs text-slate-500">
        <Link className="no-underline hover:text-emerald-300" href="/clubs">
          <ChevronLeft className="inline h-3 w-3" /> Clubes
        </Link>
        <span>/</span>
        <span className="text-slate-400">{profile.teamName}</span>
      </div>

      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="flex items-center gap-4">
          {/* Avatar placeholder */}
          <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-slate-600 bg-slate-800 text-2xl font-black text-slate-300">
            {profile.teamName.charAt(0)}
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-100">{profile.teamName}</h1>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs">
              <span className="rounded-full bg-yellow-500/20 px-2 py-0.5 font-bold text-yellow-300">
                #{profile.position} lugar
              </span>
              <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 font-semibold text-emerald-300">
                {profile.points} pontos
              </span>
              <span className={`rounded-full px-2 py-0.5 font-semibold ${gd >= 0 ? "bg-emerald-500/10 text-emerald-400" : "bg-red-500/10 text-red-400"}`}>
                SG {gd > 0 ? "+" : ""}{gd}
              </span>
            </div>
          </div>
        </div>

        <button
          className={`flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold transition-all ${isSelected
              ? "border border-emerald-500/40 bg-emerald-500/20 text-emerald-300"
              : "border border-slate-600 bg-slate-800 text-slate-300 hover:border-emerald-500/40 hover:text-emerald-300"
            }`}
          onClick={onCompare}
          type="button"
        >
          <GitCompare className="h-4 w-4" />
          {isSelected ? "Selecionado" : "Comparar"}
        </button>
      </div>
    </section>
  );
}

// ── Seção 2: Insights ─────────────────────────────────────────────────────────

function InsightsSection({ teamName }: { teamName: string }) {
  const insights = [
    { type: "success", icon: "🚀", text: `${teamName} está em sequência de ${4} jogos sem derrota — tendência ascendente.` },
    { type: "warning", icon: "⚠️", text: "Dependência ofensiva: 29% dos gols marcados por um único jogador." },
    { type: "info", icon: "📊", text: "Melhor aproveitamento em casa da competição: 80% de pontos possíveis." },
  ];
  const styles: Record<string, string> = {
    success: "border-emerald-500/30 bg-emerald-500/10 text-emerald-300",
    warning: "border-amber-500/30 bg-amber-500/10 text-amber-300",
    info: "border-sky-500/30 bg-sky-500/10 text-sky-300",
  };

  return (
    <ul className="space-y-2">
      {insights.map((ins, i) => (
        // eslint-disable-next-line react/no-array-index-key
        <li className={`flex items-start gap-3 rounded-lg border px-4 py-3 text-sm ${styles[ins.type]}`} key={i}>
          <span className="text-base">{ins.icon}</span>
          <span>{ins.text}</span>
        </li>
      ))}
    </ul>
  );
}

// ── Seção 4: Forma recente ────────────────────────────────────────────────────

function RecentFormSection({ recentForm }: { recentForm: ClubProfile["recentForm"] }) {
  return (
    <div className="space-y-2">
      {recentForm.map((game, i) => {
        const style = FORM_STYLES[game.result];
        const scoreStr = game.isHome
          ? `${game.homeGoals}–${game.awayGoals}`
          : `${game.awayGoals}–${game.homeGoals}`;
        return (
          // eslint-disable-next-line react/no-array-index-key
          <div className="flex items-center gap-3 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4 py-2.5" key={i}>
            <span className={`flex h-7 w-7 shrink-0 items-center justify-center rounded text-xs font-bold ${style}`}>
              {FORM_LABEL[game.result]}
            </span>
            <span className="flex-1 text-sm text-slate-200">
              {game.isHome ? "vs" : "@"} {game.opponent}
            </span>
            <span className="font-mono text-sm font-semibold text-slate-300">{scoreStr}</span>
            <span className="text-xs text-slate-500">{game.isHome ? "Casa" : "Fora"}</span>
          </div>
        );
      })}
    </div>
  );
}

// ── Seção 5: Casa vs Fora ─────────────────────────────────────────────────────

function HomeAwaySection({ home, away }: { home: ClubProfile["homeRecord"]; away: ClubProfile["awayRecord"] }) {
  const homeWinRate = Math.round((home.wins / (home.wins + home.draws + home.losses)) * 100);
  const awayWinRate = Math.round((away.wins / (away.wins + away.draws + away.losses)) * 100);

  const Row = ({ label, homeVal, awayVal, higher = "home" }: {
    label: string; homeVal: number; awayVal: number; higher?: "home" | "away" | "none";
  }) => (
    <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-4 rounded-lg border border-slate-700/40 bg-slate-800/30 px-4 py-2.5">
      <span className={`text-sm font-semibold ${higher === "home" && homeVal > awayVal ? "text-emerald-300" : "text-slate-200"}`}>
        {homeVal}
      </span>
      <span className="text-center text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</span>
      <span className={`text-right text-sm font-semibold ${higher === "away" && awayVal > homeVal ? "text-emerald-300" : "text-slate-200"}`}>
        {awayVal}
      </span>
    </div>
  );

  return (
    <div className="space-y-2">
      <div className="grid grid-cols-[1fr_auto_1fr] pb-1 text-center text-xs font-bold uppercase tracking-wider text-slate-500">
        <span className="text-left">Casa</span>
        <span />
        <span className="text-right">Fora</span>
      </div>
      <Row awayVal={awayWinRate} homeVal={homeWinRate} label="Aproveitamento %" />
      <Row awayVal={away.wins} homeVal={home.wins} label="Vitórias" />
      <Row awayVal={away.draws} homeVal={home.draws} label="Empates" higher="none" />
      <Row awayVal={away.losses} homeVal={home.losses} label="Derrotas" higher="away" />
      <Row awayVal={away.goalsFor} homeVal={home.goalsFor} label="Gols marcados" />
      <Row awayVal={away.goalsAgainst} homeVal={home.goalsAgainst} label="Gols sofridos" higher="away" />
    </div>
  );
}

// ── Seção 6: Gols por mês ─────────────────────────────────────────────────────

function GoalsByMonthSection({ data }: { data: ClubProfile["monthlyGoals"] }) {
  return (
    <div className="h-[220px] w-full">
      <ResponsiveContainer height="100%" width="100%">
        <BarChart data={data} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
          <CartesianGrid stroke="#334155" strokeDasharray="3 3" vertical={false} />
          <XAxis dataKey="month" stroke="#94A3B8" tick={{ fontSize: 11 }} />
          <YAxis stroke="#94A3B8" tick={{ fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: "#0F172A", border: "1px solid #334155", borderRadius: "8px", color: "#F1F5F9" }}
            itemStyle={{ color: "#F1F5F9" }}
            labelStyle={{ color: "#CBD5E1", fontWeight: 600 }}
          />
          <Bar dataKey="goalsFor" fill="#10B981" name="Gols marcados" radius={[3, 3, 0, 0]} />
          <Bar dataKey="goalsAgainst" fill="#EF4444" name="Gols sofridos" radius={[3, 3, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Seção 8: Top contribuidores ───────────────────────────────────────────────

function TopScorersSection({ scorers }: { scorers: ClubProfile["topScorers"] }) {
  return (
    <ul className="space-y-1.5">
      {scorers.map((s, i) => (
        <li
          className="flex items-center gap-3 rounded-lg border border-slate-700/50 bg-slate-800/40 px-3 py-2"
          key={s.playerId}
        >
          <span className="w-5 text-center text-xs font-bold text-slate-500">{i + 1}</span>
          <Link
            className="flex-1 truncate text-sm font-medium text-slate-200 no-underline hover:text-emerald-300"
            href={`/players/${s.playerId}`}
          >
            {s.name}
          </Link>
          <span className="flex items-center gap-1 text-xs">
            <Swords className="h-3 w-3 text-emerald-400" />
            <strong className="text-emerald-300">{s.goals}</strong>
          </span>
          <span className="flex items-center gap-1 text-xs">
            <TrendingUp className="h-3 w-3 text-sky-400" />
            <strong className="text-sky-300">{s.assists}</strong>
          </span>
        </li>
      ))}
    </ul>
  );
}

// ── Seção 9: Disciplina ───────────────────────────────────────────────────────

function DisciplineSection({ yellow, red, fouls }: { yellow: number; red: number; fouls: number }) {
  return (
    <div className="grid grid-cols-3 gap-3">
      <div className="flex flex-col items-center justify-center rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-center">
        <span className="text-3xl font-black text-amber-300">{yellow}</span>
        <span className="mt-1 text-xs font-semibold text-amber-400">Amarelos</span>
      </div>
      <div className="flex flex-col items-center justify-center rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-center">
        <span className="text-3xl font-black text-red-400">{red}</span>
        <span className="mt-1 text-xs font-semibold text-red-400">Vermelhos</span>
        <span className="mt-0.5 text-[10px] text-slate-500">Cobertura 30%</span>
      </div>
      <div className="flex flex-col items-center justify-center rounded-xl border border-slate-600 bg-slate-800/60 p-4 text-center">
        <span className="text-3xl font-black text-slate-300">{fouls}</span>
        <span className="mt-1 text-xs font-semibold text-slate-400">Faltas</span>
      </div>
    </div>
  );
}

// ── Seção 10: Elenco utilizado ────────────────────────────────────────────────

function SquadSection({ squad }: { squad: ClubProfile["squad"] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-slate-700/50">
            {["Jogador", "Pos.", "Jogos", "Minutos"].map((h) => (
              <th className="px-3 py-2 text-left font-semibold uppercase tracking-wider text-slate-500" key={h}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-700/30">
          {squad.map((p, i) => (
            // eslint-disable-next-line react/no-array-index-key
            <tr className="hover:bg-slate-800/30" key={p.playerId ?? i}>
              <td className="px-3 py-2">
                {p.playerId ? (
                  <Link className="font-medium text-slate-200 no-underline hover:text-emerald-300" href={`/players/${p.playerId}`}>
                    {p.name}
                  </Link>
                ) : (
                  <span className="text-slate-500">Jogador não identificado</span>
                )}
              </td>
              <td className="px-3 py-2 text-slate-500">{p.position ?? "–"}</td>
              <td className="px-3 py-2 font-semibold text-slate-300">{p.appearances}</td>
              <td className="px-3 py-2 text-slate-400">{p.minutesPlayed.toLocaleString("pt-BR")} min</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Página principal ──────────────────────────────────────────────────────────

export default function ClubProfilePage({ params }: { params: Promise<{ clubId: string }> }) {
  const { clubId } = use(params);
  const profile = getMockProfile(clubId);

  const comparisonEntityType = useComparisonStore((s) => s.entityType);
  const selectedIds = useComparisonStore((s) => s.selectedIds);
  const addToComparison = useComparisonStore((s) => s.add);
  const removeFromComparison = useComparisonStore((s) => s.remove);
  const setEntityType = useComparisonStore((s) => s.setEntityType);

  const isSelected = selectedIds.includes(clubId);

  function handleCompare() {
    if (comparisonEntityType !== "team") setEntityType("team");
    if (isSelected) {
      removeFromComparison(clubId);
    } else {
      addToComparison(clubId);
    }
  }

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-5">

        {/* Aviso mock */}
        <div className="flex justify-end">
          <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs font-medium text-amber-300">
            ⚠️ Dados ilustrativos
          </span>
        </div>

        {/* S1 — Header */}
        <ClubHeader isSelected={isSelected} onCompare={handleCompare} profile={profile} />

        {/* S3 — KPI cards */}
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <StatMini accent="#10B981" label="Pontos" value={profile.points} />
          <StatMini accent="#1E40AF" label="Aproveitamento" value={`${profile.winRate.toFixed(1)}%`} />
          <StatMini accent="#F97316" label="Gols Marcados" value={profile.goalsFor} />
          <StatMini accent="#06B6D4" label="Gols Sofridos" value={profile.goalsAgainst} />
        </div>

        {/* Linha: Vitórias/Empates/Derrotas */}
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="flex items-center gap-3 rounded-xl border border-emerald-500/20 bg-slate-900/60 px-5 py-3">
            <Trophy className="h-5 w-5 text-emerald-400" />
            <div>
              <p className="text-2xl font-bold text-emerald-300">{profile.wins}</p>
              <p className="text-xs text-slate-500">Vitórias</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border border-amber-500/20 bg-slate-900/60 px-5 py-3">
            <Activity className="h-5 w-5 text-amber-400" />
            <div>
              <p className="text-2xl font-bold text-amber-300">{profile.draws}</p>
              <p className="text-xs text-slate-500">Empates</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border border-red-500/20 bg-slate-900/60 px-5 py-3">
            <AlertCircle className="h-5 w-5 text-red-400" />
            <div>
              <p className="text-2xl font-bold text-red-400">{profile.losses}</p>
              <p className="text-xs text-slate-500">Derrotas</p>
            </div>
          </div>
        </div>

        {/* S2 — Insights */}
        <SectionShell icon={Star} subtitle="Destaques automáticos do clube" title="Insights">
          <InsightsSection teamName={profile.teamName} />
        </SectionShell>

        {/* S4 + S5 — Forma + Casa vs Fora (lado a lado em lg) */}
        <div className="grid gap-5 lg:grid-cols-2">
          <SectionShell icon={BarChart3} subtitle="Últimos 5 jogos" title="Forma Recente">
            <RecentFormSection recentForm={profile.recentForm} />
          </SectionShell>
          <SectionShell icon={ArrowLeftRight} subtitle="Aproveitamento casa vs fora" title="Casa vs Fora">
            <HomeAwaySection away={profile.awayRecord} home={profile.homeRecord} />
          </SectionShell>
        </div>

        {/* S6 — Gols por mês */}
        <SectionShell icon={BarChart3} subtitle="Gols marcados (verde) e sofridos (vermelho) por mês" title="Gols por Mês">
          <GoalsByMonthSection data={profile.monthlyGoals} />
        </SectionShell>

        {/* S8 + S9 — Contribuidores + Disciplina */}
        <div className="grid gap-5 lg:grid-cols-2">
          <SectionShell icon={Swords} subtitle="Por gols e assistências no período" title="Top Contribuidores">
            <TopScorersSection scorers={profile.topScorers} />
          </SectionShell>
          <SectionShell icon={Shield} subtitle="Cartões e faltas — C. Amarelos 80% | C. Vermelhos 30%" title="Disciplina">
            <DisciplineSection fouls={profile.fouls} red={profile.redCards} yellow={profile.yellowCards} />
          </SectionShell>
        </div>

        {/* S10 — Elenco */}
        <SectionShell icon={Users} subtitle="Jogadores utilizados na temporada" title="Elenco Utilizado">
          <SquadSection squad={profile.squad} />
        </SectionShell>

        {/* Link para comparação */}
        <div className="flex justify-center">
          <Link
            className="flex items-center gap-2 rounded-xl border border-slate-600 bg-slate-800/60 px-5 py-2.5 text-sm font-medium text-slate-300 no-underline transition-all hover:border-emerald-500/40 hover:text-emerald-300"
            href="/clubs/compare"
          >
            <GitCompare className="h-4 w-4" />
            Comparar com outro clube
          </Link>
        </div>

      </div>
    </div>
  );
}
