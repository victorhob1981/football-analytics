"use client";

import { useMemo } from "react";

import {
  AlertTriangle,
  ArrowLeftRight,
  BarChart3,
  ChevronLeft,
  GitCompare,
  Repeat2,
  Shield,
  Star,
  Swords,
  Target,
  Timer,
  TrendingUp,
  Users,
} from "lucide-react";
import Link from "next/link";

import {
  PolarAngleAxis,
  PolarGrid,
  Radar,
  RadarChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";

import { formatMetricValue } from "@/config/metrics.registry";
import { InsightFeed, getHighestInsightSeverity } from "@/features/insights/components";
import { usePlayerProfile } from "@/features/players/hooks";
import type { PlayerMatchStatsPoint } from "@/features/players/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useInsights } from "@/shared/hooks/useInsights";
import { useComparisonStore } from "@/shared/stores/comparison.store";
import { formatDate } from "@/shared/utils/formatters";

// ── Posição helpers ───────────────────────────────────────────────────────────

const POSITION_BADGE: Record<string, string> = {
  Goalkeeper: "bg-amber-500/20 text-amber-300 border-amber-500/30",
  Defender: "bg-sky-500/20 text-sky-300 border-sky-500/30",
  Midfielder: "bg-emerald-500/20 text-emerald-300 border-emerald-500/30",
  Forward: "bg-red-500/20 text-red-400 border-red-500/30",
  Attacker: "bg-red-500/20 text-red-400 border-red-500/30",
};

function positionBadge(position?: string | null) {
  const cls = POSITION_BADGE[position ?? ""] ?? "bg-slate-700 text-slate-400 border-slate-600";
  return (
    <span className={`rounded border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ${cls}`}>
      {position ?? "—"}
    </span>
  );
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

// ── Seção 4: Radar por 90min ──────────────────────────────────────────────────

function Per90Radar({ summary }: { summary: Record<string, number | null | undefined> }) {
  const data = [
    { metric: "Gols", player: summary.goals ?? 0, liga: 4 },
    { metric: "Assist.", player: summary.assists ?? 0, liga: 3 },
    { metric: "Chutes", player: summary.shotsTotal ?? 0, liga: 20 },
    { metric: "Precisão %", player: summary.passAccuracyPct ?? 0, liga: 75 },
    { metric: "Rating", player: (summary.rating ?? 0) * 10, liga: 65 },
  ];

  return (
    <div className="h-[240px] w-full">
      <ResponsiveContainer height="100%" width="100%">
        <RadarChart data={data} margin={{ top: 4, right: 24, bottom: 4, left: 24 }}>
          <PolarGrid stroke="#334155" />
          <PolarAngleAxis dataKey="metric" tick={{ fill: "#94A3B8", fontSize: 11 }} />
          <Radar dataKey="liga" fill="#334155" fillOpacity={0.4} name="Média Liga" stroke="#475569" strokeWidth={1.5} />
          <Radar dataKey="player" fill="#10B981" fillOpacity={0.2} name="Jogador" stroke="#10B981" strokeWidth={2.5} />
          <Tooltip
            contentStyle={{ backgroundColor: "#0F172A", border: "1px solid #334155", borderRadius: "8px", color: "#F1F5F9" }}
            itemStyle={{ color: "#F1F5F9" }}
          />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ── Seção 5: Histórico de jogos ───────────────────────────────────────────────

function MatchHistoryTable({ matches }: { matches: PlayerMatchStatsPoint[] }) {
  if (matches.length === 0) {
    return (
      <p className="rounded-lg border border-slate-700/40 bg-slate-800/30 py-6 text-center text-xs text-slate-500">
        Histórico de partidas indisponível no payload atual.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="border-b border-slate-700/50">
            {["Data", "Adversário", "Min", "Gols", "Assist.", "Rating"].map((h) => (
              <th className="px-3 py-2 text-left font-semibold uppercase tracking-wider text-slate-500" key={h}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-700/30">
          {matches.map((m, i) => (
            <tr
              className="cursor-pointer hover:bg-slate-800/30"
              // eslint-disable-next-line react/no-array-index-key
              key={m.matchId ?? i}
              onClick={() => { if (m.matchId) window.location.href = `/matches/${m.matchId}`; }}
            >
              <td className="px-3 py-2 text-slate-400">{formatDate(m.playedAt)}</td>
              <td className="px-3 py-2 font-medium text-slate-200">{m.opponentName ?? "—"}</td>
              <td className="px-3 py-2 text-slate-400">{formatMetricValue("minutes_played", m.minutesPlayed)}</td>
              <td className="px-3 py-2 font-semibold text-emerald-400">{formatMetricValue("goals", m.goals)}</td>
              <td className="px-3 py-2 text-sky-400">{formatMetricValue("assists", m.assists)}</td>
              <td className="px-3 py-2">
                {m.rating !== null && m.rating !== undefined ? (
                  <span className="flex items-center gap-1">
                    <Star className="h-3 w-3 text-amber-400" />
                    <span className="font-semibold text-amber-300">{m.rating.toFixed(1)}</span>
                  </span>
                ) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Skeleton da página ────────────────────────────────────────────────────────

function ProfileSkeleton({ playerId }: { playerId: string }) {
  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-5">
        <LoadingSkeleton className="bg-slate-800" height={160} />
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {[0, 1, 2, 3].map((i) => <LoadingSkeleton className="bg-slate-800" height={96} key={i} />)}
        </div>
        <LoadingSkeleton className="bg-slate-800" height={200} />
      </div>
    </div>
  );
}

// ── Componente principal ──────────────────────────────────────────────────────

type PlayerProfileContentProps = { playerId: string };

export function PlayerProfileContent({ playerId }: PlayerProfileContentProps) {
  const profileQuery = usePlayerProfile(playerId, { includeRecentMatches: true });
  const playerInsightsQuery = useInsights({ entityType: "player", entityId: playerId });

  const comparisonEntityType = useComparisonStore((s) => s.entityType);
  const selectedIds = useComparisonStore((s) => s.selectedIds);
  const addToComparison = useComparisonStore((s) => s.add);
  const removeFromComparison = useComparisonStore((s) => s.remove);
  const setComparisonEntityType = useComparisonStore((s) => s.setEntityType);

  const isSelected = selectedIds.includes(playerId);
  const canAddMore = selectedIds.length < 2;

  function handleCompare() {
    if (comparisonEntityType !== "player") setComparisonEntityType("player");
    if (isSelected) removeFromComparison(playerId);
    else addToComparison(playerId);
  }

  const playerInsights = playerInsightsQuery.data ?? [];
  const highestSeverity = getHighestInsightSeverity(playerInsights);
  const hasFatalInsightsError = playerInsightsQuery.isError && !playerInsightsQuery.data;

  const summaryObj = useMemo(() => {
    if (!profileQuery.data) return null;
    return profileQuery.data.summary as Record<string, number | null | undefined>;
  }, [profileQuery.data]);

  // Loading
  if (profileQuery.isLoading) return <ProfileSkeleton playerId={playerId} />;

  // Fatal error
  if (profileQuery.isError && !profileQuery.data) {
    return (
      <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6">
        <div className="mx-auto max-w-7xl rounded-xl border border-red-500/30 bg-red-500/10 p-6 text-sm text-red-400">
          <p className="font-medium">Falha ao carregar o perfil do jogador.</p>
          <p className="mt-1 opacity-70">{profileQuery.error?.message}</p>
        </div>
      </div>
    );
  }

  // Empty
  if (profileQuery.isEmpty || !profileQuery.data) {
    return (
      <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6">
        <div className="mx-auto max-w-7xl">
          <EmptyState
            className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
            description="Não foi possível encontrar dados para este jogador."
            title="Perfil indisponível"
          />
        </div>
      </div>
    );
  }

  const { player, summary, recentMatches } = profileQuery.data;
  const displayName = player.playerName?.trim().length ? player.playerName : `Jogador ${playerId}`;

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-5">

        {/* S1 — Header */}
        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-6 shadow-lg shadow-slate-950/40 backdrop-blur-sm">
          {/* Breadcrumb */}
          <div className="mb-4 flex items-center gap-2 text-xs text-slate-500">
            <Link className="no-underline hover:text-emerald-300" href="/players">
              <ChevronLeft className="inline h-3 w-3" /> Jogadores
            </Link>
            <span>/</span>
            <span className="text-slate-400">{displayName}</span>
          </div>

          <div className="flex flex-wrap items-start justify-between gap-4">
            <div className="flex items-center gap-4">
              {/* Avatar */}
              <div className="flex h-16 w-16 items-center justify-center rounded-2xl border border-slate-600 bg-slate-800 text-2xl font-black text-slate-300">
                {displayName.charAt(0).toUpperCase()}
              </div>
              <div className="space-y-1.5">
                <h1 className="text-2xl font-bold text-slate-100">{displayName}</h1>
                <div className="flex flex-wrap items-center gap-2">
                  {positionBadge(player.position)}
                  {player.teamName && (
                    <span className="text-xs font-medium text-slate-400">{player.teamName}</span>
                  )}
                  {player.nationality && (
                    <span className="text-xs text-slate-500">{player.nationality}</span>
                  )}
                </div>
              </div>
            </div>

            <button
              className={`flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold transition-all disabled:cursor-not-allowed disabled:opacity-40 ${isSelected
                  ? "border border-emerald-500/40 bg-emerald-500/20 text-emerald-300"
                  : "border border-slate-600 bg-slate-800 text-slate-300 hover:border-emerald-500/40 hover:text-emerald-300"
                }`}
              disabled={!isSelected && !canAddMore}
              onClick={handleCompare}
              type="button"
            >
              <GitCompare className="h-4 w-4" />
              {isSelected ? "Selecionado" : "Comparar"}
            </button>
          </div>
        </section>

        {/* Cobertura */}
        {profileQuery.isPartial && <PartialDataBanner coverage={profileQuery.coverage} />}
        {profileQuery.isError && (
          <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-xs text-amber-300">
            Dados parcialmente carregados: {profileQuery.error?.message}
          </div>
        )}
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500">Cobertura:</span>
          <CoverageBadge coverage={profileQuery.coverage} />
        </div>

        {/* S3 — Resumo da temporada (KPI cards) */}
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <KpiCard accent="#10B981" icon={Users} label="Partidas" value={formatMetricValue("goals", summary.matchesPlayed ?? null)} />
          <KpiCard accent="#06B6D4" icon={Timer} label="Minutos" value={formatMetricValue("minutes_played", summary.minutesPlayed ?? null)} />
          <KpiCard accent="#F97316" icon={Swords} label="Gols" value={formatMetricValue("goals", summary.goals ?? null)} />
          <KpiCard accent="#8B5CF6" icon={TrendingUp} label="Assistências" value={formatMetricValue("assists", summary.assists ?? null)} />
        </div>

        {/* Métricas secundárias */}
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="flex items-center gap-3 rounded-xl border border-slate-700/50 bg-slate-900/60 px-4 py-3">
            <Target className="h-5 w-5 text-orange-400" />
            <div>
              <p className="text-lg font-bold text-orange-300">{summary.shotsTotal ?? "—"}</p>
              <p className="text-[10px] text-slate-500">Chutes totais</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border border-slate-700/50 bg-slate-900/60 px-4 py-3">
            <Star className="h-5 w-5 text-amber-400" />
            <div>
              <p className="text-lg font-bold text-amber-300">
                {summary.rating !== null && summary.rating !== undefined
                  ? (summary.rating as number).toFixed(1)
                  : "—"}
              </p>
              <p className="text-[10px] text-slate-500">Rating médio</p>
            </div>
          </div>
          <div className="flex items-center gap-3 rounded-xl border border-slate-700/50 bg-slate-900/60 px-4 py-3">
            <ArrowLeftRight className="h-5 w-5 text-sky-400" />
            <div>
              <p className="text-lg font-bold text-sky-300">
                {summary.passAccuracyPct !== null && summary.passAccuracyPct !== undefined
                  ? `${(summary.passAccuracyPct as number).toFixed(1)}%`
                  : "—"}
              </p>
              <p className="text-[10px] text-slate-500">Precisão de passes</p>
            </div>
          </div>
        </div>

        {/* S2 — Insights */}
        <SectionShell icon={AlertTriangle} subtitle={`${playerInsights.length} insights disponíveis`} title="Insights do Jogador">
          {playerInsightsQuery.isLoading && (
            <div className="space-y-2">
              <LoadingSkeleton className="bg-slate-800" height={64} />
              <LoadingSkeleton className="bg-slate-800" height={64} />
            </div>
          )}
          {!playerInsightsQuery.isLoading && hasFatalInsightsError && (
            <p className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-xs text-red-400">
              Falha ao carregar insights: {playerInsightsQuery.error?.message}
            </p>
          )}
          {!playerInsightsQuery.isLoading && !hasFatalInsightsError && (
            <>
              {playerInsightsQuery.isPartial && <PartialDataBanner coverage={playerInsightsQuery.coverage} />}
              {playerInsightsQuery.isEmpty ? (
                <p className="py-4 text-center text-xs text-slate-500">
                  Sem insights para este jogador no recorte atual.
                </p>
              ) : (
                <InsightFeed insights={playerInsights} title="" />
              )}
            </>
          )}
        </SectionShell>

        {/* S4 — Radar por 90min vs média da liga */}
        <SectionShell
          icon={BarChart3}
          subtitle="Métricas do jogador (emerald) vs média da liga (slate) — estimativa ilustrativa"
          title="Eficiência — Radar de Métricas"
        >
          {summaryObj ? <Per90Radar summary={summaryObj} /> : (
            <p className="py-4 text-center text-xs text-slate-500">Métricas por 90 min indisponíveis.</p>
          )}
        </SectionShell>

        {/* S5 — Histórico de jogos */}
        <SectionShell icon={Shield} subtitle="Últimas partidas com gols, assistências e rating" title="Histórico de Jogos">
          <MatchHistoryTable matches={recentMatches ?? []} />
        </SectionShell>

        {/* S6 — Participação em lineups (placeholder informativo) */}
        <SectionShell icon={Users} subtitle="Titular vs banco vs ausente" title="Participação em Lineups">
          <div className="rounded-lg border border-slate-700/40 bg-slate-800/30 px-4 py-3 text-xs text-slate-500">
            Dados de escalação requerem integração com <code className="font-mono text-slate-400">mart.fact_fixture_lineups</code> — em desenvolvimento.
          </div>
        </SectionShell>

        {/* S7 — Eventos de impacto (placeholder) */}
        <SectionShell icon={Swords} subtitle="Gols marcados, cartões, substituições" title="Eventos de Impacto">
          <div className="rounded-lg border border-slate-700/40 bg-slate-800/30 px-4 py-3 text-xs text-slate-500">
            Eventos individuais requerem integração com <code className="font-mono text-slate-400">mart.fact_match_events</code> — em desenvolvimento.
          </div>
        </SectionShell>

        {/* S8 — Transferências (placeholder) */}
        <SectionShell icon={Repeat2} subtitle="Histórico de clubes" title="Transferências">
          <div className="rounded-lg border border-slate-700/40 bg-slate-800/30 px-4 py-3 text-xs text-slate-500">
            Histórico de transferências requer integração com <code className="font-mono text-slate-400">raw.player_transfers</code> — em desenvolvimento.
          </div>
        </SectionShell>

        {/* Link para comparação */}
        {selectedIds.length > 0 && comparisonEntityType === "player" && (
          <div className="flex justify-center">
            <Link
              className="flex items-center gap-2 rounded-xl bg-emerald-600 px-5 py-2.5 text-sm font-semibold text-white no-underline transition-colors hover:bg-emerald-700"
              href="/players/compare"
            >
              <GitCompare className="h-4 w-4" />
              Comparar ({selectedIds.length}/2)
            </Link>
          </div>
        )}

      </div>
    </div>
  );
}
