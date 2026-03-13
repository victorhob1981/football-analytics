"use client";

import { useState } from "react";

import {
  Activity,
  AlertCircle,
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock,
  Database,
  RefreshCw,
  Shield,
  XCircle,
} from "lucide-react";

// ── Tipos e dados mock ────────────────────────────────────────────────────────

interface ModuleCoverage {
  id: string;
  label: string;
  table: string;
  coverage: number;
  total: number;
  covered: number;
  roundBreakdown: Array<{ round: number; coverage: number; covered: number; total: number }>;
}

interface SyncState {
  entity: string;
  table: string;
  lastSync: string;
  status: "ok" | "atraso" | "erro";
  volume: number;
  lagMinutes: number;
}

interface MetricIssue {
  metric: string;
  table: string;
  coverage: number;
  impact: string;
  recommendation: string;
  severity: "critical" | "warning" | "info";
}

interface RawCoverageRow {
  developerName: string;
  coverage: number;
  modules: string[];
  status: "materializado" | "pendente";
}

const MODULES: ModuleCoverage[] = [
  {
    id: "events", label: "Eventos de Partida", table: "mart.fact_match_events",
    coverage: 72, total: 380, covered: 274,
    roundBreakdown: [
      { round: 1, coverage: 95, covered: 19, total: 20 },
      { round: 2, coverage: 90, covered: 18, total: 20 },
      { round: 3, coverage: 85, covered: 17, total: 20 },
      { round: 4, coverage: 75, covered: 15, total: 20 },
      { round: 5, coverage: 65, covered: 13, total: 20 },
      { round: 6, coverage: 50, covered: 10, total: 20 },
    ],
  },
  {
    id: "player_stats", label: "Stats de Jogadores", table: "mart.fact_fixture_player_stats",
    coverage: 88, total: 380, covered: 334,
    roundBreakdown: [
      { round: 1, coverage: 100, covered: 20, total: 20 },
      { round: 2, coverage: 95, covered: 19, total: 20 },
      { round: 3, coverage: 90, covered: 18, total: 20 },
      { round: 4, coverage: 85, covered: 17, total: 20 },
      { round: 5, coverage: 80, covered: 16, total: 20 },
      { round: 6, coverage: 72, covered: 14, total: 20 },
    ],
  },
  {
    id: "lineups", label: "Escalações", table: "mart.fact_fixture_lineups",
    coverage: 94, total: 380, covered: 357,
    roundBreakdown: [
      { round: 1, coverage: 100, covered: 20, total: 20 },
      { round: 2, coverage: 100, covered: 20, total: 20 },
      { round: 3, coverage: 95, covered: 19, total: 20 },
      { round: 4, coverage: 90, covered: 18, total: 20 },
      { round: 5, coverage: 90, covered: 18, total: 20 },
      { round: 6, coverage: 90, covered: 18, total: 20 },
    ],
  },
  {
    id: "standings", label: "Standings", table: "mart.fact_standings_snapshots",
    coverage: 98, total: 380, covered: 372,
    roundBreakdown: [
      { round: 1, coverage: 100, covered: 20, total: 20 },
      { round: 2, coverage: 100, covered: 20, total: 20 },
      { round: 3, coverage: 100, covered: 20, total: 20 },
      { round: 4, coverage: 100, covered: 20, total: 20 },
      { round: 5, coverage: 90, covered: 18, total: 20 },
      { round: 6, coverage: 97, covered: 19, total: 20 },
    ],
  },
  {
    id: "team_stats", label: "Stats de Times", table: "raw.match_statistics",
    coverage: 41, total: 380, covered: 156,
    roundBreakdown: [
      { round: 1, coverage: 60, covered: 12, total: 20 },
      { round: 2, coverage: 55, covered: 11, total: 20 },
      { round: 3, coverage: 45, covered: 9, total: 20 },
      { round: 4, coverage: 35, covered: 7, total: 20 },
      { round: 5, coverage: 25, covered: 5, total: 20 },
      { round: 6, coverage: 20, covered: 4, total: 20 },
    ],
  },
];

const SYNC_STATE: SyncState[] = [
  { entity: "Fixtures", table: "raw.fixtures", lastSync: "2025-11-22 18:30", status: "ok", volume: 380, lagMinutes: 2 },
  { entity: "Player Stats", table: "raw.fixture_player_statistics", lastSync: "2025-11-22 18:28", status: "ok", volume: 8420, lagMinutes: 4 },
  { entity: "Lineups", table: "raw.fixture_lineups", lastSync: "2025-11-22 18:25", status: "ok", volume: 7600, lagMinutes: 7 },
  { entity: "Standings", table: "raw.standings", lastSync: "2025-11-22 15:00", status: "atraso", volume: 760, lagMinutes: 210 },
  { entity: "Player Transfers", table: "raw.player_transfers", lastSync: "2025-11-21 09:00", status: "atraso", volume: 1240, lagMinutes: 1400 },
  { entity: "Sidelined", table: "raw.team_sidelined", lastSync: "2025-11-20 12:00", status: "erro", volume: 0, lagMinutes: 2880 },
  { entity: "Team Statistics", table: "raw.match_statistics", lastSync: "2025-11-22 10:00", status: "atraso", volume: 156, lagMinutes: 510 },
  { entity: "Source Sync", table: "raw.provider_sync_state", lastSync: "2025-11-22 18:35", status: "ok", volume: 12, lagMinutes: 0 },
];

const METRIC_ISSUES: MetricIssue[] = [
  {
    metric: "clean_sheets",
    table: "mart.player_season_summary",
    coverage: 0,
    impact: "Filtro por goleiro quebrado na lista de jogadores",
    recommendation: "Aguardar modelagem de clean_sheets no pipeline dbt",
    severity: "critical",
  },
  {
    metric: "xg / xg_per_90",
    table: "mart.player_90_metrics",
    coverage: 0,
    impact: "Radar de desempenho sem xG; comparativo limitado",
    recommendation: "xG indisponível na fonte ativa; manter fallback de métricas sem xG",
    severity: "critical",
  },
  {
    metric: "player_id (lineups)",
    table: "mart.fact_fixture_lineups",
    coverage: 36,
    impact: "49 slots de escalação sem player_id — impossível linkar ao perfil",
    recommendation: "Executar reconciliação interna para mapear player_id ausente",
    severity: "warning",
  },
  {
    metric: "team_performance_monthly",
    table: "mart.team_performance_monthly",
    coverage: 0,
    impact: "Seção de gols/mês totalmente sem dados no perfil de clube",
    recommendation: "Executar modelo dbt mart.team_performance_monthly com seed de datas",
    severity: "warning",
  },
  {
    metric: "red_cards (player)",
    table: "mart.player_season_summary",
    coverage: 28,
    impact: "Disciplina de jogadores subestimada",
    recommendation: "Verificar mapeamento de event_type no pipeline de eventos",
    severity: "info",
  },
];

const RAW_COVERAGE: RawCoverageRow[] = [
  { developerName: "goals", coverage: 98, modules: ["player_stats", "events"], status: "materializado" },
  { developerName: "assists", coverage: 96, modules: ["player_stats"], status: "materializado" },
  { developerName: "minutes_played", coverage: 95, modules: ["player_stats", "lineups"], status: "materializado" },
  { developerName: "shots_total", coverage: 88, modules: ["player_stats"], status: "materializado" },
  { developerName: "shots_on_target", coverage: 86, modules: ["player_stats"], status: "materializado" },
  { developerName: "pass_accuracy_pct", coverage: 82, modules: ["player_stats"], status: "materializado" },
  { developerName: "yellow_cards", coverage: 75, modules: ["player_stats", "discipline"], status: "materializado" },
  { developerName: "red_cards", coverage: 28, modules: ["player_stats", "discipline"], status: "materializado" },
  { developerName: "rating", coverage: 71, modules: ["player_stats"], status: "materializado" },
  { developerName: "fouls_committed", coverage: 60, modules: ["discipline"], status: "pendente" },
  { developerName: "key_passes", coverage: 45, modules: ["player_stats"], status: "pendente" },
  { developerName: "duels_won", coverage: 40, modules: ["player_stats"], status: "pendente" },
  { developerName: "xg", coverage: 0, modules: ["player_stats", "comparison"], status: "pendente" },
  { developerName: "clean_sheets", coverage: 0, modules: ["player_stats"], status: "pendente" },
];

// ── Helpers ───────────────────────────────────────────────────────────────────

function coverageColor(pct: number): string {
  if (pct >= 80) return "text-emerald-300";
  if (pct >= 50) return "text-amber-300";
  return "text-red-400";
}

function coverageBg(pct: number): string {
  if (pct >= 80) return "bg-emerald-500";
  if (pct >= 50) return "bg-amber-500";
  return "bg-red-500";
}

function coverageBorder(pct: number): string {
  if (pct >= 80) return "border-emerald-500/30 bg-emerald-500/10";
  if (pct >= 50) return "border-amber-500/30 bg-amber-500/10";
  return "border-red-500/30 bg-red-500/10";
}

function statusIcon(status: SyncState["status"]) {
  if (status === "ok") return <CheckCircle2 className="h-4 w-4 text-emerald-400" />;
  if (status === "atraso") return <AlertTriangle className="h-4 w-4 text-amber-400" />;
  return <XCircle className="h-4 w-4 text-red-400" />;
}

function statusBadge(status: SyncState["status"]) {
  const map: Record<SyncState["status"], string> = {
    ok: "border-emerald-500/30 bg-emerald-500/10 text-emerald-300",
    atraso: "border-amber-500/30 bg-amber-500/10 text-amber-300",
    erro: "border-red-500/30 bg-red-500/10 text-red-400",
  };
  return <span className={`rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase ${map[status]}`}>{status}</span>;
}

function severityIcon(severity: MetricIssue["severity"]) {
  if (severity === "critical") return <XCircle className="h-4 w-4 text-red-400" />;
  if (severity === "warning") return <AlertTriangle className="h-4 w-4 text-amber-400" />;
  return <AlertCircle className="h-4 w-4 text-sky-400" />;
}

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

// ── S1: Módulo card expansível ────────────────────────────────────────────────

function ModuleCard({ module }: { module: ModuleCoverage }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className={`rounded-xl border ${coverageBorder(module.coverage)} overflow-hidden`}>
      <button
        className="flex w-full items-center gap-4 p-4 text-left transition-colors hover:bg-slate-800/20"
        onClick={() => { setExpanded((x) => !x); }}
        type="button"
      >
        {/* Ícone de expansão */}
        <span className="text-slate-500">
          {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
        </span>

        {/* Nome e tabela */}
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-slate-100">{module.label}</p>
          <p className="font-mono text-[10px] text-slate-500">{module.table}</p>
        </div>

        {/* Barra de cobertura */}
        <div className="w-32 shrink-0">
          <div className="mb-1 flex justify-between text-[10px]">
            <span className="text-slate-500">{module.covered}/{module.total}</span>
            <span className={`font-bold ${coverageColor(module.coverage)}`}>{module.coverage}%</span>
          </div>
          <div className="h-2 w-full overflow-hidden rounded-full bg-slate-800">
            <div className={`h-full rounded-full ${coverageBg(module.coverage)}`} style={{ width: `${module.coverage}%` }} />
          </div>
        </div>
      </button>

      {/* Detalhe por rodada */}
      {expanded && (
        <div className="border-t border-slate-700/50 bg-slate-800/20 px-4 py-3">
          <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">Cobertura por rodada</p>
          <div className="space-y-1.5">
            {module.roundBreakdown.map((r) => (
              <div className="flex items-center gap-3" key={r.round}>
                <span className="w-10 text-[10px] text-slate-500">Rod. {r.round}</span>
                <div className="flex-1 overflow-hidden rounded-full bg-slate-800 h-1.5">
                  <div className={`h-full rounded-full ${coverageBg(r.coverage)}`} style={{ width: `${r.coverage}%` }} />
                </div>
                <span className={`w-10 text-right text-[10px] font-bold ${coverageColor(r.coverage)}`}>{r.coverage}%</span>
                <span className="w-14 text-right text-[10px] text-slate-600">{r.covered}/{r.total}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Página ────────────────────────────────────────────────────────────────────

export default function AuditPage() {
  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-5xl space-y-6">

        {/* Header */}
        <header className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <span className="text-xs font-semibold uppercase tracking-wider text-amber-400">Painel Interno</span>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-100">Auditoria de Cobertura</h1>
            <p className="mt-1 text-sm text-slate-400">Visibilidade completa sobre qualidade e disponibilidade dos dados</p>
          </div>
          <div className="flex items-center gap-2 rounded-xl border border-slate-700/50 bg-slate-800/50 px-3 py-2 text-xs text-slate-400">
            <RefreshCw className="h-3.5 w-3.5" />
            TTL: 5 min · Última atualização: 18:35
          </div>
        </header>

        {/* S1 — Cobertura por módulo */}
        <SectionShell icon={Database} subtitle="Clique em um módulo para ver o detalhamento por rodada" title="Cobertura por Módulo">
          {/* Mini KPIs */}
          <div className="mb-4 grid grid-cols-3 gap-3 sm:grid-cols-5">
            {MODULES.map((m) => (
              <div className="rounded-lg border border-slate-700/40 bg-slate-800/30 p-2 text-center" key={m.id}>
                <p className={`text-lg font-black ${coverageColor(m.coverage)}`}>{m.coverage}%</p>
                <p className="text-[9px] text-slate-500 mt-0.5 leading-tight">{m.label}</p>
              </div>
            ))}
          </div>

          <div className="space-y-2">
            {MODULES.map((m) => <ModuleCard key={m.id} module={m} />)}
          </div>
        </SectionShell>

        {/* S2 — Estado de sincronização */}
        <SectionShell icon={Activity} subtitle="Estado atual de cada entidade de ingestão" title="Estado de Sincronização">
          {/* Indicadores de lag */}
          <div className="mb-4 flex flex-wrap gap-2">
            {(["ok", "atraso", "erro"] as const).map((s) => {
              const count = SYNC_STATE.filter((x) => x.status === s).length;
              return (
                <div className="flex items-center gap-1.5 rounded-lg border border-slate-700/40 bg-slate-800/30 px-3 py-1.5" key={s}>
                  {statusIcon(s)}
                  <span className="text-xs font-semibold text-slate-300 capitalize">{s}</span>
                  <span className="text-xs text-slate-500">({count})</span>
                </div>
              );
            })}
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b border-slate-700/50">
                  {["Entidade", "Tabela", "Último Sync", "Status", "Volume", "Lag"].map((h) => (
                    <th className="whitespace-nowrap px-3 py-2.5 text-left font-semibold uppercase tracking-wider text-slate-500" key={h}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/30">
                {SYNC_STATE.map((s) => (
                  <tr className="hover:bg-slate-800/20" key={s.entity}>
                    <td className="whitespace-nowrap px-3 py-2.5 font-medium text-slate-200">{s.entity}</td>
                    <td className="px-3 py-2.5 font-mono text-[10px] text-slate-500">{s.table}</td>
                    <td className="whitespace-nowrap px-3 py-2.5 text-slate-400">{s.lastSync}</td>
                    <td className="px-3 py-2.5">{statusBadge(s.status)}</td>
                    <td className="px-3 py-2.5 text-slate-400">{s.volume.toLocaleString()}</td>
                    <td className={`px-3 py-2.5 font-semibold ${s.lagMinutes > 120 ? "text-red-400" : s.lagMinutes > 30 ? "text-amber-400" : "text-emerald-400"}`}>
                      {s.lagMinutes < 60
                        ? `${s.lagMinutes}min`
                        : `${Math.round(s.lagMinutes / 60)}h`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="mt-3 rounded-lg border border-slate-600/30 bg-slate-800/20 px-3 py-2 text-[10px] text-slate-500">
            <code>raw.provider_entity_map</code> mantido para uso futuro; reconciliação cross-source está desativada.
          </div>
        </SectionShell>

        {/* S3 — Cobertura de métricas problemáticas */}
        <SectionShell icon={AlertCircle} subtitle="Métricas com cobertura baixa e impacto nos módulos do produto" title="Métricas Problemáticas">
          <div className="space-y-3">
            {METRIC_ISSUES.map((m) => (
              <div
                className={`rounded-lg border p-4 ${m.severity === "critical" ? "border-red-500/30 bg-red-500/5" : m.severity === "warning" ? "border-amber-500/30 bg-amber-500/5" : "border-sky-500/20 bg-sky-500/5"}`}
                key={m.metric}
              >
                <div className="flex items-start gap-3">
                  {severityIcon(m.severity)}
                  <div className="flex-1 min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <code className="font-mono text-sm font-semibold text-slate-100">{m.metric}</code>
                      <span className={`text-xs font-bold ${coverageColor(m.coverage)}`}>{m.coverage}%</span>
                      <span className="font-mono text-[10px] text-slate-500">{m.table}</span>
                    </div>
                    <p className="mt-1 text-xs text-slate-400"><span className="font-semibold text-slate-300">Impacto:</span> {m.impact}</p>
                    <p className="mt-0.5 text-[10px] text-slate-500"><span className="font-semibold">Ação:</span> {m.recommendation}</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </SectionShell>

        {/* S4 — Cobertura do JSON bruto de player statistics */}
        <SectionShell
          icon={Shield}
          subtitle="Campos extraídos de raw.fixture_player_statistics — cobertura e status de materialização no mart"
          title="Cobertura de Métricas Brutas"
        >
          <div className="overflow-x-auto">
            <table className="min-w-full text-xs">
              <thead>
                <tr className="border-b border-slate-700/50">
                  {["Campo", "Cobertura", "Módulos Afetados", "Status"].map((h) => (
                    <th className="whitespace-nowrap px-3 py-2.5 text-left font-semibold uppercase tracking-wider text-slate-500" key={h}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/30">
                {RAW_COVERAGE.map((r) => (
                  <tr className="hover:bg-slate-800/20" key={r.developerName}>
                    <td className="px-3 py-2.5 font-mono text-slate-200">{r.developerName}</td>
                    <td className="px-3 py-2.5">
                      <div className="flex items-center gap-2">
                        <div className="h-1.5 w-20 overflow-hidden rounded-full bg-slate-800">
                          <div className={`h-full rounded-full ${coverageBg(r.coverage)}`} style={{ width: `${r.coverage}%` }} />
                        </div>
                        <span className={`font-bold ${coverageColor(r.coverage)}`}>{r.coverage}%</span>
                      </div>
                    </td>
                    <td className="px-3 py-2.5">
                      <div className="flex flex-wrap gap-1">
                        {r.modules.map((mod) => (
                          <span className="rounded bg-slate-700/60 px-1.5 py-0.5 text-[9px] text-slate-400" key={mod}>{mod}</span>
                        ))}
                      </div>
                    </td>
                    <td className="px-3 py-2.5">
                      <span className={`rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase ${r.status === "materializado"
                          ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
                          : "border-slate-600 bg-slate-800 text-slate-500"
                        }`}>
                        {r.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SectionShell>

      </div>
    </div>
  );
}
