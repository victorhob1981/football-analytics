"use client";

import { useMemo, useState } from "react";

import { Filter, Search } from "lucide-react";
import Link from "next/link";

import { formatMetricValue, getMetric } from "@/config/metrics.registry";
import type { RankingDefinition, RankingSortDirection } from "@/config/ranking.types";
import { useRankingTable } from "@/features/rankings/hooks";
import type { RankingTableRow } from "@/features/rankings/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";

// ── Helpers ───────────────────────────────────────────────────────────────────

function parseMinSample(value: string): number | null {
  const v = value.trim();
  if (!v) return null;
  const n = Number.parseInt(v, 10);
  return Number.isInteger(n) && n >= 0 ? n : null;
}

function resolveEntityHref(entity: RankingDefinition["entity"], id: string): string | null {
  if (entity === "player") return `/players/${id}`;
  if (entity === "team") return `/clubs/${id}`;
  if (entity === "coach") return `/coaches/${id}`;
  return null;
}

function resolveRowMetricValue(row: RankingTableRow, metricKey: string): number | null {
  if (typeof row.metricValue === "number" && Number.isFinite(row.metricValue)) return row.metricValue;
  const fallback = row[metricKey];
  if (typeof fallback === "number" && Number.isFinite(fallback)) return fallback;
  return null;
}

function resolveOutlierState(row: RankingTableRow): { isOutlier: boolean; zScore: number | null } {
  const isOutlier = row.isOutlier === true;
  const zScore = typeof row.outlierZScore === "number" && Number.isFinite(row.outlierZScore) ? row.outlierZScore : null;
  return { isOutlier, zScore };
}

// ── Rank badge ────────────────────────────────────────────────────────────────

function RankBadge({ rank }: { rank: number | null | undefined }) {
  if (!rank) return <span className="text-slate-600">—</span>;
  const gold = rank === 1 ? "bg-yellow-500/20 text-yellow-300 border-yellow-500/40" : "";
  const silver = rank === 2 ? "bg-slate-400/20 text-slate-300 border-slate-400/30" : "";
  const bronze = rank === 3 ? "bg-amber-700/20 text-amber-600 border-amber-700/30" : "";
  const base = "border-slate-700 bg-slate-800 text-slate-500";
  return (
    <span className={`inline-flex h-6 w-6 items-center justify-center rounded-full border text-[10px] font-bold ${gold || silver || bronze || base}`}>
      {rank}
    </span>
  );
}

// ── Skeleton ──────────────────────────────────────────────────────────────────

function TableSkeleton() {
  return (
    <div className="animate-pulse space-y-2">
      {Array.from({ length: 8 }).map((_, i) => (
        // eslint-disable-next-line react/no-array-index-key
        <div className="flex items-center gap-4 rounded-lg border border-slate-700/50 bg-slate-800/40 px-4 py-3" key={i}>
          <div className="h-6 w-6 rounded-full bg-slate-700" />
          <div className="h-4 flex-1 rounded bg-slate-700" />
          <div className="h-5 w-16 rounded bg-slate-700" />
        </div>
      ))}
    </div>
  );
}

// ── RankingTable dark ─────────────────────────────────────────────────────────

type RankingTableProps = { rankingDefinition: RankingDefinition };

export function RankingTable({ rankingDefinition }: RankingTableProps) {
  const [search, setSearch] = useState("");
  const [sortDirection, setSortDirection] = useState<RankingSortDirection>(rankingDefinition.defaultSort);
  const [minSampleInput, setMinSampleInput] = useState(rankingDefinition.minSample?.min?.toString() ?? "");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize] = useState(50);

  const minSampleValue = useMemo(() => parseMinSample(minSampleInput), [minSampleInput]);
  const metric = getMetric(rankingDefinition.metricKey);

  const rankingQuery = useRankingTable(rankingDefinition, {
    localFilters: { search, page: pageIndex + 1, pageSize, sortDirection, minSampleValue },
  });

  const rows = rankingQuery.data?.rows ?? [];
  const totalCount = rankingQuery.pagination?.totalCount ?? rows.length;
  const totalPages = Math.ceil(totalCount / pageSize);

  // Coverage warning
  const hasCoverageWarning = Boolean(rankingDefinition.coverageWarning);

  if (!metric) {
    return (
      <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-xs text-red-400">
        Métrica "{rankingDefinition.metricKey}" não encontrada no metrics registry.
      </div>
    );
  }

  if (rankingQuery.isError && rows.length === 0) {
    return (
      <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-400">
        <p className="font-medium">Falha ao carregar ranking.</p>
        <p className="mt-1 opacity-70">{rankingQuery.error?.message}</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Filtros locais */}
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-1 flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
          <span className="flex items-center gap-1.5"><Search className="h-3 w-3" /> Buscar</span>
          <input
            className="w-full min-w-[160px] rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 placeholder:text-slate-500 focus:border-emerald-400 focus:outline-none focus:ring-2 focus:ring-emerald-500/30"
            onChange={(e) => { setSearch(e.target.value); setPageIndex(0); }}
            placeholder="Buscar por nome"
            type="text"
            value={search}
          />
        </label>

        <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
          <span className="flex items-center gap-1.5"><Filter className="h-3 w-3" /> Ordenação</span>
          <select
            className="rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none"
            onChange={(e) => { setSortDirection(e.target.value as RankingSortDirection); setPageIndex(0); }}
            value={sortDirection}
          >
            <option value="desc">↓ Descendente</option>
            <option value="asc">↑ Ascendente</option>
          </select>
        </label>

        {rankingDefinition.minSample && (
          <label className="flex flex-col gap-1 text-xs font-semibold uppercase tracking-wide text-slate-300">
            <span>Mín. {rankingDefinition.minSample.field}</span>
            <input
              className="w-28 rounded-md border border-slate-600 bg-slate-950/60 px-2.5 py-2 text-sm text-slate-100 focus:border-emerald-400 focus:outline-none"
              min={0}
              onChange={(e) => { setMinSampleInput(e.target.value); setPageIndex(0); }}
              placeholder={rankingDefinition.minSample.min?.toString() ?? ""}
              type="number"
              value={minSampleInput}
            />
          </label>
        )}
      </div>

      {/* Aviso de cobertura */}
      {hasCoverageWarning && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
          ⚠️ {rankingDefinition.coverageWarning}
        </div>
      )}
      {rankingQuery.isPartial && <PartialDataBanner coverage={rankingQuery.coverage} />}
      {rankingQuery.isError && rows.length > 0 && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-300">
          Dados parcialmente carregados: {rankingQuery.error?.message}
        </div>
      )}
      {!rankingQuery.isLoading && (
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500">Cobertura:</span>
          <CoverageBadge coverage={rankingQuery.coverage} />
        </div>
      )}

      {/* Tabela */}
      {rankingQuery.isLoading ? (
        <TableSkeleton />
      ) : rows.length === 0 ? (
        <p className="rounded-xl border border-slate-700 bg-slate-900 py-10 text-center text-sm text-slate-500">
          Nenhum resultado para os filtros atuais.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="border-b border-slate-700/50">
                <th className="w-10 px-3 py-2.5 text-left font-semibold uppercase tracking-wider text-slate-500">#</th>
                <th className="px-3 py-2.5 text-left font-semibold uppercase tracking-wider text-slate-500">
                  {rankingDefinition.entity === "player" ? "Jogador" : rankingDefinition.entity === "team" ? "Clube" : "Entidade"}
                </th>
                <th className="px-3 py-2.5 text-right font-semibold uppercase tracking-wider text-slate-500">
                  {metric.label}
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/30">
              {rows.map((row, i) => {
                const href = resolveEntityHref(rankingDefinition.entity, row.entityId);
                const value = resolveRowMetricValue(row, rankingDefinition.metricKey);
                const isTop3 = (row.rank ?? i + 1) <= 3;
                const { isOutlier, zScore } = resolveOutlierState(row);

                return (
                  <tr
                    className={`transition-colors hover:bg-slate-800/30 ${isTop3 ? "bg-emerald-900/5" : ""}`}
                    key={row.entityId}
                  >
                    <td className="px-3 py-2.5">
                      <RankBadge rank={row.rank ?? i + 1} />
                    </td>
                    <td className="px-3 py-2.5 font-medium">
                      <div className="flex flex-wrap items-center gap-2">
                        {href ? (
                          <Link className="text-slate-200 no-underline hover:text-emerald-300" href={href}>
                            {row.entityName ?? row.entityId}
                          </Link>
                        ) : (
                          <span className="text-slate-200">{row.entityName ?? row.entityId}</span>
                        )}
                        {isOutlier && (
                          <span
                            className="inline-flex items-center gap-1 rounded border border-amber-500/40 bg-amber-500/10 px-1.5 py-0.5 text-[10px] font-bold text-amber-300"
                            title={zScore !== null ? `Valor significativamente acima da média (z-score: ${zScore.toFixed(2)})` : "Valor significativamente acima da média"}
                          >
                            ⚡ Destaque estatístico
                          </span>
                        )}
                      </div>
                    </td>
                    <td className={`px-3 py-2.5 text-right font-bold ${isTop3 ? "text-emerald-300" : "text-slate-200"}`}>
                      {formatMetricValue(rankingDefinition.metricKey, value)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Paginação */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between border-t border-slate-700/50 pt-3">
          <button
            className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
            disabled={pageIndex === 0}
            onClick={() => { setPageIndex((p) => p - 1); }}
            type="button"
          >
            ← Anterior
          </button>
          <span className="text-xs text-slate-400">Página {pageIndex + 1} de {totalPages}</span>
          <button
            className="rounded-md border border-slate-600 bg-slate-800 px-3 py-1.5 text-xs text-slate-300 transition-colors hover:border-emerald-500/60 hover:text-emerald-300 disabled:opacity-40"
            disabled={pageIndex + 1 >= totalPages}
            onClick={() => { setPageIndex((p) => p + 1); }}
            type="button"
          >
            Próxima →
          </button>
        </div>
      )}
    </div>
  );
}
