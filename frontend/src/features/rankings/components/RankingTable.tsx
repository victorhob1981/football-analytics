"use client";

import { useMemo, useState } from "react";

import Link from "next/link";

import type { ColumnDef } from "@tanstack/react-table";

import { getMetric, formatMetricValue } from "@/config/metrics.registry";
import type { RankingDefinition, RankingSortDirection } from "@/config/ranking.types";
import { useRankingTable } from "@/features/rankings/hooks";
import type { RankingTableRow } from "@/features/rankings/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import { DataTable } from "@/shared/components/data-display/DataTable";

type RankingTableProps = {
  rankingDefinition: RankingDefinition;
};

function parseMinSample(value: string): number | null {
  const normalizedValue = value.trim();

  if (normalizedValue.length === 0) {
    return null;
  }

  const parsedValue = Number.parseInt(normalizedValue, 10);

  if (!Number.isInteger(parsedValue) || parsedValue < 0) {
    return null;
  }

  return parsedValue;
}

function resolveEntityHref(entity: RankingDefinition["entity"], entityId: string): string | null {
  if (entity === "player") {
    return `/players/${entityId}`;
  }

  if (entity === "team") {
    return `/clubs/${entityId}`;
  }

  if (entity === "coach") {
    return `/coaches/${entityId}`;
  }

  return null;
}

function resolveRowMetricValue(row: RankingTableRow, metricKey: string): number | null {
  if (typeof row.metricValue === "number" && Number.isFinite(row.metricValue)) {
    return row.metricValue;
  }

  const fallbackValue = row[metricKey];

  if (typeof fallbackValue === "number" && Number.isFinite(fallbackValue)) {
    return fallbackValue;
  }

  return null;
}

export function RankingTable({ rankingDefinition }: RankingTableProps) {
  const [search, setSearch] = useState("");
  const [sortDirection, setSortDirection] = useState<RankingSortDirection>(rankingDefinition.defaultSort);
  const [minSampleInput, setMinSampleInput] = useState(rankingDefinition.minSample?.min?.toString() ?? "");
  const minSampleValue = useMemo(() => parseMinSample(minSampleInput), [minSampleInput]);

  const metric = getMetric(rankingDefinition.metricKey);
  const rankingQuery = useRankingTable(rankingDefinition, {
    localFilters: {
      search,
      sortDirection,
      minSampleValue,
    },
  });

  const rows = rankingQuery.data?.rows ?? [];

  const columns = useMemo<Array<ColumnDef<RankingTableRow, unknown>>>(
    () => [
      {
        accessorFn: (row) => row.rank ?? null,
        id: "rank",
        header: "#",
        cell: ({ row }) => row.original.rank ?? "-",
      },
      {
        accessorFn: (row) => row.entityName ?? row.entityId,
        id: "entity",
        header: rankingDefinition.entity === "player" ? "Jogador" : "Entidade",
        cell: ({ row }) => {
          const entityName = row.original.entityName ?? row.original.entityId;
          const entityHref = resolveEntityHref(rankingDefinition.entity, row.original.entityId);

          if (!entityHref) {
            return entityName;
          }

          return (
            <Link className="font-medium text-slate-900 hover:underline" href={entityHref}>
              {entityName}
            </Link>
          );
        },
      },
      {
        accessorFn: (row) => resolveRowMetricValue(row, rankingDefinition.metricKey),
        id: "metric",
        header: metric?.label ?? rankingDefinition.metricKey,
        cell: ({ row }) => {
          const metricValue = resolveRowMetricValue(row.original, rankingDefinition.metricKey);
          return formatMetricValue(rankingDefinition.metricKey, metricValue);
        },
      },
    ],
    [metric?.label, rankingDefinition.entity, rankingDefinition.metricKey],
  );

  if (!metric) {
    return (
      <main className="space-y-3">
        <h1 className="text-xl font-semibold">{rankingDefinition.label}</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          Ranking invalido: metricKey &quot;{rankingDefinition.metricKey}&quot; nao encontrado no metrics registry.
        </section>
      </main>
    );
  }

  if (rankingQuery.isError && rows.length === 0) {
    return (
      <main className="space-y-3">
        <h1 className="text-xl font-semibold">{rankingDefinition.label}</h1>
        <section className="rounded-md border border-rose-300 bg-rose-50 p-4 text-sm text-rose-700">
          <p>Falha ao carregar ranking.</p>
          <p>{rankingQuery.error?.message}</p>
        </section>
      </main>
    );
  }

  return (
    <main className="space-y-4">
      <header className="space-y-1">
        <h1 className="text-xl font-semibold">{rankingDefinition.label}</h1>
        <p className="text-sm text-slate-600">{rankingDefinition.description}</p>
      </header>

      <section className="grid gap-3 rounded-md border border-slate-200 bg-white p-4 md:grid-cols-3">
        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Buscar entidade
          <input
            className="rounded border border-slate-300 px-2 py-1"
            onChange={(event) => {
              setSearch(event.target.value);
            }}
            placeholder="Buscar por nome"
            type="text"
            value={search}
          />
        </label>

        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Ordenacao
          <select
            className="rounded border border-slate-300 bg-white px-2 py-1"
            onChange={(event) => {
              setSortDirection(event.target.value as RankingSortDirection);
            }}
            value={sortDirection}
          >
            <option value="desc">Descendente</option>
            <option value="asc">Ascendente</option>
          </select>
        </label>

        <label className="flex flex-col gap-1 text-sm text-slate-700">
          Minimo de amostra
          <input
            className="rounded border border-slate-300 px-2 py-1"
            onChange={(event) => {
              setMinSampleInput(event.target.value);
            }}
            placeholder={rankingDefinition.minSample?.min?.toString() ?? "Opcional"}
            type="number"
            value={minSampleInput}
          />
        </label>
      </section>

      <section className="rounded-md border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
        RankingMetricSelector: placeholder (na rota atual o rankingType ja define a metrica).
      </section>

      {rankingQuery.isError ? (
        <section className="rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-800">
          Dados carregados com alerta: {rankingQuery.error?.message}
        </section>
      ) : null}

      {rankingQuery.isPartial ? <PartialDataBanner coverage={rankingQuery.coverage} /> : null}

      {!rankingQuery.isLoading ? (
        <div className="flex items-center gap-2">
          <span className="text-sm text-slate-600">Status de cobertura:</span>
          <CoverageBadge coverage={rankingQuery.coverage} />
        </div>
      ) : null}

      <DataTable<RankingTableRow>
        columns={columns}
        data={rows}
        emptyDescription="Nao ha linhas para os filtros atuais."
        emptyTitle="Ranking vazio"
        enableVirtualization
        initialPageSize={50}
        loading={rankingQuery.isLoading}
        pageSizeOptions={[25, 50, 100]}
        virtualizerMaxHeight={520}
      />
    </main>
  );
}
