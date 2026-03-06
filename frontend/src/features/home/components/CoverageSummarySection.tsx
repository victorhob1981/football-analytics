"use client";

import Link from "next/link";

import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { formatPercentage } from "@/shared/utils/formatters";
import { useCoverageSummary } from "@/features/home/hooks";
import type { CoverageLevel, CoverageModuleItem } from "@/features/home/types";

const LEVEL_CONFIG: Record<
  CoverageLevel,
  {
    card: string;
    text: string;
    bar: string;
  }
> = {
  high: {
    card: "border-emerald-700 bg-emerald-950/20",
    text: "text-emerald-200",
    bar: "bg-emerald-500",
  },
  partial: {
    card: "border-amber-700 bg-amber-950/20",
    text: "text-amber-200",
    bar: "bg-amber-500",
  },
  low: {
    card: "border-rose-700 bg-rose-950/20",
    text: "text-rose-200",
    bar: "bg-rose-500",
  },
};

function CoverageBar({ module: mod }: { module: CoverageModuleItem }) {
  const cfg = LEVEL_CONFIG[mod.level];
  const pct = Math.max(0, Math.min(100, mod.percentage));

  return (
    <div className={`rounded-lg border p-3 ${cfg.card}`}>
      <div className="flex items-center justify-between gap-2">
        <p className={`text-sm font-medium ${cfg.text}`}>{mod.label}</p>
        <p className={`text-sm font-semibold tabular-nums ${cfg.text}`}>{formatPercentage(pct, 0)}</p>
      </div>

      <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-slate-700">
        <div className={`h-full transition-all duration-500 ${cfg.bar}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function CoverageSkeletons() {
  return (
    <div className="space-y-3">
      {Array.from({ length: 4 }).map((_, i) => (
        <LoadingSkeleton className="bg-slate-700" height={56} key={i} />
      ))}
    </div>
  );
}

export function CoverageSummarySection() {
  const { data, isLoading, isEmpty } = useCoverageSummary();

  if (isLoading) {
    return <CoverageSkeletons />;
  }

  if (isEmpty || !data || data.modules.length === 0) {
    return (
      <EmptyState
        className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
        description="Dados de cobertura nao disponiveis no momento."
        title="Sem dados de cobertura"
      />
    );
  }

  return (
    <div className="space-y-3">
      {data.modules.map((mod) => (
        <CoverageBar key={mod.label} module={mod} />
      ))}
      <div className="text-right">
        <Link
          className="text-xs font-medium text-emerald-300 no-underline hover:text-emerald-200 hover:underline"
          href="/audit"
        >
          Painel completo de auditoria
        </Link>
      </div>
    </div>
  );
}
