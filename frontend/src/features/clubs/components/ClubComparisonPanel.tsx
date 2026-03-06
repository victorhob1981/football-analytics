"use client";

import Link from "next/link";

import { useComparisonStore } from "@/shared/stores/comparison.store";

export function ClubComparisonPanel() {
  const entityType = useComparisonStore((state) => state.entityType);
  const selectedIds = useComparisonStore((state) => state.selectedIds);
  const clearSelection = useComparisonStore((state) => state.clear);

  const canCompare = entityType === "team" && selectedIds.length === 2;

  if (!canCompare) {
    return null;
  }

  return (
    <aside className="fixed bottom-5 right-5 z-50">
      <div className="flex items-center gap-2 rounded-xl border border-emerald-500/40 bg-slate-900/95 p-2 shadow-xl shadow-slate-950/60 backdrop-blur">
        <Link
          className="rounded-lg bg-emerald-600 px-4 py-2 text-xs font-semibold text-white no-underline transition-colors hover:bg-emerald-700"
          href="/clubs/compare"
        >
          Comparar clubes selecionados
        </Link>
        <button
          className="rounded-lg border border-slate-600 px-3 py-2 text-xs font-medium text-slate-300 transition-colors hover:border-slate-500 hover:text-slate-100"
          onClick={() => {
            clearSelection();
          }}
          type="button"
        >
          Limpar
        </button>
      </div>
    </aside>
  );
}
