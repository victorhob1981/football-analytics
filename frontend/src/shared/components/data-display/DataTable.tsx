"use client";

import { useMemo, useRef, useState } from "react";

import {
  flexRender,
  functionalUpdate,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  type ColumnDef,
  type PaginationState,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";

type DataTableProps<TData extends object> = {
  data: TData[];
  columns: Array<ColumnDef<TData, unknown>>;
  loading?: boolean;
  className?: string;
  initialPageSize?: number;
  pageSizeOptions?: number[];
  emptyTitle?: string;
  emptyDescription?: string;
  enableVirtualization?: boolean;
  virtualizerMaxHeight?: number;
  virtualizerEstimateSize?: number;
  virtualizerOverscan?: number;
  manualPagination?: boolean;
  pageIndex?: number;
  pageSize?: number;
  totalCount?: number;
  onPageChange?: (pageIndex: number) => void;
  onPageSizeChange?: (pageSize: number) => void;
};

function normalizePageSizeOptions(pageSizeOptions: number[], fallbackPageSize: number): number[] {
  const normalizedOptions = Array.from(
    new Set(pageSizeOptions.filter((pageSize) => Number.isInteger(pageSize) && pageSize > 0)),
  );

  if (normalizedOptions.length === 0) {
    return [fallbackPageSize];
  }

  return normalizedOptions.sort((a, b) => a - b);
}

export function DataTable<TData extends object>({
  data,
  columns,
  loading = false,
  className,
  initialPageSize = 10,
  pageSizeOptions = [10, 20, 50],
  emptyTitle,
  emptyDescription,
  enableVirtualization = false,
  virtualizerMaxHeight = 480,
  virtualizerEstimateSize = 44,
  virtualizerOverscan = 6,
  manualPagination = false,
  pageIndex,
  pageSize,
  totalCount,
  onPageChange,
  onPageSizeChange,
}: DataTableProps<TData>) {
  const safeInitialPageSize = Number.isInteger(initialPageSize) && initialPageSize > 0 ? initialPageSize : 10;
  const fallbackPageSize = Number.isInteger(pageSize) && (pageSize ?? 0) > 0 ? (pageSize as number) : safeInitialPageSize;
  const resolvedPageSizeOptions = useMemo(() => {
    const options = normalizePageSizeOptions(pageSizeOptions, fallbackPageSize);
    if (manualPagination && Number.isInteger(pageSize) && (pageSize ?? 0) > 0 && !options.includes(pageSize as number)) {
      return [...options, pageSize as number].sort((a, b) => a - b);
    }
    return options;
  }, [fallbackPageSize, manualPagination, pageSize, pageSizeOptions]);

  const [sorting, setSorting] = useState<SortingState>([]);
  const [localPagination, setLocalPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: resolvedPageSizeOptions.includes(safeInitialPageSize) ? safeInitialPageSize : resolvedPageSizeOptions[0],
  });
  const virtualizerScrollRef = useRef<HTMLDivElement>(null);
  const effectivePagination: PaginationState = manualPagination
    ? {
        pageIndex: Number.isInteger(pageIndex) && (pageIndex ?? 0) >= 0 ? (pageIndex as number) : 0,
        pageSize: Number.isInteger(pageSize) && (pageSize ?? 0) > 0 ? (pageSize as number) : resolvedPageSizeOptions[0],
      }
    : localPagination;
  const resolvedTotalCount = manualPagination ? Math.max(totalCount ?? data.length, 0) : data.length;
  const resolvedPageCount = Math.max(Math.ceil(resolvedTotalCount / Math.max(effectivePagination.pageSize, 1)), 1);

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      pagination: effectivePagination,
    },
    onSortingChange: setSorting,
    onPaginationChange: (updater) => {
      const nextPagination = functionalUpdate(updater, effectivePagination);
      if (!manualPagination) {
        setLocalPagination(nextPagination);
        return;
      }
      if (nextPagination.pageSize !== effectivePagination.pageSize) {
        onPageSizeChange?.(nextPagination.pageSize);
        onPageChange?.(0);
        return;
      }
      if (nextPagination.pageIndex !== effectivePagination.pageIndex) {
        onPageChange?.(nextPagination.pageIndex);
      }
    },
    manualPagination,
    pageCount: manualPagination ? resolvedPageCount : undefined,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  });

  const tableRows = table.getRowModel().rows;
  const visibleColumnsCount = Math.max(table.getVisibleLeafColumns().length, 1);
  const shouldVirtualize = enableVirtualization && tableRows.length > 0;
  const currentPage = effectivePagination.pageIndex + 1;
  const canPreviousPage = manualPagination ? effectivePagination.pageIndex > 0 : table.getCanPreviousPage();
  const canNextPage = manualPagination ? currentPage < resolvedPageCount : table.getCanNextPage();

  const rowVirtualizer = useVirtualizer({
    count: shouldVirtualize ? tableRows.length : 0,
    getScrollElement: () => virtualizerScrollRef.current,
    estimateSize: () => virtualizerEstimateSize,
    overscan: virtualizerOverscan,
  });

  const virtualRows = shouldVirtualize ? rowVirtualizer.getVirtualItems() : [];
  const virtualPaddingTop = shouldVirtualize && virtualRows.length > 0 ? virtualRows[0].start : 0;
  const virtualPaddingBottom =
    shouldVirtualize && virtualRows.length > 0
      ? rowVirtualizer.getTotalSize() - virtualRows[virtualRows.length - 1].end
      : 0;

  const classes = ["overflow-hidden rounded-lg border border-slate-200 bg-white", className].filter(Boolean).join(" ");

  if (loading) {
    return (
      <section className={classes}>
        <div className="space-y-3 p-4">
          <LoadingSkeleton height={20} />
          <LoadingSkeleton height={20} />
          <LoadingSkeleton height={20} />
          <LoadingSkeleton height={20} />
        </div>
      </section>
    );
  }

  if (data.length === 0) {
    return (
      <section className={classes}>
        <div className="p-4">
          <EmptyState description={emptyDescription} title={emptyTitle} />
        </div>
      </section>
    );
  }

  return (
    <section className={classes}>
      <div className="overflow-x-auto">
        <div
          ref={virtualizerScrollRef}
          className={shouldVirtualize ? "overflow-y-auto" : undefined}
          style={shouldVirtualize ? { maxHeight: `${virtualizerMaxHeight}px` } : undefined}
        >
          <table className="min-w-full border-collapse">
            <thead className="bg-slate-50">
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const canSort = header.column.getCanSort();
                    const sortedState = header.column.getIsSorted();

                    return (
                      <th
                        className="border-b border-slate-200 px-3 py-2 text-left text-sm font-medium text-slate-700"
                        key={header.id}
                      >
                        {header.isPlaceholder ? null : (
                          <button
                            className={`inline-flex items-center gap-1 ${canSort ? "cursor-pointer select-none" : "cursor-default"}`}
                            onClick={canSort ? header.column.getToggleSortingHandler() : undefined}
                            type="button"
                          >
                            {flexRender(header.column.columnDef.header, header.getContext())}
                            {sortedState === "asc" ? <span aria-hidden="true">^</span> : null}
                            {sortedState === "desc" ? <span aria-hidden="true">v</span> : null}
                          </button>
                        )}
                      </th>
                    );
                  })}
                </tr>
              ))}
            </thead>
            <tbody>
              {shouldVirtualize && virtualPaddingTop > 0 ? (
                <tr aria-hidden="true">
                  <td colSpan={visibleColumnsCount} style={{ height: `${virtualPaddingTop}px` }} />
                </tr>
              ) : null}

              {shouldVirtualize
                ? virtualRows.map((virtualRow) => {
                    const row = tableRows[virtualRow.index];

                    return (
                      <tr className="border-b border-slate-100 last:border-b-0" key={`${row.id}-${virtualRow.index}`}>
                        {row.getVisibleCells().map((cell) => (
                          <td className="px-3 py-2 text-sm text-slate-700" key={cell.id}>
                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                          </td>
                        ))}
                      </tr>
                    );
                  })
                : tableRows.map((row) => (
                    <tr className="border-b border-slate-100 last:border-b-0" key={row.id}>
                      {row.getVisibleCells().map((cell) => (
                        <td className="px-3 py-2 text-sm text-slate-700" key={cell.id}>
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </td>
                      ))}
                    </tr>
                  ))}

              {shouldVirtualize && virtualPaddingBottom > 0 ? (
                <tr aria-hidden="true">
                  <td colSpan={visibleColumnsCount} style={{ height: `${virtualPaddingBottom}px` }} />
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <footer className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-200 bg-slate-50 px-3 py-2 text-sm">
        <div className="flex items-center gap-2">
          <button
            className="rounded border border-slate-300 px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50"
            disabled={!canPreviousPage}
            onClick={() => {
              if (manualPagination) {
                onPageChange?.(Math.max(effectivePagination.pageIndex - 1, 0));
                return;
              }
              table.previousPage();
            }}
            type="button"
          >
            Anterior
          </button>
          <button
            className="rounded border border-slate-300 px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50"
            disabled={!canNextPage}
            onClick={() => {
              if (manualPagination) {
                onPageChange?.(Math.min(effectivePagination.pageIndex + 1, resolvedPageCount - 1));
                return;
              }
              table.nextPage();
            }}
            type="button"
          >
            Proximo
          </button>
        </div>

        <span className="text-slate-600">
          Pagina {currentPage} de {manualPagination ? resolvedPageCount : table.getPageCount()}
        </span>

        <label className="flex items-center gap-2 text-slate-600">
          Linhas
          <select
            className="rounded border border-slate-300 bg-white px-2 py-1"
            onChange={(event) => {
              const nextPageSize = Number(event.target.value);
              if (manualPagination) {
                onPageSizeChange?.(nextPageSize);
                onPageChange?.(0);
                return;
              }
              table.setPageSize(nextPageSize);
            }}
            value={effectivePagination.pageSize}
          >
            {resolvedPageSizeOptions.map((pageSizeOption) => (
              <option key={pageSizeOption} value={pageSizeOption}>
                {pageSizeOption}
              </option>
            ))}
          </select>
        </label>
      </footer>
    </section>
  );
}
