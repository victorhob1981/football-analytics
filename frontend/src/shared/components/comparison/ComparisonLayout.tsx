import type { ReactNode } from "react";

type ComparisonColumn = {
  title: string;
  subtitle?: string;
  content?: ReactNode;
};

type ComparisonLayoutProps = {
  title?: string;
  description?: string;
  left: ComparisonColumn;
  right: ComparisonColumn;
  className?: string;
  emptyLabel?: string;
};

function renderColumn(column: ComparisonColumn, emptyLabel: string) {
  return (
    <article className="rounded-md border border-slate-200 bg-white p-4">
      <header className="mb-3">
        <h3 className="text-base font-semibold text-slate-900">{column.title}</h3>
        {column.subtitle ? <p className="text-sm text-slate-600">{column.subtitle}</p> : null}
      </header>

      <div className="min-h-[120px]">
        {column.content ? (
          column.content
        ) : (
          <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">{emptyLabel}</p>
        )}
      </div>
    </article>
  );
}

export function ComparisonLayout({
  title = "Comparativo",
  description,
  left,
  right,
  className,
  emptyLabel = "Sem dados para esta entidade.",
}: ComparisonLayoutProps) {
  const classes = ["space-y-3", className].filter(Boolean).join(" ");

  return (
    <section className={classes}>
      <header>
        <h2 className="text-lg font-semibold text-slate-900">{title}</h2>
        {description ? <p className="text-sm text-slate-600">{description}</p> : null}
      </header>

      <div className="grid gap-3 md:grid-cols-2">
        {renderColumn(left, emptyLabel)}
        {renderColumn(right, emptyLabel)}
      </div>
    </section>
  );
}
