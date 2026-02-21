type EmptyStateProps = {
  title?: string;
  description?: string;
  actionLabel?: string;
  onAction?: () => void;
  className?: string;
};

export function EmptyState({
  title = "Sem dados para exibir",
  description = "Ajuste os filtros ou tente novamente mais tarde.",
  actionLabel,
  onAction,
  className,
}: EmptyStateProps) {
  const classes = ["rounded-md border border-dashed border-slate-300 bg-slate-50 p-6 text-center", className]
    .filter(Boolean)
    .join(" ");

  return (
    <section className={classes}>
      <h3 className="text-base font-medium text-slate-800">{title}</h3>
      <p className="mt-2 text-sm text-slate-600">{description}</p>
      {actionLabel && onAction ? (
        <button className="mt-4 rounded border border-slate-300 px-3 py-1 text-sm" onClick={onAction} type="button">
          {actionLabel}
        </button>
      ) : null}
    </section>
  );
}
