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
  const classes = [
    "rounded-[1.45rem] border border-dashed border-[rgba(191,201,195,0.62)] bg-[rgba(255,255,255,0.72)] p-6 text-center shadow-[0_22px_60px_-50px_rgba(17,28,45,0.18)]",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <section className={classes}>
      <h3 className="font-[family:var(--font-profile-headline)] text-xl font-extrabold text-[#111c2d]">
        {title}
      </h3>
      <p className="mt-2 text-sm leading-6 text-[#57657a]">{description}</p>
      {actionLabel && onAction ? (
        <button
          className="mt-4 inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
          onClick={onAction}
          type="button"
        >
          {actionLabel}
        </button>
      ) : null}
    </section>
  );
}
