import type { ReactNode } from "react";

type HomeSectionShellProps = {
  title: string;
  subtitle?: string;
  children: ReactNode;
  className?: string;
  action?: ReactNode;
};

export function HomeSectionShell({
  title,
  subtitle,
  children,
  className,
  action,
}: HomeSectionShellProps) {
  const base = [
    "rounded-xl border border-slate-700/80 bg-slate-900/80 p-6 shadow-lg shadow-slate-950/30 backdrop-blur-sm",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <section className={base}>
      <header className="mb-4 flex items-start justify-between gap-3">
        <div className="space-y-1">
          <h2 className="text-lg font-semibold text-slate-100">{title}</h2>
          {subtitle ? <p className="text-sm text-slate-400">{subtitle}</p> : null}
        </div>
        {action ? <div className="shrink-0">{action}</div> : null}
      </header>
      {children}
    </section>
  );
}
