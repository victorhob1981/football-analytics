"use client";

import Link from "next/link";

import { usePlatformShellState } from "@/shared/components/navigation/usePlatformShellState";

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

export function PlatformShellFrame() {
  const shellState = usePlatformShellState();

  return (
    <section
      aria-label="Contexto da página"
      className="border-b border-[rgba(216,227,251,0.72)] bg-[linear-gradient(180deg,rgba(255,255,255,0.88)_0%,rgba(246,249,255,0.84)_100%)] shadow-[0_14px_34px_-34px_rgba(17,28,45,0.18)] backdrop-blur-xl"
    >
      <div className="mx-auto w-full max-w-7xl px-6 py-4 md:px-8">
        <div className="rounded-[1.7rem] border border-white/72 bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(243,247,255,0.88)_100%)] p-5 shadow-[0_24px_58px_-44px_rgba(17,28,45,0.18)]">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
            <div className="min-w-0 space-y-4">
              <div className="flex flex-wrap items-center gap-2 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                {shellState.breadcrumbs.map((breadcrumb, index) => (
                  <span className="inline-flex items-center gap-2" key={`${breadcrumb.label}-${index}`}>
                    {index > 0 ? <span className="text-[#8fa097]">/</span> : null}
                    {breadcrumb.href ? (
                      <Link className="transition-colors hover:text-[#003526]" href={breadcrumb.href}>
                        {breadcrumb.label}
                      </Link>
                    ) : (
                      <span>{breadcrumb.label}</span>
                    )}
                  </span>
                ))}
              </div>

              <div className="space-y-2">
                <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
                  {shellState.surfaceLabel}
                </p>
                <p className="max-w-4xl font-[family:var(--font-profile-headline)] text-[2rem] font-extrabold tracking-[-0.045em] text-[#111c2d] md:text-[2.45rem]">
                  {shellState.surfaceTitle}
                </p>
                <p className="max-w-3xl text-sm/6 text-[#57657a]">{shellState.description}</p>
              </div>
            </div>

            <div className="grid gap-3 xl:min-w-[18rem] xl:max-w-[22rem]">
              {shellState.scopeTags.length > 0 ? (
                <div className="rounded-[1.35rem] border border-[rgba(216,227,251,0.8)] bg-white/78 px-4 py-4">
                  <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                    Recorte ativo
                  </p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {shellState.scopeTags.map((tag) => (
                      <span
                        className="inline-flex items-center rounded-full bg-[rgba(216,227,251,0.76)] px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
                        key={tag}
                      >
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>
              ) : null}

              {shellState.helperText ? (
                <div className="rounded-[1.35rem] border border-[rgba(191,201,195,0.55)] bg-[rgba(240,243,255,0.72)] px-4 py-4">
                  <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                    Como navegar
                  </p>
                  <p className="mt-2 text-sm/6 text-[#57657a]">{shellState.helperText}</p>
                </div>
              ) : null}
            </div>
          </div>

          {shellState.surfaceLinks.length > 0 ? (
            <div className="mt-5 border-t border-[rgba(216,227,251,0.76)] pt-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Navegação desta superfície
              </p>
              <nav aria-label="Atalhos da página" className="mt-3 flex flex-wrap items-center gap-2">
                {shellState.surfaceLinks.map((link) => (
                  <Link
                    className={joinClasses(
                      "inline-flex items-center rounded-full px-4 py-2 text-[0.72rem] font-semibold uppercase tracking-[0.16em] transition-[transform,background-color,color] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] active:scale-[0.985]",
                      link.isActive
                        ? "bg-[#003526] text-white shadow-[0_20px_42px_-30px_rgba(0,53,38,0.48)]"
                        : "bg-[rgba(240,243,255,0.96)] text-[#1f2d40] hover:-translate-y-0.5 hover:bg-white",
                    )}
                    href={link.href}
                    key={link.href}
                  >
                    {link.label}
                  </Link>
                ))}
              </nav>
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}
