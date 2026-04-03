"use client";

import type { ReactNode } from "react";

import Link from "next/link";

import {
  ProfilePanel,
  ProfileShell,
  ProfileTag,
  ProfileTabs,
} from "@/shared/components/profile/ProfilePrimitives";
import { ProfileRouteCard } from "@/shared/components/profile/ProfileRouteCard";
import { CanonicalRouteContextSync } from "@/shared/components/routing/CanonicalRouteContextSync";
import type { CompetitionSeasonContext } from "@/shared/types/context.types";
import { buildCompetitionHubPath } from "@/shared/utils/context-routing";

type SeasonSurfaceNavItem = {
  badge?: ReactNode;
  href: string;
  isActive: boolean;
  key: string;
  label: ReactNode;
};

type SeasonSurfaceRouteCard = {
  description: string;
  href: string;
  label: string;
  title: string;
};

type SeasonSurfaceHeroAction = {
  href: string;
  label: string;
  tone?: "primary" | "secondary";
};

type CompetitionSeasonSurfaceShellProps = {
  children: ReactNode;
  context: CompetitionSeasonContext;
  heroActions?: SeasonSurfaceHeroAction[];
  heroAside?: ReactNode;
  heroDescription: string;
  heroEyebrow: string;
  heroSummary?: ReactNode;
  navItems: SeasonSurfaceNavItem[];
  routeCards: SeasonSurfaceRouteCard[];
  tags?: string[];
  title?: string;
};

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

export function CompetitionSeasonSurfaceShell({
  children,
  context,
  heroActions = [],
  heroAside,
  heroDescription,
  heroEyebrow,
  heroSummary,
  navItems,
  routeCards,
  tags = [],
  title,
}: CompetitionSeasonSurfaceShellProps) {
  return (
    <CanonicalRouteContextSync context={context}>
      <ProfileShell className="space-y-6">
        <div className="flex flex-wrap items-center gap-2 text-[0.78rem] font-semibold uppercase tracking-[0.16em] text-[#455468]">
          <Link className="transition-colors hover:text-[#003526]" href="/competitions">
            Competicoes
          </Link>
          <span className="text-[#8fa097]">/</span>
          <Link
            className="transition-colors hover:text-[#003526]"
            href={buildCompetitionHubPath(context.competitionKey)}
          >
            {context.competitionName}
          </Link>
          <span className="text-[#8fa097]">/</span>
          <span>{context.seasonLabel}</span>
        </div>

        <section className="relative isolate overflow-hidden rounded-[2rem] border border-white/65 bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(246,249,255,0.96)_46%,rgba(243,247,243,0.94)_100%)] p-5 shadow-[0_28px_88px_-58px_rgba(17,28,45,0.26)] md:p-6 xl:p-8">
          <div className="pointer-events-none absolute inset-x-0 top-0 h-44 bg-[radial-gradient(circle_at_top_left,rgba(216,227,251,0.72),transparent_52%),radial-gradient(circle_at_top_right,rgba(139,214,182,0.2),transparent_40%)]" />

          <div className="relative grid gap-5 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.86fr)]">
            <div className="space-y-5">
              {tags.length > 0 ? (
                <div className="flex flex-wrap items-center gap-2">
                  {tags.map((tag) => (
                    <ProfileTag className="bg-white text-[#455468]" key={tag}>
                      {tag}
                    </ProfileTag>
                  ))}
                </div>
              ) : null}

              <div className="space-y-3">
                <p className="text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#0a3d2c]">
                  {heroEyebrow}
                </p>
                <h1 className="max-w-4xl font-[family:var(--font-profile-headline)] text-[2.65rem] font-extrabold tracking-[-0.05em] text-[#111c2d] md:text-[3.35rem]">
                  {title ?? `${context.competitionName} ${context.seasonLabel}`}
                </h1>
                <p className="max-w-3xl text-sm/7 text-[#57657a] md:text-[0.98rem]">
                  {heroDescription}
                </p>
              </div>

              <div className="flex flex-wrap gap-2">
                {heroActions.map((action) => (
                  <Link
                    className={joinClasses(
                      "inline-flex items-center rounded-full px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] transition-[transform,background-color,border-color,box-shadow] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] active:scale-[0.985]",
                      action.tone === "secondary"
                        ? "border border-[rgba(112,121,116,0.24)] bg-white/88 text-[#455468] hover:-translate-y-0.5 hover:border-[#8bd6b6] hover:bg-white"
                        : "bg-[#003526] text-white hover:-translate-y-0.5 hover:bg-[#004631] hover:shadow-[0_20px_42px_-28px_rgba(0,53,38,0.55)]",
                    )}
                    href={action.href}
                    key={`${action.label}-${action.href}`}
                  >
                    {action.label}
                  </Link>
                ))}
              </div>

              {heroSummary}
            </div>

            {heroAside ?? (
              <ProfilePanel className="flex h-full items-center justify-center text-sm/6 text-[#57657a]" tone="soft">
                Sem painel lateral para esta edicao.
              </ProfilePanel>
            )}
          </div>
        </section>

        <ProfileTabs
          ariaLabel="Navegacao da edicao"
          aside={<ProfileTag key="season-context">{context.seasonLabel}</ProfileTag>}
          items={navItems}
        />

        {children}

        <section className="grid gap-4 xl:grid-cols-4">
          {routeCards.map((card) => (
            <ProfileRouteCard
              description={card.description}
              href={card.href}
              key={`${card.label}-${card.title}-${card.href}`}
              label={card.label}
              title={card.title}
            />
          ))}
        </section>
      </ProfileShell>
    </CanonicalRouteContextSync>
  );
}
