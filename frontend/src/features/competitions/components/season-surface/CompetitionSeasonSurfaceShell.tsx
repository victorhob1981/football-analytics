"use client";

import type { ReactNode } from "react";

import Link from "next/link";

import {
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

type CompetitionSeasonSurfaceShellProps = {
  children: ReactNode;
  context: CompetitionSeasonContext;
  heroDescription?: string;
  heroEyebrow?: string;
  heroSummary?: ReactNode;
  navItems: SeasonSurfaceNavItem[];
  routeCards: SeasonSurfaceRouteCard[];
  tags?: string[];
  title?: string;
};

export function CompetitionSeasonSurfaceShell({
  children,
  context,
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

        <section className="rounded-[1.7rem] border border-[rgba(208,220,236,0.88)] bg-[linear-gradient(180deg,rgba(255,255,255,0.96)_0%,rgba(247,250,254,0.94)_100%)] p-5 shadow-[0_20px_56px_-48px_rgba(17,28,45,0.2)] md:p-6">
          <div className="space-y-4">
            {tags.length > 0 ? (
              <div className="flex flex-wrap items-center gap-2">
                {tags.map((tag) => (
                  <ProfileTag className="bg-white text-[#455468]" key={tag}>
                    {tag}
                  </ProfileTag>
                ))}
              </div>
            ) : null}

            <div className="space-y-2">
              {heroEyebrow ? (
                <p className="text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#5f6f86]">
                  {heroEyebrow}
                </p>
              ) : null}
              <h1 className="max-w-4xl font-[family:var(--font-profile-headline)] text-[2.45rem] font-extrabold tracking-[-0.05em] text-[#111c2d] md:text-[3rem]">
                {title ?? `${context.competitionName} ${context.seasonLabel}`}
              </h1>
              {heroDescription ? (
                <p className="max-w-3xl text-sm/6 text-[#57657a]">{heroDescription}</p>
              ) : null}
            </div>

            {heroSummary ? <div className="border-t border-[rgba(220,229,239,0.86)] pt-4">{heroSummary}</div> : null}
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
