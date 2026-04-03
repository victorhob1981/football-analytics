import Link from "next/link";

import type { MatchTimelineEvent } from "@/features/matches/types";
import { PartialDataBanner } from "@/shared/components/coverage/PartialDataBanner";
import {
  ProfileCoveragePill,
  ProfilePanel,
  ProfileTag,
} from "@/shared/components/profile/ProfilePrimitives";
import type { CoverageState } from "@/shared/types/coverage.types";
import type {
  CompetitionSeasonContext,
  CompetitionSeasonContextInput,
} from "@/shared/types/context.types";
import {
  appendFilterQueryString,
  buildCanonicalPlayerPath,
  buildCanonicalTeamPath,
  buildPlayerResolverPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";

type MatchTimelinePlaceholderProps = {
  events: MatchTimelineEvent[] | undefined;
  coverage?: CoverageState;
  competitionContext?: CompetitionSeasonContext | null;
  contextInput?: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  };
  homeTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamId?: string | null;
  awayTeamName?: string | null;
};

function resolveMinuteLabel(event: MatchTimelineEvent): string {
  if (typeof event.minute === "number" && Number.isFinite(event.minute)) {
    return `${event.minute}'`;
  }

  return "Min ?";
}

function resolveEventTitle(event: MatchTimelineEvent): string {
  const playerName = event.playerName?.trim();
  const detail = event.detail?.trim();
  const type = event.type?.trim();

  if (playerName && detail) {
    return `${playerName} · ${detail}`;
  }

  if (playerName) {
    return playerName;
  }

  if (detail) {
    return detail;
  }

  return type?.length ? type : "Evento sem detalhe";
}

function resolveEventMeta(event: MatchTimelineEvent): string {
  return (
    [event.teamName?.trim(), event.type?.trim()].filter(Boolean).join(" · ") || "Sem meta adicional"
  );
}

function resolveEventTone(type: string | null | undefined): string {
  const normalizedType = type?.trim().toLowerCase();

  if (normalizedType?.includes("goal")) {
    return "bg-[#003526] text-white";
  }

  if (normalizedType?.includes("red")) {
    return "bg-[#93000a] text-white";
  }

  if (normalizedType?.includes("yellow")) {
    return "bg-[#ffdcc3] text-[#6e3900]";
  }

  return "bg-[rgba(216,227,251,0.72)] text-[#404944]";
}

function resolveTeamScope(
  event: MatchTimelineEvent,
  homeTeamId?: string | null,
  homeTeamName?: string | null,
  awayTeamId?: string | null,
  awayTeamName?: string | null,
): string {
  if (event.teamId && homeTeamId && event.teamId === homeTeamId) {
    return homeTeamName?.trim() || "Mandante";
  }

  if (event.teamId && awayTeamId && event.teamId === awayTeamId) {
    return awayTeamName?.trim() || "Visitante";
  }

  return event.teamName?.trim() || "Sem time identificado";
}

function resolvePlayerHref(
  playerId: string | null | undefined,
  competitionContext: CompetitionSeasonContext | null,
  contextInput: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  },
): string | null {
  if (!playerId) {
    return null;
  }

  if (!competitionContext) {
    return buildPlayerResolverPath(playerId, contextInput);
  }

  return appendFilterQueryString(
    buildCanonicalPlayerPath(competitionContext, playerId),
    contextInput,
    ["competitionId", "seasonId"],
  );
}

function resolveTeamHref(
  teamId: string | null | undefined,
  competitionContext: CompetitionSeasonContext | null,
  contextInput: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  },
): string | null {
  if (!teamId) {
    return null;
  }

  if (!competitionContext) {
    return buildTeamResolverPath(teamId, contextInput);
  }

  return appendFilterQueryString(
    buildCanonicalTeamPath(competitionContext, teamId),
    contextInput,
    ["competitionId", "seasonId"],
  );
}

export function MatchTimelinePlaceholder({
  events,
  coverage = { status: "unknown", label: "Timeline" },
  competitionContext = null,
  contextInput = {},
  homeTeamId,
  homeTeamName,
  awayTeamId,
  awayTeamName,
}: MatchTimelinePlaceholderProps) {
  const timelineEvents = [...(events ?? [])].sort((left, right) => {
    const leftMinute = typeof left.minute === "number" ? left.minute : Number.POSITIVE_INFINITY;
    const rightMinute = typeof right.minute === "number" ? right.minute : Number.POSITIVE_INFINITY;

    if (leftMinute !== rightMinute) {
      return leftMinute - rightMinute;
    }

    const leftSecond = typeof left.second === "number" ? left.second : Number.POSITIVE_INFINITY;
    const rightSecond = typeof right.second === "number" ? right.second : Number.POSITIVE_INFINITY;
    return leftSecond - rightSecond;
  });

  return (
    <ProfilePanel className="space-y-5">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Linha do tempo
          </p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
            Linha do tempo da partida
          </h2>
          <p className="mt-2 text-sm leading-6 text-[#57657a]">
            Lance a lance da partida, com atalhos para o jogador e o time quando estiverem disponíveis.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <ProfileCoveragePill coverage={coverage} />
          <ProfileTag>{timelineEvents.length} eventos</ProfileTag>
          {timelineEvents.length > 0 ? (
            <ProfileTag>{resolveMinuteLabel(timelineEvents[timelineEvents.length - 1])}</ProfileTag>
          ) : null}
        </div>
      </div>

      <PartialDataBanner
        coverage={coverage}
        message="Alguns lances ainda nao chegaram completos para esta partida."
      />

      {timelineEvents.length === 0 ? (
        <div className="rounded-[1.2rem] border border-dashed border-[rgba(112,121,116,0.32)] bg-[rgba(240,243,255,0.78)] px-4 py-5 text-sm text-[#57657a]">
          Nenhum lance registrado para esta partida.
        </div>
      ) : (
        <ol className="space-y-3">
          {timelineEvents.map((event, index) => (
            <li
              className="grid gap-4 rounded-[1.35rem] bg-[rgba(240,243,255,0.82)] px-4 py-4 md:grid-cols-[auto_minmax(0,1fr)]"
              key={event.eventId ?? `${event.type ?? "event"}-${event.minute ?? index}-${index}`}
            >
              <div
                className={`inline-flex h-11 w-11 items-center justify-center rounded-full text-xs font-bold uppercase tracking-[0.08em] ${resolveEventTone(event.type)}`}
              >
                {resolveMinuteLabel(event)}
              </div>
              <div className="space-y-2">
                <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                  <div>
                    {resolvePlayerHref(event.playerId, competitionContext, contextInput) ? (
                      <Link
                        className="text-sm font-semibold text-[#111c2d] transition-colors hover:text-[#00513b]"
                        href={resolvePlayerHref(event.playerId, competitionContext, contextInput)!}
                      >
                        {resolveEventTitle(event)}
                      </Link>
                    ) : (
                      <p className="text-sm font-semibold text-[#111c2d]">
                        {resolveEventTitle(event)}
                      </p>
                    )}
                    <p className="mt-1 text-xs uppercase tracking-[0.16em] text-[#57657a]">
                      {resolveEventMeta(event)}
                    </p>
                  </div>
                  {resolveTeamHref(event.teamId, competitionContext, contextInput) ? (
                    <Link
                      href={resolveTeamHref(event.teamId, competitionContext, contextInput)!}
                    >
                      <ProfileTag>
                        {resolveTeamScope(
                          event,
                          homeTeamId,
                          homeTeamName,
                          awayTeamId,
                          awayTeamName,
                        )}
                      </ProfileTag>
                    </Link>
                  ) : (
                    <ProfileTag>
                      {resolveTeamScope(event, homeTeamId, homeTeamName, awayTeamId, awayTeamName)}
                    </ProfileTag>
                  )}
                </div>
              </div>
            </li>
          ))}
        </ol>
      )}
    </ProfilePanel>
  );
}
