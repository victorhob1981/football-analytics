import Link from "next/link";

import type { MatchLineupPlayer } from "@/features/matches/types";
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
  buildPlayerResolverPath,
} from "@/shared/utils/context-routing";

type MatchLineupsPlaceholderProps = {
  coverage?: CoverageState;
  competitionContext?: CompetitionSeasonContext | null;
  contextInput?: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  };
  lineups: MatchLineupPlayer[] | undefined;
  homeTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamId?: string | null;
  awayTeamName?: string | null;
};

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

function renderPlayerMeta(player: MatchLineupPlayer): string {
  return [
    player.position?.trim(),
    typeof player.shirtNumber === "number" ? `#${player.shirtNumber}` : null,
    typeof player.formationPosition === "number" ? `posição ${player.formationPosition}` : null,
  ]
    .filter(Boolean)
    .join(" · ");
}

function resolveLineupSortValue(player: MatchLineupPlayer): number {
  if (typeof player.formationPosition === "number" && Number.isFinite(player.formationPosition)) {
    return player.formationPosition;
  }

  if (typeof player.shirtNumber === "number" && Number.isFinite(player.shirtNumber)) {
    return player.shirtNumber + 100;
  }

  return Number.POSITIVE_INFINITY;
}

function TeamRosterColumn({
  title,
  lineup,
  competitionContext,
  contextInput,
}: {
  title: string;
  lineup: MatchLineupPlayer[];
  competitionContext: CompetitionSeasonContext | null;
  contextInput: CompetitionSeasonContextInput & {
    roundId?: string | null;
    venue?: string | null;
    lastN?: number | null;
    dateRangeStart?: string | null;
    dateRangeEnd?: string | null;
  };
}) {
  const starters = [...lineup]
    .filter((player) => player.isStarter)
    .sort((left, right) => resolveLineupSortValue(left) - resolveLineupSortValue(right));
  const bench = [...lineup]
    .filter((player) => !player.isStarter)
    .sort((left, right) => resolveLineupSortValue(left) - resolveLineupSortValue(right));
  const hasFormationData = starters.some(
    (player) => player.formationField || typeof player.formationPosition === "number",
  );

  const renderRosterList = (players: MatchLineupPlayer[], emptyLabel: string) => {
    if (players.length === 0) {
      return (
        <div className="rounded-[1rem] border border-dashed border-[rgba(112,121,116,0.32)] bg-white/70 px-4 py-4 text-sm text-[#57657a]">
          {emptyLabel}
        </div>
      );
    }

    return (
      <ul className="space-y-2">
        {players.map((player, index) => {
          const playerName = player.playerName?.trim() || "Jogador sem nome";
          const playerHref = resolvePlayerHref(player.playerId, competitionContext, contextInput);
          const meta = renderPlayerMeta(player);

          return (
            <li
              className="rounded-[1rem] bg-white/75 px-4 py-3"
              key={`${player.playerId ?? `slot-${index}`}-${player.teamId ?? title}`}
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  {playerHref ? (
                    <Link
                      className="text-sm font-semibold text-[#111c2d] transition-colors hover:text-[#00513b]"
                      href={playerHref}
                    >
                      {playerName}
                    </Link>
                  ) : (
                    <p className="text-sm font-semibold text-[#111c2d]">{playerName}</p>
                  )}
                  {meta ? (
                    <p className="mt-1 text-xs uppercase tracking-[0.16em] text-[#57657a]">
                      {meta}
                    </p>
                  ) : null}
                </div>
                <ProfileTag>{player.isStarter ? "Titular" : "Banco"}</ProfileTag>
              </div>
            </li>
          );
        })}
      </ul>
    );
  };

  return (
    <section className="space-y-4 rounded-[1.35rem] bg-[rgba(240,243,255,0.82)] p-4">
      <div className="flex items-end justify-between gap-3">
        <div>
          <h3 className="font-[family:var(--font-profile-headline)] text-xl font-extrabold text-[#111c2d]">
            {title}
          </h3>
          <p className="mt-1 text-xs uppercase tracking-[0.16em] text-[#57657a]">
            {lineup.length} atletas retornados
          </p>
        </div>
        <div className="flex items-center gap-2">
          {hasFormationData ? <ProfileTag>Posições da formação</ProfileTag> : null}
          <ProfileTag>{starters.length} titulares</ProfileTag>
          <ProfileTag>{bench.length} banco</ProfileTag>
        </div>
      </div>

      <div className="space-y-3">
        <div>
          <p className="mb-2 text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Titulares
          </p>
          {renderRosterList(starters, "Sem titulares retornados para este time.")}
        </div>

        <div>
          <p className="mb-2 text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Banco
          </p>
          {renderRosterList(bench, "Sem banco retornado para este time.")}
        </div>
      </div>
    </section>
  );
}

export function MatchLineupsPlaceholder({
  coverage = { status: "unknown", label: "Escalações" },
  competitionContext = null,
  contextInput = {},
  lineups,
  homeTeamId,
  homeTeamName,
  awayTeamId,
  awayTeamName,
}: MatchLineupsPlaceholderProps) {
  const lineupItems = lineups ?? [];
  const homeLineup = lineupItems.filter(
    (player) => player.teamId && homeTeamId && player.teamId === homeTeamId,
  );
  const awayLineup = lineupItems.filter(
    (player) => player.teamId && awayTeamId && player.teamId === awayTeamId,
  );
  const unassignedLineup = lineupItems.filter((player) => {
    if (!player.teamId) {
      return true;
    }

    if (homeTeamId && player.teamId === homeTeamId) {
      return false;
    }

    if (awayTeamId && player.teamId === awayTeamId) {
      return false;
    }

    return true;
  });

  return (
    <ProfilePanel className="space-y-5">
      <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Escalações
          </p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-2xl font-extrabold text-[#111c2d]">
            Escalações da partida
          </h2>
          <p className="mt-2 text-sm leading-6 text-[#57657a]">
            Separação por time, com titulares, banco, posição e camisa quando esses dados estiverem disponíveis.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <ProfileCoveragePill coverage={coverage} />
          <ProfileTag>{lineupItems.length} atletas listados</ProfileTag>
        </div>
      </div>

      <PartialDataBanner
        coverage={coverage}
        message="Algumas informações de escalação ainda chegaram incompletas para esta partida."
      />

      {lineupItems.length === 0 ? (
        <div className="rounded-[1.2rem] border border-dashed border-[rgba(112,121,116,0.32)] bg-[rgba(240,243,255,0.78)] px-4 py-5 text-sm text-[#57657a]">
          Nenhuma escalação confirmada para esta partida.
        </div>
      ) : (
        <div className="grid gap-4 xl:grid-cols-2">
          <TeamRosterColumn
            competitionContext={competitionContext}
            contextInput={contextInput}
            lineup={homeLineup}
            title={homeTeamName?.trim() || "Mandante"}
          />
          <TeamRosterColumn
            competitionContext={competitionContext}
            contextInput={contextInput}
            lineup={awayLineup}
            title={awayTeamName?.trim() || "Visitante"}
          />
          {unassignedLineup.length > 0 ? (
            <div className="xl:col-span-2">
              <TeamRosterColumn
                competitionContext={competitionContext}
                contextInput={contextInput}
                lineup={unassignedLineup}
                title="Sem time identificado"
              />
            </div>
          ) : null}
        </div>
      )}
    </ProfilePanel>
  );
}
