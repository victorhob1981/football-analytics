import { useMemo, useState } from "react";

import Link from "next/link";

import type { MatchLineupPlayer } from "@/features/matches/types";

type MatchLineupsPlaceholderProps = {
  lineups: MatchLineupPlayer[] | undefined;
  homeTeamId?: string | null;
  awayTeamId?: string | null;
  homeTeamName?: string | null;
  awayTeamName?: string | null;
};

function splitLineupsByTeam(
  lineups: MatchLineupPlayer[],
  homeTeamId?: string | null,
  awayTeamId?: string | null,
): { homePlayers: MatchLineupPlayer[]; awayPlayers: MatchLineupPlayer[] } {
  if (homeTeamId || awayTeamId) {
    return {
      homePlayers: lineups.filter((player) => player.teamId === homeTeamId),
      awayPlayers: lineups.filter((player) => player.teamId === awayTeamId),
    };
  }

  const pivot = Math.ceil(lineups.length / 2);

  return {
    homePlayers: lineups.slice(0, pivot),
    awayPlayers: lineups.slice(pivot),
  };
}

function parseFormationRow(formationField?: string | null): number | null {
  if (!formationField) {
    return null;
  }

  const trimmed = formationField.trim();
  if (!/^\d+:\d+$/.test(trimmed)) {
    return null;
  }

  const [rowStr] = trimmed.split(":");
  const rowValue = Number(rowStr);
  return Number.isFinite(rowValue) ? rowValue : null;
}

function inferRowByPosition(position?: string | null): number {
  const normalized = (position ?? "").toLowerCase();

  if (normalized.includes("goal")) return 1;
  if (normalized.includes("def")) return 2;
  if (normalized.includes("mid")) return 3;
  if (normalized.includes("forward") || normalized.includes("att")) return 4;

  return 5;
}

function buildFormationRows(players: MatchLineupPlayer[]) {
  const starters = players.filter((player) => player.isStarter);
  const grouped = new Map<number, MatchLineupPlayer[]>();

  starters.forEach((player) => {
    const row = parseFormationRow(player.formationField) ?? inferRowByPosition(player.position);
    const current = grouped.get(row) ?? [];
    current.push(player);
    grouped.set(row, current);
  });

  return Array.from(grouped.entries())
    .sort(([left], [right]) => left - right)
    .map(([row, lineupPlayers]) => ({
      row,
      players: [...lineupPlayers].sort((left, right) => {
        const formationDelta = (left.formationPosition ?? 999) - (right.formationPosition ?? 999);
        if (formationDelta !== 0) return formationDelta;
        return (left.shirtNumber ?? 999) - (right.shirtNumber ?? 999);
      }),
    }));
}

function PlayerName({ player }: { player: MatchLineupPlayer }) {
  if (player.playerId) {
    return (
      <Link className="font-medium text-slate-900 hover:underline" href={`/players/${player.playerId}`}>
        {player.playerName ?? "Jogador"}
      </Link>
    );
  }

  return <span className="font-medium text-slate-700">{player.playerName ?? "Slot vazio"}</span>;
}

export function MatchLineupsPlaceholder({
  lineups,
  homeTeamId,
  awayTeamId,
  homeTeamName,
  awayTeamName,
}: MatchLineupsPlaceholderProps) {
  const lineupItems = useMemo(() => lineups ?? [], [lineups]);
  const [tab, setTab] = useState<"home" | "away">("home");

  const { homePlayers, awayPlayers } = useMemo(
    () => splitLineupsByTeam(lineupItems, homeTeamId, awayTeamId),
    [awayTeamId, homeTeamId, lineupItems],
  );

  const currentPlayers = tab === "home" ? homePlayers : awayPlayers;
  const currentTeamName = tab === "home" ? homeTeamName ?? "Mandante" : awayTeamName ?? "Visitante";
  const starters = currentPlayers.filter((player) => player.isStarter);
  const bench = currentPlayers.filter((player) => !player.isStarter);
  const formationRows = buildFormationRows(currentPlayers);

  return (
    <section className="space-y-3 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Lineups</h2>

      {lineupItems.length === 0 ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          Escalacoes nao disponiveis para esta partida.
        </p>
      ) : (
        <>
          <div className="flex gap-2">
            <button
              className={`rounded border px-3 py-1.5 text-sm ${
                tab === "home" ? "border-slate-900 bg-slate-900 text-white" : "border-slate-300 text-slate-700"
              }`}
              onClick={() => {
                setTab("home");
              }}
              type="button"
            >
              {homeTeamName ?? "Mandante"}
            </button>
            <button
              className={`rounded border px-3 py-1.5 text-sm ${
                tab === "away" ? "border-slate-900 bg-slate-900 text-white" : "border-slate-300 text-slate-700"
              }`}
              onClick={() => {
                setTab("away");
              }}
              type="button"
            >
              {awayTeamName ?? "Visitante"}
            </button>
          </div>

          <div className="space-y-2 rounded border border-slate-200 bg-slate-50 p-3">
            <p className="text-sm font-medium text-slate-900">Formacao visual ({currentTeamName})</p>
            {formationRows.length === 0 ? (
              <p className="text-sm text-slate-600">Formacao indisponivel para este time.</p>
            ) : (
              <div className="space-y-2">
                {formationRows.map((row) => (
                  <div className="flex flex-wrap justify-center gap-2" key={row.row}>
                    {row.players.map((player, index) => (
                      <div
                        className="min-w-20 rounded border border-slate-300 bg-white px-2 py-1 text-center text-xs"
                        key={`${player.playerId ?? player.playerName ?? "slot"}-${index}`}
                      >
                        <p className="text-slate-500">#{player.shirtNumber ?? "-"}</p>
                        <p className="truncate font-medium text-slate-800">{player.playerName ?? "Slot"}</p>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="grid gap-3 lg:grid-cols-2">
            <div className="space-y-2">
              <p className="text-sm font-semibold text-slate-800">Titulares ({starters.length})</p>
              {starters.length === 0 ? (
                <p className="rounded border border-slate-200 bg-slate-50 p-2 text-sm text-slate-600">Sem dados.</p>
              ) : (
                <ul className="space-y-1">
                  {starters.map((player, index) => (
                    <li className="rounded border border-slate-100 bg-slate-50 px-2 py-1 text-sm" key={`${player.playerId ?? index}-starter`}>
                      <span className="mr-2 text-slate-500">#{player.shirtNumber ?? "-"}</span>
                      <PlayerName player={player} />
                    </li>
                  ))}
                </ul>
              )}
            </div>

            <div className="space-y-2">
              <p className="text-sm font-semibold text-slate-800">Banco ({bench.length})</p>
              {bench.length === 0 ? (
                <p className="rounded border border-slate-200 bg-slate-50 p-2 text-sm text-slate-600">Sem dados.</p>
              ) : (
                <ul className="space-y-1">
                  {bench.map((player, index) => (
                    <li className="rounded border border-slate-100 bg-slate-50 px-2 py-1 text-sm" key={`${player.playerId ?? index}-bench`}>
                      <span className="mr-2 text-slate-500">#{player.shirtNumber ?? "-"}</span>
                      <PlayerName player={player} />
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </>
      )}
    </section>
  );
}
