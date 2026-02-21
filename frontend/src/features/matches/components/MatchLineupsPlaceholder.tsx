import type { MatchLineupPlayer } from "@/features/matches/types";

type MatchLineupsPlaceholderProps = {
  lineups: MatchLineupPlayer[] | undefined;
};

function resolvePlayerLabel(player: MatchLineupPlayer): string {
  const playerName = player.playerName?.trim() || "Jogador sem nome";
  const position = player.position?.trim();
  const shirtNumber = typeof player.shirtNumber === "number" ? `#${player.shirtNumber}` : null;
  const starterTag = player.isStarter ? "Titular" : "Banco";

  return [playerName, position, shirtNumber, starterTag].filter(Boolean).join(" | ");
}

export function MatchLineupsPlaceholder({ lineups }: MatchLineupsPlaceholderProps) {
  const lineupItems = lineups ?? [];

  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Lineups (placeholder)</h2>

      {lineupItems.length === 0 ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          TODO: layout completo de escalacoes e formacoes. Nenhum lineup retornado para o jogo.
        </p>
      ) : (
        <ul className="space-y-1 text-sm text-slate-700">
          {lineupItems.slice(0, 10).map((player, index) => (
            <li
              className="rounded border border-slate-100 bg-slate-50 px-2 py-1"
              key={`${player.playerId ?? `slot-${index}`}-${player.teamId ?? "team-unknown"}`}
            >
              {resolvePlayerLabel(player)}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
