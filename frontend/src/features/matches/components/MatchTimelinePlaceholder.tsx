import type { MatchTimelineEvent } from "@/features/matches/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";

type MatchTimelinePlaceholderProps = {
  events: MatchTimelineEvent[] | undefined;
};

function toTimelineLabel(event: MatchTimelineEvent): string {
  const minute = typeof event.minute === "number" ? `${event.minute}'` : "min ?";
  const type = event.type ?? "evento";
  const detail = event.detail?.trim();
  const playerName = event.playerName?.trim();

  if (detail && detail.length > 0) {
    return `${minute} ${type} - ${detail}`;
  }

  if (playerName && playerName.length > 0) {
    return `${minute} ${type} - ${playerName}`;
  }

  return `${minute} ${type}`;
}

function eventIcon(eventType?: string | null): string {
  switch (eventType) {
    case "Goal":
      return "G";
    case "Yellow Card":
      return "YC";
    case "Red Card":
      return "RC";
    case "Substitution":
    case "subst":
      return "SUB";
    default:
      return "EV";
  }
}

export function MatchTimelinePlaceholder({ events }: MatchTimelinePlaceholderProps) {
  const timelineEvents = events ?? [];

  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Timeline da Partida</h2>

      {timelineEvents.length === 0 ? (
        <div className="space-y-2 rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          <p>Eventos nao disponiveis para esta partida.</p>
          <CoverageBadge coverage={{ status: "empty", label: "Cobertura de eventos" }} />
        </div>
      ) : (
        <ul className="space-y-1 text-sm text-slate-700">
          {timelineEvents.slice(0, 8).map((event, index) => (
            <li
              className="flex items-center gap-2 rounded border border-slate-100 bg-slate-50 px-2 py-1"
              key={event.eventId ?? `${index}-${event.minute ?? "x"}`}
            >
              <span className="inline-flex min-w-9 items-center justify-center rounded border border-slate-300 bg-white px-1 py-0.5 text-[10px] font-semibold text-slate-600">
                {eventIcon(event.type)}
              </span>
              <span>{toTimelineLabel(event)}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
