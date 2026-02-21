import type { MatchTimelineEvent } from "@/features/matches/types";

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

export function MatchTimelinePlaceholder({ events }: MatchTimelinePlaceholderProps) {
  const timelineEvents = events ?? [];

  return (
    <section className="space-y-2 rounded-md border border-slate-200 bg-white p-4">
      <h2 className="text-lg font-medium text-slate-900">Timeline (placeholder)</h2>

      {timelineEvents.length === 0 ? (
        <p className="rounded border border-dashed border-slate-300 bg-slate-50 p-3 text-sm text-slate-600">
          TODO: timeline detalhada por minuto. Payload atual sem eventos para este recorte.
        </p>
      ) : (
        <ul className="space-y-1 text-sm text-slate-700">
          {timelineEvents.slice(0, 8).map((event, index) => (
            <li className="rounded border border-slate-100 bg-slate-50 px-2 py-1" key={event.eventId ?? `${index}-${event.minute ?? "x"}`}>
              {toTimelineLabel(event)}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
