import type { ClubHonor, ClubHonorScope, ClubHonors } from "@/features/teams/types";
import { ProfileCoveragePill, ProfilePanel } from "@/shared/components/profile/ProfilePrimitives";

type TeamHonorsSectionProps = {
  honors: ClubHonors;
};

const SCOPE_ORDER: ClubHonorScope[] = ["mundial", "continental", "nacional", "estadual", "other"];

const SCOPE_LABELS: Record<ClubHonorScope, string> = {
  mundial: "Mundiais",
  continental: "Continentais",
  nacional: "Nacionais",
  estadual: "Estaduais",
  other: "Outras",
};

const INTEGER_FORMATTER = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });

function formatInteger(value: number): string {
  return INTEGER_FORMATTER.format(value);
}

function getHonorLabel(honor: ClubHonor): string {
  if (honor.seasonLabel?.trim()) {
    return honor.seasonLabel;
  }

  if (typeof honor.year === "number") {
    return String(honor.year);
  }

  return "Edição não informada";
}

function groupHonors(items: ClubHonor[]) {
  return SCOPE_ORDER.map((scope) => {
    const scopedItems = items.filter((item) => item.scope === scope);
    const competitions = Array.from(
      scopedItems.reduce((groups, item) => {
        const current = groups.get(item.competitionName) ?? [];
        current.push(item);
        groups.set(item.competitionName, current);
        return groups;
      }, new Map<string, ClubHonor[]>()),
    )
      .map(([competitionName, honorsForCompetition]) => ({
        competitionName,
        items: honorsForCompetition,
      }))
      .sort((left, right) =>
        right.items.length === left.items.length
          ? left.competitionName.localeCompare(right.competitionName, "pt-BR")
          : right.items.length - left.items.length,
      );

    return { scope, competitions, total: scopedItems.length };
  }).filter((group) => group.total > 0);
}

export function TeamHonorsSection({ honors }: TeamHonorsSectionProps) {
  const groups = groupHonors(honors.items);
  const sources = Array.from(
    honors.items.reduce((items, honor) => {
      if (honor.sourceUrl?.trim()) {
        items.set(honor.sourceUrl, honor.sourceName);
      }
      return items;
    }, new Map<string, string>()),
  );

  return (
    <ProfilePanel className="space-y-6" tone="base">
      <header className="grid gap-4 border-b border-[rgba(191,201,195,0.42)] pb-5 md:grid-cols-[minmax(0,1fr)_auto] md:items-end">
        <div>
          <p className="text-[0.7rem] font-bold uppercase tracking-[0.2em] text-[#57657a]">
            Conquistas documentadas
          </p>
          <h2 className="mt-2 max-w-3xl font-[family:var(--font-profile-headline)] text-3xl font-extrabold tracking-[-0.045em] text-[#111c2d] md:text-4xl">
            O que o acervo registra
          </h2>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[#57657a]">
            {honors.criterionLabel} O total abaixo descreve somente os registros publicados pela plataforma.
          </p>
        </div>

        <div className="md:text-right">
          <p className="font-[family:var(--font-profile-headline)] text-5xl font-extrabold tracking-[-0.06em] text-[#00513b]">
            {formatInteger(honors.total)}
          </p>
          <p className="mt-1 text-xs font-semibold uppercase tracking-[0.16em] text-[#57657a]">
            registros documentados
          </p>
        </div>
      </header>

      {groups.length > 0 ? (
        <div className="grid gap-x-8 gap-y-6 lg:grid-cols-2">
          {groups.map((group) => (
            <section aria-labelledby={`honors-${group.scope}`} key={group.scope}>
              <div className="flex items-baseline justify-between gap-4">
                <h3
                  className="font-[family:var(--font-profile-headline)] text-xl font-extrabold text-[#111c2d]"
                  id={`honors-${group.scope}`}
                >
                  {SCOPE_LABELS[group.scope]}
                </h3>
                <span className="text-sm font-bold text-[#00513b]">{formatInteger(group.total)}</span>
              </div>

              <div className="mt-3 divide-y divide-[rgba(191,201,195,0.42)] border-y border-[rgba(191,201,195,0.42)]">
                {group.competitions.map((competition) => (
                  <details className="group py-3" key={`${group.scope}-${competition.competitionName}`}>
                    <summary className="flex min-h-11 cursor-pointer list-none items-center justify-between gap-4 rounded-lg px-1 text-left focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#00513b] [&::-webkit-details-marker]:hidden">
                      <span className="text-sm font-semibold text-[#111c2d]">
                        {competition.competitionName}
                      </span>
                      <span className="flex shrink-0 items-center gap-2 text-sm text-[#57657a]">
                        {competition.items.length}×
                        <svg
                          aria-hidden="true"
                          className="h-4 w-4 transition-transform duration-200 group-open:rotate-180 motion-reduce:transition-none"
                          fill="none"
                          viewBox="0 0 20 20"
                        >
                          <path d="m6 8 4 4 4-4" stroke="currentColor" strokeLinecap="round" strokeWidth="1.7" />
                        </svg>
                      </span>
                    </summary>
                    <ul className="grid gap-1.5 pb-1 pl-1 pt-2 text-sm leading-6 text-[#57657a] sm:grid-cols-2">
                      {competition.items.map((honor, index) => (
                        <li key={`${honor.competitionKey ?? honor.competitionName}-${getHonorLabel(honor)}-${index}`}>
                          {getHonorLabel(honor)}
                        </li>
                      ))}
                    </ul>
                  </details>
                ))}
              </div>
            </section>
          ))}
        </div>
      ) : (
        <p className="text-sm leading-6 text-[#57657a]">
          A API reconhece este bloco, mas ainda não publicou itens de conquista para o clube.
        </p>
      )}

      <footer className="flex flex-wrap items-center justify-between gap-3">
        <ProfileCoveragePill
          coverage={{ ...honors.coverage, label: "Cobertura das conquistas" }}
        />
        {sources.length > 0 ? (
          <p className="text-xs leading-5 text-[#57657a]">
            Fontes: {sources.map(([url, name], index) => (
              <span key={url}>
                {index > 0 ? " · " : ""}
                <a
                  className="font-semibold text-[#00513b] underline decoration-[#8bd6b6] underline-offset-4 hover:text-[#003526]"
                  href={url}
                  rel="noreferrer"
                  target="_blank"
                >
                  {name}
                </a>
              </span>
            ))}
          </p>
        ) : null}
      </footer>
    </ProfilePanel>
  );
}
