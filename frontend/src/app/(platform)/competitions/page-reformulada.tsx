import Link from "next/link";

import { SUPPORTED_COMPETITIONS } from "@/config/competitions.registry";
import { getLatestSeasonForCompetition, listSeasonsForCompetition } from "@/config/seasons.registry";
import { buildCompetitionHubPath } from "@/shared/utils/context-routing";

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function ArrowRightIcon({ className }: { className?: string }) {
  return (
    <svg
      aria-hidden="true"
      className={joinClasses("h-4 w-4", className)}
      fill="none"
      viewBox="0 0 24 24"
    >
      <path
        d="M6.5 12h11m0 0-4.5-4.5M17.5 12l-4.5 4.5"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.9"
      />
    </svg>
  );
}

function SectionHeader({
  actionHref,
  actionLabel,
  countLabel,
  description,
  eyebrow,
  title,
}: {
  actionHref?: string;
  actionLabel?: string;
  countLabel?: string;
  description?: string;
  eyebrow?: string;
  title: string;
}) {
  return (
    <div className="flex flex-col gap-6 md:flex-row md:items-end md:justify-between">
      <div className="space-y-3">
        {eyebrow ? (
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
            {eyebrow}
          </p>
        ) : null}
        <h2 className="font-[family:var(--font-app-headline)] text-[2rem] font-extrabold tracking-[-0.04em] text-[#003526] md:text-[2.35rem]">
          {title}
        </h2>
        {description ? (
          <p className="max-w-2xl text-sm leading-7 text-[#57657a] md:text-base">{description}</p>
        ) : null}
      </div>

      <div className="flex flex-wrap items-center gap-4">
        {countLabel ? (
          <span className="text-sm font-semibold text-[#57657a]">{countLabel}</span>
        ) : null}
        {actionHref && actionLabel ? (
          <Link
            className="inline-flex items-center gap-2 text-sm font-semibold uppercase tracking-[0.16em] text-[#003526] transition-colors hover:text-[#00513b]"
            href={actionHref}
          >
            <span>{actionLabel}</span>
            <ArrowRightIcon />
          </Link>
        ) : null}
      </div>
    </div>
  );
}

function CompetitionCard({
  competition,
  latestSeason,
  seasonsCount,
}: {
  competition: (typeof SUPPORTED_COMPETITIONS)[0];
  latestSeason: ReturnType<typeof getLatestSeasonForCompetition>;
  seasonsCount: number;
}) {
  return (
    <Link
      className="group rounded-[1.35rem] border border-[rgba(216,227,251,0.86)] bg-[rgba(240,243,255,0.82)] px-5 py-5 transition-all hover:border-[#8bd6b6] hover:bg-white hover:shadow-sm"
      href={buildCompetitionHubPath(competition.key)}
    >
      <div className="space-y-4">
        {/* Header com logo e badge */}
        <div className="flex items-start justify-between gap-4">
          <div className="flex items-start gap-3">
            {/* Logo placeholder */}
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-[#ecfff4]">
              <span className="text-[0.65rem] font-bold uppercase tracking-[0.14em] text-[#003526]">
                {competition.shortName}
              </span>
            </div>

            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <span className="inline-block rounded-full bg-[rgba(216,227,251,0.76)] px-2.5 py-1 text-[0.65rem] font-semibold uppercase tracking-[0.14em] text-[#57657a]">
                  {competition.country}
                </span>
                <span className="inline-block rounded-full bg-[rgba(216,227,251,0.76)] px-2.5 py-1 text-[0.65rem] font-semibold uppercase tracking-[0.14em] text-[#57657a]">
                  {competition.seasonCalendar === "annual" ? "Anual" : "Cruzada"}
                </span>
              </div>
              <h3 className="mt-2.5 font-[family:var(--font-app-headline)] text-[1.35rem] font-extrabold leading-tight tracking-[-0.035em] text-[#003526]">
                {competition.name}
              </h3>
            </div>
          </div>

          {/* Coverage badge */}
          <span className="inline-block rounded-full bg-[#a6f2d1] px-3 py-1.5 text-[0.65rem] font-semibold uppercase tracking-[0.14em] text-[#00513b]">
            Completo
          </span>
        </div>

        {/* Description */}
        <p className="text-sm leading-6 text-[#57657a]">
          Acervo de <span className="font-semibold text-[#183247]">{latestSeason?.label ?? "-"}</span> com{" "}
          <span className="font-semibold text-[#183247]">{seasonsCount} temporadas</span> disponíveis para análise.
        </p>

        {/* Stats Grid */}
        <div className="grid grid-cols-2 gap-3">
          <div className="rounded-lg bg-white/50 px-3 py-3">
            <p className="text-[0.65rem] font-semibold uppercase tracking-[0.14em] text-[#57657a]">
              Temporadas
            </p>
            <p className="mt-1.5 font-[family:var(--font-app-headline)] text-lg font-extrabold text-[#003526]">
              {seasonsCount}
            </p>
          </div>
          <div className="rounded-lg bg-white/50 px-3 py-3">
            <p className="text-[0.65rem] font-semibold uppercase tracking-[0.14em] text-[#57657a]">
              Mais recente
            </p>
            <p className="mt-1.5 font-[family:var(--font-app-headline)] text-lg font-extrabold text-[#003526]">
              {latestSeason?.label ?? "-"}
            </p>
          </div>
        </div>

        {/* CTA */}
        <div className="flex items-center justify-between border-t border-[rgba(216,227,251,0.5)] pt-4">
          <span className="text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#003526] transition-colors group-hover:text-[#00513b]">
            Abrir competição
          </span>
          <ArrowRightIcon className="h-4 w-4 text-[#003526] transition-transform group-hover:translate-x-1" />
        </div>
      </div>
    </Link>
  );
}

export default function CompetitionsIndexPage() {
  const allCompetitions = SUPPORTED_COMPETITIONS;
  const domesticCompetitions = allCompetitions.filter((c) => c.type !== "international_cup");
  const continentalCompetitions = allCompetitions.filter((c) => c.type === "international_cup");

  return (
    <div className="space-y-12 px-6 py-8 md:px-10 md:py-10 xl:px-12">
      <div className="mx-auto max-w-6xl space-y-12">
        {/* Hero Section */}
        <section className="space-y-8">
          <div className="space-y-6">
            <div className="space-y-3">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Catálogo
              </p>
              <h1 className="font-[family:var(--font-app-headline)] text-[3rem] font-extrabold leading-[0.92] tracking-[-0.06em] text-[#003526] md:text-[3.5rem]">
                Explore Competições
              </h1>
              <p className="max-w-2xl text-base leading-8 text-[#57657a] md:text-[1.05rem]">
                {allCompetitions.length} competições com cobertura completa de dados históricos. Acesse temporadas, tabelas, rankings e partidas em um único fluxo.
              </p>
            </div>

            <div className="flex flex-wrap gap-3">
              <Link
                className="inline-flex items-center gap-2 rounded-full bg-[#003526] px-5 py-3 font-semibold uppercase tracking-[0.16em] text-white transition-colors hover:bg-[#00513b]"
                href="#competicoes-nacionais"
              >
                <span>Explorar competições</span>
                <ArrowRightIcon className="text-white" />
              </Link>
            </div>
          </div>

          {/* Stats Cards */}
          <div className="grid gap-4 sm:grid-cols-3">
            <div className="rounded-[1.35rem] border border-[rgba(216,227,251,0.86)] bg-[rgba(240,243,255,0.82)] px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Total
              </p>
              <p className="mt-3 font-[family:var(--font-app-headline)] text-2xl font-extrabold text-[#003526]">
                {allCompetitions.length}
              </p>
              <p className="mt-1 text-xs text-[#57657a]">competições catalogadas</p>
            </div>

            <div className="rounded-[1.35rem] border border-[rgba(216,227,251,0.86)] bg-[rgba(240,243,255,0.82)] px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Nacionais
              </p>
              <p className="mt-3 font-[family:var(--font-app-headline)] text-2xl font-extrabold text-[#003526]">
                {domesticCompetitions.length}
              </p>
              <p className="mt-1 text-xs text-[#57657a]">ligas domésticas</p>
            </div>

            <div className="rounded-[1.35rem] border border-[rgba(216,227,251,0.86)] bg-[rgba(240,243,255,0.82)] px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Continentais
              </p>
              <p className="mt-3 font-[family:var(--font-app-headline)] text-2xl font-extrabold text-[#003526]">
                {continentalCompetitions.length}
              </p>
              <p className="mt-1 text-xs text-[#57657a]">competições internacionais</p>
            </div>
          </div>
        </section>

        {/* Domestic Competitions Section */}
        {domesticCompetitions.length > 0 && (
          <section className="space-y-6" id="competicoes-nacionais">
            <SectionHeader
              countLabel={`${domesticCompetitions.length} competições`}
              description="Cobertura prioritária do acervo com acesso direto à temporada mais recente em cada card."
              eyebrow="Catálogo"
              title="Competições Nacionais"
            />

            <div className="grid gap-5 lg:grid-cols-2 xl:grid-cols-3">
              {domesticCompetitions.map((competition) => {
                const latestSeason = getLatestSeasonForCompetition(competition);
                const seasonsCount = listSeasonsForCompetition(competition).length;

                return (
                  <CompetitionCard
                    competition={competition}
                    key={competition.id}
                    latestSeason={latestSeason}
                    seasonsCount={seasonsCount}
                  />
                );
              })}
            </div>
          </section>
        )}

        {/* Continental Competitions Section */}
        {continentalCompetitions.length > 0 && (
          <section className="space-y-6">
            <SectionHeader
              countLabel={`${continentalCompetitions.length} competições`}
              description="Competições continentais com o mesmo nível de leitura de recorte, volume e profundidade."
              eyebrow="Catálogo"
              title="Continentais"
            />

            <div className="grid gap-5 lg:grid-cols-2 xl:grid-cols-3">
              {continentalCompetitions.map((competition) => {
                const latestSeason = getLatestSeasonForCompetition(competition);
                const seasonsCount = listSeasonsForCompetition(competition).length;

                return (
                  <CompetitionCard
                    competition={competition}
                    key={competition.id}
                    latestSeason={latestSeason}
                    seasonsCount={seasonsCount}
                  />
                );
              })}
            </div>
          </section>
        )}
      </div>
    </div>
  );
}
