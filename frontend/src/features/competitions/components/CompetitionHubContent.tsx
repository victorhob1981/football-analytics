"use client";

import Image from "next/image";
import Link from "next/link";

import type { CompetitionDef } from "@/config/competitions.registry";
import {
  getLatestSeasonForCompetition,
  listSeasonsForCompetition,
} from "@/config/seasons.registry";
import { useCompetitionStructure } from "@/features/competitions/hooks";
import { describeCompetitionEdition } from "@/features/competitions/utils/competition-structure";
import { resolveSeasonChampionArtwork } from "@/features/competitions/utils/champion-media";
import {
  ProfileAlert,
  ProfilePanel,
  ProfileShell,
  ProfileTag,
} from "@/shared/components/profile/ProfilePrimitives";
import { CompetitionRouteContextSync } from "@/shared/components/routing/CompetitionRouteContextSync";
import { buildSeasonHubPath, buildSeasonHubTabPath } from "@/shared/utils/context-routing";

type CompetitionHubContentProps = {
  competition: CompetitionDef;
};

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function CompetitionSeasonFormatTag({
  structureEnabled = true,
  competitionKey,
  seasonLabel,
}: {
  structureEnabled?: boolean;
  competitionKey: string;
  seasonLabel: string;
}) {
  const structureQuery = useCompetitionStructure({
    competitionKey,
    seasonLabel,
  }, {
    enabled: structureEnabled,
  });
  const editionLabel = describeCompetitionEdition(structureQuery.data);

  if (!editionLabel) {
    return null;
  }

  return <ProfileTag>{editionLabel}</ProfileTag>;
}

function describeSeasonCalendar(competition: CompetitionDef): string {
  return competition.seasonCalendar === "annual" ? "Ano a ano" : "Temporada cruzada";
}

function describeCompetitionType(competition: CompetitionDef): string {
  if (competition.type === "domestic_league") {
    return "Liga nacional";
  }

  if (competition.type === "domestic_cup") {
    return "Copa nacional";
  }

  return "Copa internacional";
}

function CompetitionSeasonShortcut({
  description,
  href,
  title,
}: {
  description: string;
  href: string;
  title: string;
}) {
  return (
    <Link
      className="rounded-[1.25rem] border border-[rgba(191,201,195,0.52)] bg-white px-4 py-4 transition-[transform,border-color,background-color,box-shadow] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-0.5 hover:border-[#8bd6b6] hover:shadow-[0_20px_52px_-40px_rgba(17,28,45,0.24)] active:scale-[0.985]"
      href={href}
    >
      <h3 className="font-[family:var(--font-profile-headline)] text-[1.22rem] font-extrabold tracking-[-0.03em] text-[#111c2d]">
        {title}
      </h3>
      <p className="mt-2 text-sm/6 text-[#57657a]">{description}</p>
      <div className="mt-4 inline-flex items-center gap-2 text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
        <span>Abrir {title.toLowerCase()}</span>
      </div>
    </Link>
  );
}

function CompetitionSeasonEntryCard({
  competition,
  href,
  isActiveSeason,
  seasonLabel,
}: {
  competition: CompetitionDef;
  href: string;
  isActiveSeason: boolean;
  seasonLabel: string;
}) {
  return (
    <Link
      className={joinClasses(
        "rounded-[1.3rem] border px-4 py-4 transition-[transform,border-color,background-color,box-shadow] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-0.5 hover:bg-white hover:shadow-[0_20px_52px_-40px_rgba(17,28,45,0.22)] active:scale-[0.985]",
        isActiveSeason
          ? "border-[#8bd6b6] bg-[linear-gradient(180deg,rgba(245,255,250,0.96)_0%,rgba(240,243,255,0.92)_100%)]"
          : "border-[rgba(191,201,195,0.52)] bg-[rgba(246,248,252,0.92)]",
      )}
      href={href}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
            Temporada
          </p>
          <p className="mt-2 font-[family:var(--font-profile-headline)] text-[1.85rem] font-extrabold tracking-[-0.04em] text-[#111c2d]">
            {seasonLabel}
          </p>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          {isActiveSeason ? <ProfileTag>Atual</ProfileTag> : null}
          <ProfileTag>{competition.seasonCalendar === "annual" ? "Anual" : "Cruzada"}</ProfileTag>
          <CompetitionSeasonFormatTag
            competitionKey={competition.key}
            seasonLabel={seasonLabel}
            structureEnabled={competition.type !== "domestic_league"}
          />
        </div>
      </div>
      <div className="mt-4 inline-flex items-center gap-2 text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
        <span>{isActiveSeason ? "Entrar na temporada principal" : "Abrir temporada"}</span>
      </div>
    </Link>
  );
}

function CompetitionChampionSpotlight({
  competition,
  latestSeasonLabel,
  seasonsCount,
}: {
  competition: CompetitionDef;
  latestSeasonLabel: string | null;
  seasonsCount: number;
}) {
  const artwork =
    latestSeasonLabel === null
      ? null
      : resolveSeasonChampionArtwork(competition.key, latestSeasonLabel);

  if (!latestSeasonLabel) {
    return (
      <aside className="rounded-[1.9rem] border border-[rgba(191,201,195,0.52)] bg-white/92 p-5 shadow-[0_24px_72px_-54px_rgba(17,28,45,0.24)] md:p-6">
        <div className="space-y-3">
          <span className="inline-flex items-center rounded-full bg-[#eef3ff] px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#455468]">
            Competição
          </span>
          <h2 className="font-[family:var(--font-profile-headline)] text-[1.9rem] font-extrabold tracking-[-0.035em] text-[#111c2d]">
            {competition.name}
          </h2>
          <p className="text-sm/6 text-[#57657a]">
            Sem temporada principal disponível agora. A lista completa continua abaixo quando o
            catálogo estiver carregado.
          </p>
        </div>
      </aside>
    );
  }

  if (!artwork) {
    return (
      <aside className="rounded-[1.9rem] border border-[rgba(191,201,195,0.52)] bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(242,247,244,0.92)_100%)] p-5 shadow-[0_24px_72px_-54px_rgba(17,28,45,0.24)] md:p-6">
        <div className="space-y-5">
          <div className="space-y-2">
            <span className="inline-flex items-center rounded-full bg-[#e7f4ee] px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#0a3d2c]">
              Temporada em destaque
            </span>
            <h2 className="font-[family:var(--font-profile-headline)] text-[2.2rem] font-extrabold tracking-[-0.04em] text-[#111c2d]">
              {latestSeasonLabel}
            </h2>
            <p className="text-sm/6 text-[#57657a]">
              Entrada principal desta competição. O foco aqui é escolher a edição certa e seguir
              para calendário, tabela ou rankings.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
            <div className="rounded-[1.2rem] border border-[rgba(191,201,195,0.52)] bg-white px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Temporadas
              </p>
              <p className="mt-2 font-[family:var(--font-profile-headline)] text-[1.6rem] font-extrabold text-[#111c2d]">
                {seasonsCount}
              </p>
            </div>
            <div className="rounded-[1.2rem] border border-[rgba(191,201,195,0.52)] bg-white px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Modelo
              </p>
              <p className="mt-2 font-[family:var(--font-profile-headline)] text-[1.6rem] font-extrabold text-[#111c2d]">
                {competition.seasonCalendar === "annual" ? "Anual" : "Cruzado"}
              </p>
            </div>
          </div>
        </div>
      </aside>
    );
  }

  return (
    <aside className="relative isolate min-h-[340px] overflow-hidden rounded-[1.9rem] border border-white/10 bg-[#081c15] shadow-[0_34px_84px_-56px_rgba(8,25,20,0.65)]">
      <Image
        alt={`Campeão ${artwork.teamName} em ${competition.name} ${latestSeasonLabel}`}
        className="object-cover object-center"
        fill
        priority
        sizes="(min-width: 1280px) 360px, 100vw"
        src={artwork.src}
      />
      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(8,28,21,0.06)_0%,rgba(8,28,21,0.38)_36%,rgba(8,28,21,0.94)_100%)]" />
      <div className="relative flex h-full flex-col justify-between p-5 md:p-6">
        <div className="flex flex-wrap items-center gap-2">
          <span className="inline-flex items-center rounded-full bg-white/12 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-white/88">
            Última edição fechada
          </span>
          <span className="inline-flex items-center rounded-full bg-white/8 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-white/72">
            {latestSeasonLabel}
          </span>
        </div>

        <div className="space-y-3">
          <p className="max-w-sm text-sm/6 text-white/72">
            Referência visual da edição mais recente para dar identidade sem transformar a página em
            peça promocional.
          </p>
          <div className="space-y-1">
            <h2 className="font-[family:var(--font-profile-headline)] text-[2.1rem] font-extrabold tracking-[-0.04em] text-white">
              {artwork.teamName}
            </h2>
            <p className="text-sm text-white/72">{competition.name}</p>
          </div>
        </div>
      </div>
    </aside>
  );
}

export function CompetitionHubContent({ competition }: CompetitionHubContentProps) {
  const seasons = listSeasonsForCompetition(competition);
  const activeSeason = getLatestSeasonForCompetition(competition);
  const activeSeasonPath = activeSeason
    ? buildSeasonHubPath({
        competitionKey: competition.key,
        seasonLabel: activeSeason.label,
      })
    : null;
  const activeSeasonCalendarHref = activeSeason
    ? buildSeasonHubTabPath(
        {
          competitionKey: competition.key,
          seasonLabel: activeSeason.label,
        },
        "calendar",
      )
    : null;
  const activeSeasonStandingsHref = activeSeason
    ? buildSeasonHubTabPath(
        {
          competitionKey: competition.key,
          seasonLabel: activeSeason.label,
        },
        "standings",
      )
    : null;
  const activeSeasonRankingsHref = activeSeason
    ? buildSeasonHubTabPath(
        {
          competitionKey: competition.key,
          seasonLabel: activeSeason.label,
        },
        "rankings",
      )
    : null;
  const spotlightSeasonLabels = seasons.slice(0, 4);

  return (
    <CompetitionRouteContextSync competition={competition}>
      <ProfileShell className="space-y-6">
        <div className="flex flex-wrap items-center gap-2 text-[0.78rem] font-semibold uppercase tracking-[0.16em] text-[#455468]">
          <span>Competições</span>
          <span className="text-[#8fa097]">/</span>
          <span>{competition.shortName}</span>
        </div>

        <section className="relative isolate overflow-hidden rounded-[2rem] border border-white/65 bg-[linear-gradient(180deg,rgba(255,255,255,0.92)_0%,rgba(246,249,255,0.96)_46%,rgba(243,247,243,0.94)_100%)] p-5 shadow-[0_28px_88px_-58px_rgba(17,28,45,0.26)] md:p-6 xl:p-8">
          <div className="pointer-events-none absolute inset-x-0 top-0 h-44 bg-[radial-gradient(circle_at_top_left,rgba(216,227,251,0.72),transparent_52%),radial-gradient(circle_at_top_right,rgba(139,214,182,0.2),transparent_40%)]" />

          <div className="relative grid gap-5 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.86fr)]">
            <div className="space-y-5">
              <div className="flex flex-wrap items-center gap-2">
                <ProfileTag className="bg-white text-[#455468]">{competition.region}</ProfileTag>
                <ProfileTag className="bg-white text-[#455468]">{competition.country}</ProfileTag>
                <ProfileTag className="bg-white text-[#455468]">
                  {describeSeasonCalendar(competition)}
                </ProfileTag>
                <ProfileTag className="bg-white text-[#455468]">
                  {describeCompetitionType(competition)}
                </ProfileTag>
              </div>

              <div className="space-y-3">
                <p className="text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#0a3d2c]">
                  Competição
                </p>
                <h1 className="max-w-4xl font-[family:var(--font-profile-headline)] text-[2.65rem] font-extrabold tracking-[-0.05em] text-[#111c2d] md:text-[3.35rem]">
                  {competition.name}
                </h1>
                <p className="max-w-3xl text-sm/7 text-[#57657a] md:text-[0.98rem]">
                  Escolha a temporada certa e entre direto na edição. O hub da competição precisa
                  resolver contexto e acesso, não se explicar.
                </p>
              </div>

              {activeSeason && activeSeasonPath ? (
                <div className="grid gap-4 lg:grid-cols-[minmax(0,1.1fr)_minmax(260px,0.9fr)]">
                  <div className="rounded-[1.7rem] border border-[rgba(191,201,195,0.52)] bg-white px-5 py-5 shadow-[0_22px_64px_-48px_rgba(17,28,45,0.22)]">
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                          Temporada principal
                        </p>
                        <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-[2.6rem] font-extrabold tracking-[-0.05em] text-[#111c2d]">
                          {activeSeason.label}
                        </h2>
                      </div>
                      <div className="flex flex-wrap items-center justify-end gap-2">
                        <ProfileTag>Atual</ProfileTag>
                        <ProfileTag>
                          {competition.seasonCalendar === "annual" ? "Anual" : "Cruzada"}
                        </ProfileTag>
                        <CompetitionSeasonFormatTag
                          competitionKey={competition.key}
                          seasonLabel={activeSeason.label}
                          structureEnabled={competition.type !== "domestic_league"}
                        />
                      </div>
                    </div>

                    <p className="mt-4 text-sm/6 text-[#57657a]">
                      Entrada principal desta competição. Daqui a pessoa escolhe a edição e segue
                      para calendário, tabela ou rankings sem voltar ao catálogo.
                    </p>

                    <div className="mt-5 grid gap-3 md:grid-cols-3">
                      {activeSeasonCalendarHref ? (
                        <CompetitionSeasonShortcut
                          description="Lista de partidas e saída para o match center."
                          href={activeSeasonCalendarHref}
                          title="Calendário"
                        />
                      ) : null}
                      {activeSeasonStandingsHref ? (
                        <CompetitionSeasonShortcut
                          description="Classificação, rodadas e leitura estrutural da edição."
                          href={activeSeasonStandingsHref}
                          title="Tabela"
                        />
                      ) : null}
                      {activeSeasonRankingsHref ? (
                        <CompetitionSeasonShortcut
                          description="Destaques de jogadores e times no mesmo recorte."
                          href={activeSeasonRankingsHref}
                          title="Rankings"
                        />
                      ) : null}
                    </div>

                    <Link
                      className="mt-5 inline-flex items-center gap-2 text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#003526] transition-colors hover:text-[#00513b]"
                      href={activeSeasonPath}
                    >
                      <span>Entrar na temporada</span>
                    </Link>
                  </div>

                  <div className="rounded-[1.7rem] border border-[rgba(191,201,195,0.52)] bg-[rgba(246,248,252,0.94)] px-4 py-5 shadow-[0_20px_56px_-46px_rgba(17,28,45,0.16)]">
                    <div className="flex items-center justify-between gap-3">
                      <div>
                        <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                          Temporadas disponíveis
                        </p>
                        <p className="mt-2 text-sm/6 text-[#57657a]">
                          Acessos rápidos para navegar entre edições fechadas.
                        </p>
                      </div>
                      <Link
                        className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.24)] bg-white/88 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#455468] transition-[transform,border-color,background-color] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-0.5 hover:border-[#8bd6b6] hover:bg-white active:scale-[0.985]"
                        href="/competitions"
                      >
                        Voltar ao catálogo
                      </Link>
                    </div>

                    <div className="mt-4 grid gap-2.5">
                      {spotlightSeasonLabels.map((season) => (
                        <CompetitionSeasonEntryCard
                          competition={competition}
                          href={buildSeasonHubPath({
                            competitionKey: competition.key,
                            seasonLabel: season.label,
                          })}
                          isActiveSeason={activeSeason.id === season.id}
                          key={season.id}
                          seasonLabel={season.label}
                        />
                      ))}
                    </div>
                  </div>
                </div>
              ) : (
                <ProfileAlert title="Temporadas indisponíveis" tone="warning">
                  Não encontramos temporadas disponíveis para esta competição no momento.
                </ProfileAlert>
              )}
            </div>

            <CompetitionChampionSpotlight
              competition={competition}
              latestSeasonLabel={activeSeason?.label ?? null}
              seasonsCount={seasons.length}
            />
          </div>
        </section>

        <ProfilePanel className="space-y-5">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
                Todas as temporadas
              </p>
              <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-[2rem] font-extrabold tracking-[-0.04em] text-[#111c2d]">
                Escolha a edição certa
              </h2>
              <p className="mt-2 max-w-3xl text-sm/6 text-[#57657a]">
                {seasons.length} temporadas disponíveis nesta competição, com contexto consistente
                para calendário, tabela e rankings.
              </p>
            </div>
            {activeSeasonPath ? (
              <Link
                className="inline-flex items-center rounded-full bg-[#003526] px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-white transition-[transform,background-color] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-0.5 hover:bg-[#004e39] active:scale-[0.985]"
                href={activeSeasonPath}
              >
                Abrir temporada atual
              </Link>
            ) : null}
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            {seasons.map((season) => (
              <CompetitionSeasonEntryCard
                competition={competition}
                href={buildSeasonHubPath({
                  competitionKey: competition.key,
                  seasonLabel: season.label,
                })}
                isActiveSeason={activeSeason?.id === season.id}
                key={season.id}
                seasonLabel={season.label}
              />
            ))}
          </div>
        </ProfilePanel>
      </ProfileShell>
    </CompetitionRouteContextSync>
  );
}
