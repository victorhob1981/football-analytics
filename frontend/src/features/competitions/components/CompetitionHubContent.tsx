"use client";

import { useMemo } from "react";

import Image from "next/image";
import Link from "next/link";

import {
  getCompetitionVisualAssetId,
  type CompetitionDef,
} from "@/config/competitions.registry";
import {
  getLatestSeasonForCompetition,
  listSeasonsForCompetition,
  type SeasonDef,
} from "@/config/seasons.registry";
import {
  useCompetitionAnalytics,
  useCompetitionHistoricalStats,
  useCompetitionStructure,
  useStageTies,
} from "@/features/competitions/hooks";
import type {
  CompetitionHistoricalStatGroup,
  CompetitionHistoricalStatItem,
  StageTie,
} from "@/features/competitions/types";
import { resolveSeasonChampionArtwork } from "@/features/competitions/utils/champion-media";
import { resolveCompetitionSeasonSurface } from "@/features/competitions/utils/competition-season-surface";
import { fetchStandings } from "@/features/standings/services/standings.service";
import type { StandingsTableData, StandingsTableRow } from "@/features/standings/types";
import { standingsQueryKeys } from "@/features/standings/queryKeys";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { ProfileMedia } from "@/shared/components/profile/ProfileMedia";
import {
  ProfilePanel,
  ProfileCoveragePill,
  ProfileShell,
  ProfileTag,
} from "@/shared/components/profile/ProfilePrimitives";
import { CompetitionRouteContextSync } from "@/shared/components/routing/CompetitionRouteContextSync";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import {
  buildPlayerResolverPath,
  buildSeasonHubPath,
  buildTeamResolverPath,
} from "@/shared/utils/context-routing";

type CompetitionHubContentProps = {
  competition: CompetitionDef;
};

const HISTORICAL_STATS_AS_OF_YEAR = 2025;
const HISTORICAL_TABLE_LIMIT = 6;

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

function buildFallbackLabel(value: string): string {
  const tokens = value
    .replace(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (tokens.length === 0) {
    return "FA";
  }

  if (tokens.length === 1) {
    return tokens[0].slice(0, 3).toUpperCase();
  }

  return tokens
    .slice(0, 2)
    .map((token) => token[0])
    .join("")
    .slice(0, 3)
    .toUpperCase();
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

function describeCompetitionFormatFallback(competition: CompetitionDef): string {
  if (competition.type === "domestic_league") {
    return "Pontos corridos";
  }

  if (competition.type === "domestic_cup") {
    return "Mata-mata";
  }

  return "Competicao continental";
}

function formatWholeNumber(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }

  return new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 }).format(value);
}

function formatHistoricalValue(item: CompetitionHistoricalStatItem): string {
  if (item.valueLabel && item.valueLabel.trim().length > 0) {
    return item.valueLabel;
  }

  if (typeof item.value === "number") {
    return formatWholeNumber(item.value);
  }

  if (typeof item.value === "string" && item.value.trim().length > 0) {
    return item.value;
  }

  return "-";
}

function isHistoricalStatsDataEmpty(data: ReturnType<typeof useCompetitionHistoricalStats>["data"]) {
  if (!data) {
    return true;
  }

  return (
    data.champions.items.length === 0 &&
    data.scorers.items.length === 0 &&
    data.teamRecords.items.length === 0 &&
    data.matchRecords.items.length === 0 &&
    data.playerRecords.items.length === 0
  );
}

function buildHistoricalEntityHref(
  item: CompetitionHistoricalStatItem,
  competition: CompetitionDef,
): string | null {
  if (!item.entityId) {
    return null;
  }

  const contextFilters = {
    competitionId: competition.id,
    competitionKey: competition.key,
  };

  if (item.entityType === "team") {
    return buildTeamResolverPath(item.entityId, contextFilters);
  }

  if (item.entityType === "player") {
    return buildPlayerResolverPath(item.entityId, contextFilters);
  }

  return null;
}

function HistoricalEntityLabel({
  competition,
  item,
}: {
  competition: CompetitionDef;
  item: CompetitionHistoricalStatItem;
}) {
  const href = buildHistoricalEntityHref(item, competition);

  if (!href || !item.entityId) {
    return <span className="truncate font-semibold text-[#111c2d]">{item.entityName}</span>;
  }

  const mediaCategory = item.entityType === "player" ? "players" : "clubs";

  return (
    <Link
      className="inline-flex min-w-0 items-center gap-2 font-semibold text-[#111c2d] transition-colors hover:text-[#00513b]"
      href={href}
    >
      <ProfileMedia
        alt={item.entityName}
        assetId={item.entityId}
        category={mediaCategory}
        className="h-7 w-7 rounded-full"
        fallback={buildFallbackLabel(item.entityName)}
        imageClassName="p-1"
        shape="circle"
      />
      <span className="truncate">{item.entityName}</span>
    </Link>
  );
}

function HistoricalStatsTable({
  competition,
  group,
  title,
  valueHeader,
}: {
  competition: CompetitionDef;
  group: CompetitionHistoricalStatGroup;
  title: string;
  valueHeader: string;
}) {
  const rows = group.items.slice(0, HISTORICAL_TABLE_LIMIT);

  if (rows.length === 0) {
    return null;
  }

  return (
    <div className="overflow-hidden rounded-[1.2rem] border border-[rgba(191,201,195,0.48)] bg-white/82">
      <div className="border-b border-[rgba(191,201,195,0.42)] px-4 py-3">
        <h3 className="font-[family:var(--font-profile-headline)] text-[1.25rem] font-extrabold tracking-[-0.03em] text-[#111c2d]">
          {title}
        </h3>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[360px] text-left text-sm">
          <thead className="bg-[rgba(240,243,255,0.66)] text-[0.66rem] uppercase tracking-[0.16em] text-[#57657a]">
            <tr>
              <th className="w-12 px-4 py-3 font-semibold">#</th>
              <th className="px-4 py-3 font-semibold">Nome</th>
              <th className="px-4 py-3 text-right font-semibold">{valueHeader}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[rgba(191,201,195,0.38)]">
            {rows.map((item, index) => (
              <tr key={`${item.statCode}-${item.entityName}-${item.rank ?? index}`}>
                <td className="px-4 py-3 text-[#57657a]">{item.rank ?? index + 1}</td>
                <td className="max-w-[16rem] px-4 py-3">
                  <HistoricalEntityLabel competition={competition} item={item} />
                </td>
                <td className="px-4 py-3 text-right font-semibold text-[#111c2d]">
                  {formatHistoricalValue(item)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function HistoricalRecordCard({
  competition,
  item,
}: {
  competition: CompetitionDef;
  item: CompetitionHistoricalStatItem;
}) {
  return (
    <article className="rounded-[1.2rem] border border-[rgba(191,201,195,0.48)] bg-white/82 px-4 py-4">
      <p className="text-[0.66rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
        {item.label}
      </p>
      <p className="mt-3 font-[family:var(--font-profile-headline)] text-[1.85rem] font-extrabold leading-none tracking-[-0.04em] text-[#111c2d]">
        {formatHistoricalValue(item)}
      </p>
      <div className="mt-4 flex min-w-0 items-center gap-2 text-sm">
        <HistoricalEntityLabel competition={competition} item={item} />
      </div>
      {item.seasonLabel ? (
        <p className="mt-3 text-xs font-semibold uppercase tracking-[0.16em] text-[#57657a]">
          Temporada {item.seasonLabel}
        </p>
      ) : null}
    </article>
  );
}

function HistoricalRecordGroup({
  competition,
  group,
  title,
}: {
  competition: CompetitionDef;
  group: CompetitionHistoricalStatGroup;
  title: string;
}) {
  if (group.items.length === 0) {
    return null;
  }

  return (
    <div className="space-y-3">
      <h3 className="font-[family:var(--font-profile-headline)] text-[1.25rem] font-extrabold tracking-[-0.03em] text-[#111c2d]">
        {title}
      </h3>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {group.items.map((item, index) => (
          <HistoricalRecordCard
            competition={competition}
            item={item}
            key={`${item.statCode}-${item.entityName}-${item.seasonLabel ?? index}`}
          />
        ))}
      </div>
    </div>
  );
}

function CompetitionHistoricalStatsSection({ competition }: { competition: CompetitionDef }) {
  const historicalStatsQuery = useCompetitionHistoricalStats({
    competitionKey: competition.key,
    asOfYear: HISTORICAL_STATS_AS_OF_YEAR,
  });

  if (historicalStatsQuery.isLoading) {
    return (
      <ProfilePanel className="space-y-3">
        <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
          Historico
        </p>
        <p className="text-sm/6 text-[#57657a]">Carregando estatisticas historicas.</p>
      </ProfilePanel>
    );
  }

  const data = historicalStatsQuery.data;

  if (historicalStatsQuery.isError || !data || isHistoricalStatsDataEmpty(data)) {
    return null;
  }

  return (
    <ProfilePanel className="space-y-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Historico
          </p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-[2.15rem] font-extrabold tracking-[-0.05em] text-[#111c2d]">
            Recordes da competicao
          </h2>
          <p className="mt-2 max-w-3xl text-sm/6 text-[#57657a]">
            Dados historicos extraidos da Wikipedia, com IDs internos usados apenas quando a
            correspondencia foi resolvida com seguranca.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <ProfileCoveragePill coverage={historicalStatsQuery.coverage} />
          <ProfileTag>Base {HISTORICAL_STATS_AS_OF_YEAR}</ProfileTag>
        </div>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <HistoricalStatsTable
          competition={competition}
          group={data.champions}
          title="Maiores campeoes"
          valueHeader="Titulos"
        />
        <HistoricalStatsTable
          competition={competition}
          group={data.scorers}
          title="Artilheiros historicos"
          valueHeader="Gols"
        />
      </div>

      <HistoricalRecordGroup competition={competition} group={data.teamRecords} title="Recordes de time" />
      <HistoricalRecordGroup competition={competition} group={data.matchRecords} title="Recordes de jogo" />
      <HistoricalRecordGroup
        competition={competition}
        group={data.playerRecords}
        title="Recordes individuais"
      />
    </ProfilePanel>
  );
}

function resolveChampionFromStandings(rows: StandingsTableRow[]): StandingsTableRow | null {
  return rows.find((row) => row.position === 1) ?? rows[0] ?? null;
}

function resolveChampionTie(ties: StageTie[]): StageTie | null {
  return ties.find((tie) => tie.winnerTeamId || tie.winnerTeamName) ?? ties[0] ?? null;
}

function useSeasonChampionStandings(
  competition: CompetitionDef,
  season: SeasonDef,
  enabled: boolean,
) {
  return useQueryWithCoverage<StandingsTableData>({
    queryKey: standingsQueryKeys.table({
      competitionId: competition.id,
      seasonId: season.queryId,
    }),
    queryFn: () =>
      fetchStandings({
        competitionId: competition.id,
        seasonId: season.queryId,
      }),
    enabled: enabled && Boolean(competition.id && season.queryId),
    staleTime: 5 * 60 * 1000,
    gcTime: 20 * 60 * 1000,
    isDataEmpty: (data) => data.rows.length === 0,
  });
}

function CompetitionHero({
  competition,
  latestSeason,
  seasonsCount,
}: {
  competition: CompetitionDef;
  latestSeason: SeasonDef | null;
  seasonsCount: number;
}) {
  const visualAssetId = getCompetitionVisualAssetId(competition);
  const artwork = latestSeason
    ? resolveSeasonChampionArtwork(competition.key, latestSeason.label)
    : null;

  return (
    <section className="relative isolate overflow-hidden rounded-[2rem] border border-white/65 bg-[linear-gradient(180deg,rgba(255,255,255,0.94)_0%,rgba(247,250,248,0.96)_48%,rgba(237,246,241,0.94)_100%)] p-5 shadow-[0_34px_88px_-58px_rgba(17,28,45,0.28)] md:p-6 xl:p-8">
      <div className="pointer-events-none absolute inset-x-0 top-0 h-52 bg-[radial-gradient(circle_at_top_left,rgba(216,227,251,0.7),transparent_54%),radial-gradient(circle_at_top_right,rgba(139,214,182,0.26),transparent_42%)]" />
      <div className="pointer-events-none absolute bottom-[-18%] right-[12%] h-64 w-64 rounded-full bg-[rgba(0,53,38,0.08)] blur-3xl" />

      <div className="relative grid gap-6 xl:grid-cols-[minmax(0,1.25fr)_minmax(320px,0.75fr)] xl:items-stretch">
        <div className="space-y-6">
          <div className="flex flex-wrap items-center gap-2">
            <ProfileTag className="bg-white text-[#455468]">{competition.region}</ProfileTag>
            <ProfileTag className="bg-white text-[#455468]">{competition.country}</ProfileTag>
            <ProfileTag className="bg-white text-[#455468]">
              {describeCompetitionType(competition)}
            </ProfileTag>
          </div>

          <div className="grid gap-4 sm:grid-cols-[auto_minmax(0,1fr)] sm:items-start">
            <ProfileMedia
              alt={`Logo de ${competition.name}`}
              assetId={visualAssetId}
              category="competitions"
              className="h-20 w-20 shadow-[0_24px_50px_-34px_rgba(17,28,45,0.38)] md:h-24 md:w-24"
              fallback={buildFallbackLabel(competition.shortName)}
              fallbackClassName="text-lg"
              imageClassName="p-3"
            />

            <div className="space-y-3">
              <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#0a3d2c]">
                Hub da competicao
              </p>
              <h1 className="max-w-4xl font-[family:var(--font-profile-headline)] text-[2.8rem] font-extrabold leading-[0.95] tracking-[-0.06em] text-[#111c2d] md:text-[3.55rem]">
                {competition.name}
              </h1>
              <p className="max-w-3xl text-sm/7 text-[#57657a] md:text-[0.98rem]">
                Uma competicao, varias edicoes. Escolha a temporada certa para abrir tabela,
                mata-mata, partidas, rankings e perfis no contexto correto.
              </p>
            </div>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <div className="rounded-[1.35rem] border border-[rgba(191,201,195,0.52)] bg-white/92 px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Temporadas
              </p>
              <p className="mt-2 font-[family:var(--font-profile-headline)] text-[1.8rem] font-extrabold text-[#111c2d]">
                {formatWholeNumber(seasonsCount)}
              </p>
            </div>
            <div className="rounded-[1.35rem] border border-[rgba(191,201,195,0.52)] bg-white/92 px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Ultima edicao
              </p>
              <p className="mt-2 font-[family:var(--font-profile-headline)] text-[1.8rem] font-extrabold text-[#111c2d]">
                {latestSeason?.label ?? "-"}
              </p>
            </div>
            <div className="rounded-[1.35rem] border border-[rgba(191,201,195,0.52)] bg-white/92 px-4 py-4">
              <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Calendario
              </p>
              <p className="mt-2 font-[family:var(--font-profile-headline)] text-[1.8rem] font-extrabold text-[#111c2d]">
                {describeSeasonCalendar(competition)}
              </p>
            </div>
          </div>
        </div>

        <aside className="relative min-h-[320px] overflow-hidden rounded-[1.7rem] border border-[rgba(8,48,35,0.16)] bg-[linear-gradient(135deg,#042f22_0%,#0a4a37_100%)] shadow-[0_34px_84px_-56px_rgba(8,25,20,0.62)]">
          {artwork ? (
            <Image
              alt={`Imagem da edicao ${competition.name} ${latestSeason?.label ?? ""}`}
              className="object-cover object-center"
              fill
              priority
              sizes="(min-width: 1280px) 360px, 100vw"
              src={artwork.src}
            />
          ) : null}
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,rgba(166,242,209,0.2),transparent_30%),linear-gradient(180deg,rgba(4,47,34,0.12)_0%,rgba(4,47,34,0.54)_46%,rgba(4,47,34,0.92)_100%)]" />
          <div className="relative flex h-full min-h-[320px] flex-col justify-between p-5 md:p-6">
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-full border border-white/12 bg-white/10 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-white/88">
                Catalogo de edicoes
              </span>
              {latestSeason ? (
                <span className="rounded-full border border-white/12 bg-white/8 px-3 py-1 text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-white/72">
                  {latestSeason.label}
                </span>
              ) : null}
            </div>

            <div className="space-y-4">
              <ProfileMedia
                alt={`Logo de ${competition.name}`}
                assetId={visualAssetId}
                category="competitions"
                className="h-24 w-24 border-white/16 bg-white/12 text-white"
                fallback={buildFallbackLabel(competition.shortName)}
                fallbackClassName="text-xl text-white"
                imageClassName="p-3"
                tone="contrast"
              />
              <div>
                <p className="text-sm/6 text-[#d7efe4]">
                  Entre por edicao para manter filtros, rankings e calendario sempre no recorte
                  correto.
                </p>
                <p className="mt-3 font-[family:var(--font-profile-headline)] text-[2rem] font-extrabold tracking-[-0.04em] text-white">
                  {seasonsCount > 0 ? `${seasonsCount} temporadas disponiveis` : "Sem temporadas"}
                </p>
              </div>
            </div>
          </div>
        </aside>
      </div>
    </section>
  );
}

function SeasonCardStat({
  label,
  value,
}: {
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-[1.05rem] border border-[rgba(191,201,195,0.44)] bg-[rgba(246,248,252,0.82)] px-3 py-3">
      <p className="text-[0.62rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">{label}</p>
      <p className="mt-1.5 truncate font-[family:var(--font-profile-headline)] text-[1.05rem] font-extrabold tracking-[-0.03em] text-[#111c2d]">
        {value}
      </p>
    </div>
  );
}

function SeasonCard({
  competition,
  isLatestSeason,
  season,
}: {
  competition: CompetitionDef;
  isLatestSeason: boolean;
  season: SeasonDef;
}) {
  const structureEnabled = competition.type !== "domestic_league";
  const structureQuery = useCompetitionStructure(
    {
      competitionKey: competition.key,
      seasonLabel: season.label,
    },
    {
      enabled: structureEnabled,
    },
  );
  const resolution = useMemo(
    () =>
      resolveCompetitionSeasonSurface({
        competitionType: competition.type,
        structure: structureQuery.data,
      }),
    [competition.type, structureQuery.data],
  );
  const analyticsQuery = useCompetitionAnalytics({
    competitionKey: competition.key,
    seasonLabel: season.label,
  });
  const standingsQuery = useSeasonChampionStandings(
    competition,
    season,
    competition.type === "domestic_league" || resolution.type === "league",
  );
  const finalTiesQuery = useStageTies({
    competitionKey: competition.key,
    seasonLabel: season.label,
    stageId: resolution.finalKnockoutStage?.stageId,
  });

  const championRow = resolveChampionFromStandings(standingsQuery.data?.rows ?? []);
  const championTie = resolveChampionTie(finalTiesQuery.data?.ties ?? []);
  const shouldUseStandingsChampion =
    competition.type === "domestic_league" || resolution.type === "league";
  const isChampionLoading =
    shouldUseStandingsChampion
      ? standingsQuery.isLoading
      : structureQuery.isLoading || finalTiesQuery.isLoading;
  const championName = isChampionLoading
    ? "..."
    : shouldUseStandingsChampion
      ? (championRow?.teamName ?? "Nao identificado")
      : (championTie?.winnerTeamName ?? "Nao identificado");
  const championTeamId =
    shouldUseStandingsChampion ? championRow?.teamId : championTie?.winnerTeamId;
  const formatLabel =
    structureQuery.isLoading && structureEnabled
      ? "..."
      : resolution.editionLabel ?? describeCompetitionFormatFallback(competition);
  const matchCount = analyticsQuery.isLoading
    ? "..."
    : formatWholeNumber(analyticsQuery.data?.seasonSummary.matchCount);
  const seasonHref = buildSeasonHubPath({
    competitionKey: competition.key,
    seasonLabel: season.label,
  });

  return (
    <Link
      className={joinClasses(
        "group relative overflow-hidden rounded-[1.55rem] border px-4 py-4 shadow-[0_18px_58px_-46px_rgba(17,28,45,0.18)] transition-[transform,border-color,background-color,box-shadow] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-1 hover:border-[#8bd6b6] hover:bg-white hover:shadow-[0_28px_68px_-44px_rgba(17,28,45,0.28)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-[#00513b] active:scale-[0.985]",
        isLatestSeason
          ? "border-[#8bd6b6] bg-[linear-gradient(180deg,rgba(245,255,250,0.96)_0%,rgba(240,243,255,0.92)_100%)]"
          : "border-[rgba(191,201,195,0.52)] bg-white/88",
      )}
      href={seasonHref}
    >
      <div className="pointer-events-none absolute inset-x-0 top-0 h-28 bg-[radial-gradient(circle_at_top_left,rgba(139,214,182,0.22),transparent_52%),linear-gradient(180deg,rgba(240,250,246,0.9)_0%,transparent_100%)]" />
      <div className="relative space-y-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-[0.68rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
              Temporada
            </p>
            <h3 className="mt-2 font-[family:var(--font-profile-headline)] text-[2.35rem] font-extrabold leading-none tracking-[-0.06em] text-[#111c2d]">
              {season.label}
            </h3>
          </div>
          <div className="flex flex-wrap justify-end gap-2">
            {isLatestSeason ? <ProfileTag>Mais recente</ProfileTag> : null}
            <ProfileTag>{season.calendar === "annual" ? "Anual" : "Cruzada"}</ProfileTag>
          </div>
        </div>

        <div className="rounded-[1.2rem] border border-[rgba(191,201,195,0.44)] bg-white/86 px-3 py-3">
          <div className="flex items-center gap-3">
            <ProfileMedia
              alt={`Campeao ${championName}`}
              assetId={championTeamId}
              category="clubs"
              className="h-11 w-11 rounded-full"
              fallback={buildFallbackLabel(championName)}
              imageClassName="p-1.5"
              shape="circle"
            />
            <div className="min-w-0">
              <p className="text-[0.62rem] font-semibold uppercase tracking-[0.16em] text-[#57657a]">
                Campeao
              </p>
              <p className="mt-1 truncate font-[family:var(--font-profile-headline)] text-[1.35rem] font-extrabold tracking-[-0.04em] text-[#111c2d]">
                {championName}
              </p>
            </div>
          </div>
        </div>

        <div className="grid gap-2 sm:grid-cols-2">
          <SeasonCardStat label="Formato" value={formatLabel} />
          <SeasonCardStat label="Partidas" value={matchCount} />
        </div>

        <div className="flex items-center justify-between border-t border-[rgba(191,201,195,0.4)] pt-4 text-[0.72rem] font-semibold uppercase tracking-[0.16em] text-[#003526]">
          <span>Abrir edicao</span>
          <span className="transition-transform group-hover:translate-x-1">-&gt;</span>
        </div>
      </div>
    </Link>
  );
}

function SeasonsGrid({
  competition,
  latestSeason,
  seasons,
}: {
  competition: CompetitionDef;
  latestSeason: SeasonDef | null;
  seasons: SeasonDef[];
}) {
  if (seasons.length === 0) {
    return (
      <ProfilePanel className="space-y-4">
        <EmptyState
          className="rounded-[1.2rem] border-[rgba(191,201,195,0.55)] bg-[rgba(240,243,255,0.88)]"
          description="Nao encontramos temporadas disponiveis para esta competicao no catalogo atual."
          title="Sem temporadas"
        />
      </ProfilePanel>
    );
  }

  return (
    <ProfilePanel className="space-y-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.18em] text-[#57657a]">
            Edicoes disponiveis
          </p>
          <h2 className="mt-2 font-[family:var(--font-profile-headline)] text-[2.15rem] font-extrabold tracking-[-0.05em] text-[#111c2d]">
            Escolha a temporada
          </h2>
          <p className="mt-2 max-w-3xl text-sm/6 text-[#57657a]">
            Cada card abre uma edicao especifica, preservando o contexto correto para partidas,
            tabela, mata-mata, rankings, times e jogadores.
          </p>
        </div>
        <Link
          className="inline-flex items-center justify-center rounded-full border border-[rgba(191,201,195,0.56)] bg-white px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#455468] transition-[transform,border-color,background-color] duration-200 ease-[cubic-bezier(0.23,1,0.32,1)] hover:-translate-y-0.5 hover:border-[#8bd6b6] hover:bg-[#f7fbf8] active:scale-[0.985]"
          href="/competitions"
        >
          Voltar ao catalogo
        </Link>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {seasons.map((season) => (
          <SeasonCard
            competition={competition}
            isLatestSeason={latestSeason?.id === season.id}
            key={season.id}
            season={season}
          />
        ))}
      </div>
    </ProfilePanel>
  );
}

export function CompetitionHubContent({ competition }: CompetitionHubContentProps) {
  const seasons = listSeasonsForCompetition(competition);
  const latestSeason = getLatestSeasonForCompetition(competition) ?? null;

  return (
    <CompetitionRouteContextSync competition={competition}>
      <ProfileShell className="space-y-6" variant="plain">
        <div className="flex flex-wrap items-center gap-2 text-[0.78rem] font-semibold uppercase tracking-[0.16em] text-[#455468]">
          <Link className="transition-colors hover:text-[#003526]" href="/competitions">
            Competicoes
          </Link>
          <span className="text-[#8fa097]">/</span>
          <span>{competition.shortName}</span>
        </div>

        <CompetitionHero
          competition={competition}
          latestSeason={latestSeason}
          seasonsCount={seasons.length}
        />

        <SeasonsGrid competition={competition} latestSeason={latestSeason} seasons={seasons} />

        <CompetitionHistoricalStatsSection competition={competition} />
      </ProfileShell>
    </CompetitionRouteContextSync>
  );
}
