import { buildCompetitionDefinition, getCompetitionByKey } from "@/config/competitions.registry";
import { CompetitionHubContent } from "@/features/competitions/components/CompetitionHubContent";
import { fetchHomePage } from "@/features/home/services/home.service";
import type { HomeCompetitionCard } from "@/features/home/types/home.types";
import { ProfileAlert, ProfileShell } from "@/shared/components/profile/ProfilePrimitives";

type CompetitionHubPageProps = {
  params: Promise<{
    competitionKey: string;
  }>;
};

export default async function CompetitionHubPage({ params }: CompetitionHubPageProps) {
  const { competitionKey } = await params;
  let competition = getCompetitionByKey(competitionKey);
  let catalogCompetition: HomeCompetitionCard | undefined;

  if (!competition) {
    try {
      const response = await fetchHomePage();
      catalogCompetition = response.data.competitions.find(
        (item) => item.competitionKey === competitionKey,
      );
      if (catalogCompetition) {
        competition = buildCompetitionDefinition({
          id: catalogCompetition.competitionId,
          key: catalogCompetition.competitionKey,
          name: catalogCompetition.competitionName,
          shortName: catalogCompetition.competitionName,
          country: catalogCompetition.country ?? undefined,
          region: catalogCompetition.region ?? undefined,
          scope: catalogCompetition.scope ?? undefined,
          type:
            catalogCompetition.type === "domestic_cup" ||
            catalogCompetition.type === "international_cup"
              ? catalogCompetition.type
              : "domestic_league",
          visualAssetId: catalogCompetition.assetId ?? undefined,
          seasonCalendar: catalogCompetition.latestContext?.seasonLabel?.includes("/")
            ? "split_year"
            : "annual",
        });
      }
    } catch {
      // The catalog error is rendered below as an unavailable competition.
    }
  }

  if (!competition) {
    return (
      <ProfileShell className="space-y-6">
        <ProfileAlert title="Competição não encontrada" tone="critical">
          Não encontramos essa competição no catálogo atual. Volte para a lista e escolha outra
          opção.
        </ProfileAlert>
      </ProfileShell>
    );
  }

  return (
    <CompetitionHubContent
      catalogCompetition={catalogCompetition}
      competition={competition}
    />
  );
}
