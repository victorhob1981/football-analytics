import { getCompetitionByKey } from "@/config/competitions.registry";
import { CompetitionHubContent } from "@/features/competitions/components/CompetitionHubContent";
import { ProfileAlert, ProfileShell } from "@/shared/components/profile/ProfilePrimitives";

type CompetitionHubPageProps = {
  params: Promise<{
    competitionKey: string;
  }>;
};

export default async function CompetitionHubPage({ params }: CompetitionHubPageProps) {
  const { competitionKey } = await params;
  const competition = getCompetitionByKey(competitionKey);

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

  return <CompetitionHubContent competition={competition} />;
}
