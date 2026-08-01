import { ContextualTeamRouteResolver } from "@/features/teams/components/ContextualTeamRouteResolver";
import { loadTeamHonorsPreview } from "@/features/teams/server/teamHonorsPreview";
import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";
import { CanonicalRouteContextSync } from "@/shared/components/routing/CanonicalRouteContextSync";
import { resolveCompetitionSeasonContext } from "@/shared/utils/context-routing";

type CanonicalClubProfilePageProps = {
  params: Promise<{
    competitionKey: string;
    seasonLabel: string;
    clubId: string;
  }>;
};

export default async function CanonicalClubProfilePage({ params }: CanonicalClubProfilePageProps) {
  const { competitionKey, seasonLabel, clubId } = await params;
  const honorsPreview = await loadTeamHonorsPreview(clubId);
  const context = resolveCompetitionSeasonContext({
    competitionKey,
    seasonLabel,
  });

  if (!context) {
    return (
      <PlatformStateSurface
        actionHref="/competitions"
        actionLabel="Ir para competições"
        description="Esta temporada não corresponde a um contexto válido para abrir o clube."
        kicker="Clube"
        title="Perfil de clube indisponível"
        tone="critical"
      />
    );
  }

  return (
    <CanonicalRouteContextSync context={context}>
      <ContextualTeamRouteResolver
        context={context}
        honorsPreview={honorsPreview}
        surface="clubs"
        teamId={clubId}
      />
    </CanonicalRouteContextSync>
  );
}
