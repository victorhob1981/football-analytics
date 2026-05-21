import { PlayerProfileContent } from "@/app/(platform)/players/[playerId]/PlayerProfileContent";
import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";
import { CanonicalRouteContextSync } from "@/shared/components/routing/CanonicalRouteContextSync";
import { resolveCompetitionSeasonContext } from "@/shared/utils/context-routing";

type CanonicalPlayerProfilePageProps = {
  params: Promise<{
    competitionKey: string;
    playerId: string;
    seasonLabel: string;
  }>;
};

export default async function CanonicalPlayerProfilePage({
  params,
}: CanonicalPlayerProfilePageProps) {
  const { competitionKey, playerId, seasonLabel } = await params;
  const context = resolveCompetitionSeasonContext({
    competitionKey,
    seasonLabel,
  });

  if (!context) {
    return (
      <PlatformStateSurface
        actionHref="/competitions"
        actionLabel="Ir para competições"
        description="Esta temporada não corresponde a um contexto válido para abrir o jogador."
        kicker="Jogador"
        title="Perfil de jogador indisponível"
        tone="critical"
      />
    );
  }

  return (
    <CanonicalRouteContextSync context={context}>
      <PlayerProfileContent contextOverride={context} playerId={playerId} />
    </CanonicalRouteContextSync>
  );
}
