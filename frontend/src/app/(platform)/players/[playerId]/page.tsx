import { PlayerRouteResolver } from "./PlayerRouteResolver";

type PlayerDetailsPageProps = {
  params: Promise<{ playerId: string }>;
};

export default async function PlayerDetailsPage({ params }: PlayerDetailsPageProps) {
  const { playerId } = await params;

  return <PlayerRouteResolver playerId={playerId} />;
}
