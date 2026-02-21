import { PlayerProfileContent } from "./PlayerProfileContent";

type PlayerDetailsPageProps = {
  params: Promise<{ playerId: string }>;
};

export default async function PlayerDetailsPage({ params }: PlayerDetailsPageProps) {
  const { playerId } = await params;

  return <PlayerProfileContent playerId={playerId} />;
}
