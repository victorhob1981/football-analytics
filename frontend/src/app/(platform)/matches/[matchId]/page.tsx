import { MatchCenterContent } from "./MatchCenterContent";

type MatchCenterPageProps = {
  params: Promise<{ matchId: string }>;
};

export default async function MatchCenterPage({ params }: MatchCenterPageProps) {
  const { matchId } = await params;

  return <MatchCenterContent matchId={matchId} />;
}
