import { TeamRouteResolver } from "@/app/(platform)/teams/[teamId]/TeamRouteResolver";

type ClubDetailsPageProps = {
  params: Promise<{ clubId: string }>;
};

export default async function ClubDetailsPage({ params }: ClubDetailsPageProps) {
  const { clubId } = await params;

  return <TeamRouteResolver teamId={clubId} />;
}
