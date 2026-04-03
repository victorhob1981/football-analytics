import { TeamRouteResolver } from "./TeamRouteResolver";

type TeamResolverPageProps = {
  params: Promise<{ teamId: string }>;
};

export default async function TeamResolverPage({ params }: TeamResolverPageProps) {
  const { teamId } = await params;

  return <TeamRouteResolver teamId={teamId} />;
}
