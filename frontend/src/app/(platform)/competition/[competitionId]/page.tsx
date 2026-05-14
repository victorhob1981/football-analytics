type CompetitionPageProps = {
  params: Promise<{ competitionId: string }>;
};

export default async function CompetitionPage({ params }: CompetitionPageProps) {
  const { competitionId } = await params;

  return (
    <main>
      <h1>Rota: /competition/{competitionId}</h1>
      <p>TODO: Placeholder Competition.</p>
    </main>
  );
}
