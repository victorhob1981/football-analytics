type CoachPageProps = {
  params: Promise<{ coachId: string }>;
};

export default async function CoachPage({ params }: CoachPageProps) {
  const { coachId } = await params;

  return (
    <main>
      <h1>Rota: /coaches/{coachId}</h1>
      <p>TODO: Placeholder detalhe de tecnico.</p>
    </main>
  );
}
