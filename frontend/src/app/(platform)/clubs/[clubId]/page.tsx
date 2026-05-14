type ClubDetailsPageProps = {
  params: Promise<{ clubId: string }>;
};

export default async function ClubDetailsPage({ params }: ClubDetailsPageProps) {
  const { clubId } = await params;

  return (
    <main>
      <h1>Rota: /clubs/{clubId}</h1>
      <p>TODO: Placeholder detalhe de clube.</p>
    </main>
  );
}
