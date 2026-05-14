import { getRankingDefinition, RANKING_DEFINITIONS } from "@/config/ranking.registry";
import { RankingTable } from "@/features/rankings/components";

type RankingsPageProps = {
  params: Promise<{ rankingType: string }>;
};

export default async function RankingsPage({ params }: RankingsPageProps) {
  const { rankingType } = await params;
  const rankingDefinition = getRankingDefinition(rankingType);

  if (!rankingDefinition) {
    return (
      <main className="space-y-3">
        <h1 className="text-xl font-semibold">Ranking nao encontrado</h1>
        <p className="text-sm text-slate-600">Tipo solicitado: {rankingType}</p>
        <section className="rounded-md border border-slate-200 bg-white p-4 text-sm">
          <p className="mb-2 font-medium text-slate-800">Rankings disponiveis no registry:</p>
          <ul className="list-disc space-y-1 pl-5 text-slate-600">
            {RANKING_DEFINITIONS.map((ranking) => (
              <li key={ranking.id}>{ranking.id}</li>
            ))}
          </ul>
        </section>
      </main>
    );
  }

  return <RankingTable rankingDefinition={rankingDefinition} />;
}
