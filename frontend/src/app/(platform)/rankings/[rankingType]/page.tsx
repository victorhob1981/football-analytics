import Link from "next/link";

import { getRankingDefinition, RANKING_DEFINITIONS } from "@/config/ranking.registry";
import { RankingTable } from "@/features/rankings/components";
import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { ProfilePanel, ProfileShell } from "@/shared/components/profile/ProfilePrimitives";

type RankingsPageProps = {
  params: Promise<{ rankingType: string }>;
};

export default async function RankingsPage({ params }: RankingsPageProps) {
  const { rankingType } = await params;
  const rankingDefinition = getRankingDefinition(rankingType);

  if (!rankingDefinition) {
    return (
      <ProfileShell className="space-y-6">
        <header className="space-y-2">
          <p className="text-[0.72rem] font-semibold uppercase tracking-[0.22em] text-[#57657a]">
            Rankings
          </p>
          <h1 className="font-[family:var(--font-profile-headline)] text-4xl font-extrabold tracking-tight text-[#111c2d]">
            Ranking indisponivel
          </h1>
          <p className="text-sm text-[#57657a]">
            O ranking solicitado nao esta disponivel nesta versao do produto.
          </p>
        </header>
        <ProfilePanel className="space-y-4 text-sm text-[#1f2d40]">
          <EmptyState
            title="Ranking nao encontrado"
            description={`Nao foi possivel abrir o ranking "${rankingType}". Escolha uma das leituras disponiveis abaixo.`}
          />
          <div className="grid gap-2 sm:grid-cols-2">
            {RANKING_DEFINITIONS.map((ranking) => (
              <Link
                className="rounded-[1.1rem] border border-[rgba(191,201,195,0.55)] bg-white/80 px-4 py-3 transition-colors hover:border-[#8bd6b6] hover:bg-white"
                href={`/rankings/${ranking.id}`}
                key={ranking.id}
              >
                <p className="font-semibold text-[#111c2d]">{ranking.label}</p>
                <p className="mt-1 text-xs uppercase tracking-[0.16em] text-[#57657a]">
                  {ranking.id}
                </p>
              </Link>
            ))}
          </div>
        </ProfilePanel>
      </ProfileShell>
    );
  }

  return <RankingTable rankingDefinition={rankingDefinition} />;
}
