import Link from "next/link";

import { getRankingDefinition, RANKING_DEFINITIONS, listRankingsByEntity } from "@/config/ranking.registry";
import { RankingTable } from "@/features/rankings/components";

type RankingsPageProps = { params: Promise<{ rankingType: string }> };

const ENTITY_LABELS: Record<string, string> = {
  player: "Jogadores",
  team: "Clubes",
  coach: "Treinadores",
};

export default async function RankingsPage({ params }: RankingsPageProps) {
  const { rankingType } = await params;
  const rankingDefinition = getRankingDefinition(rankingType);

  const playerRankings = listRankingsByEntity("player");
  const teamRankings = listRankingsByEntity("team");

  // ── NavBar de categorias ─────────────────────────────────────────────────

  const CategoryNav = (
    <nav className="rounded-xl border border-slate-700/80 bg-slate-900/80 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
      {/* Tabs de entidade */}
      <div className="flex border-b border-slate-700/50 px-2 pt-2">
        {[
          { label: "Jogadores", items: playerRankings },
          { label: "Clubes", items: teamRankings },
        ].filter((g) => g.items.length > 0).map((group) => {
          const isGroupActive = group.items.some((r) => r.id === rankingType);
          return (
            <Link
              className={`rounded-t-md px-4 py-2 text-xs font-semibold transition-colors no-underline ${isGroupActive
                  ? "border-b-2 border-emerald-400 text-emerald-300"
                  : "text-slate-500 hover:text-slate-300"
                }`}
              href={group.items[0] ? `/rankings/${group.items[0].id}` : "#"}
              key={group.label}
            >
              {group.label}
            </Link>
          );
        })}
      </div>

      {/* Pills de ranking */}
      <div className="flex flex-wrap gap-1.5 p-3">
        {RANKING_DEFINITIONS.filter((r) => r.entity === (rankingDefinition?.entity ?? playerRankings[0]?.entity ?? "player")).map((r) => (
          <Link
            className={`rounded-full px-3 py-1 text-[11px] font-medium no-underline transition-colors ${r.id === rankingType
                ? "bg-emerald-500/20 text-emerald-300"
                : "border border-slate-700 bg-slate-800/60 text-slate-400 hover:border-slate-500 hover:text-slate-200"
              }`}
            href={`/rankings/${r.id}`}
            key={r.id}
          >
            {r.label}
          </Link>
        ))}
      </div>
    </nav>
  );

  // ── Ranking não encontrado ────────────────────────────────────────────────

  if (!rankingDefinition) {
    return (
      <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
        <div className="mx-auto max-w-5xl space-y-5">
          <header>
            <h1 className="text-2xl font-semibold text-slate-100">Central de Rankings</h1>
            <p className="mt-1 text-sm text-slate-400">Ranking não encontrado: <code className="font-mono text-slate-300">{rankingType}</code></p>
          </header>
          {CategoryNav}
          <div className="rounded-xl border border-slate-700 bg-slate-900 p-8 text-center text-sm text-slate-500">
            Selecione um ranking no menu acima.
          </div>
        </div>
      </div>
    );
  }

  // ── Página principal ──────────────────────────────────────────────────────

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-5xl space-y-5">

        {/* Header */}
        <header className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              {ENTITY_LABELS[rankingDefinition.entity] ?? rankingDefinition.entity}
            </p>
            <h1 className="mt-1 text-2xl font-semibold tracking-tight text-slate-100">
              {rankingDefinition.label}
            </h1>
            <p className="mt-1 text-sm text-slate-400">{rankingDefinition.description}</p>
          </div>
        </header>

        {/* NavBar de categorias */}
        {CategoryNav}

        {/* Tabela de ranking */}
        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <RankingTable rankingDefinition={rankingDefinition} />
        </section>

      </div>
    </div>
  );
}
