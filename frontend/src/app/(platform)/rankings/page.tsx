import { redirect } from "next/navigation";

import { RANKING_DEFINITIONS } from "@/config/ranking.registry";

export default function RankingsEntryPage() {
  const defaultRankingId = RANKING_DEFINITIONS[0]?.id ?? "player-goals";
  redirect(`/rankings/${defaultRankingId}`);
}
