"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import type { ReactNode } from "react";

import type { RankingDefinition } from "@/config/ranking.types";
import { buildRankingPath } from "@/shared/utils/context-routing";

import styles from "./page.module.css";

function readSearchParam(searchParams: Pick<URLSearchParams, "get">, key: string): string | null {
  const value = searchParams.get(key)?.trim() ?? "";
  return value && value.toLowerCase() !== "all" ? value : null;
}

function parseLastNValue(value: string | null): number | null {
  if (!value) {
    return null;
  }

  const parsedValue = Number.parseInt(value, 10);
  return Number.isInteger(parsedValue) && parsedValue > 0 ? parsedValue : null;
}

export function RankingCatalogLink({
  children,
  rankingId,
}: {
  children: ReactNode;
  rankingId: RankingDefinition["id"];
}) {
  const searchParams = useSearchParams();

  return (
    <Link
      className={styles.catalogCard}
      href={buildRankingPath(rankingId, {
        competitionId: readSearchParam(searchParams, "competitionId"),
        seasonId: readSearchParam(searchParams, "seasonId"),
        roundId: readSearchParam(searchParams, "roundId"),
        venue: readSearchParam(searchParams, "venue"),
        lastN: parseLastNValue(readSearchParam(searchParams, "lastN")),
        dateRangeStart: readSearchParam(searchParams, "dateRangeStart"),
        dateRangeEnd: readSearchParam(searchParams, "dateRangeEnd"),
      })}
    >
      {children}
    </Link>
  );
}
