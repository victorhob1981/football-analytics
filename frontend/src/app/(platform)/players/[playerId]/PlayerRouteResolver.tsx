"use client";

import { useEffect, useMemo } from "react";

import Link from "next/link";
import { useSearchParams } from "next/navigation";

import { usePlayerContexts } from "@/features/players/hooks";
import { PlatformStateSurface } from "@/shared/components/feedback/PlatformStateSurface";
import { useResolvedCompetitionContext } from "@/shared/hooks/useResolvedCompetitionContext";
import {
  buildCanonicalPlayerPath,
  buildRetainedFilterQueryString,
  resolveCompetitionSeasonContextFromSearchParams,
  resolveCompetitionSeasonContext,
} from "@/shared/utils/context-routing";

type PlayerRouteResolverProps = {
  playerId: string;
};

function buildPlayerResolverFailureCopy(isError: boolean, status?: number) {
  if (status === 404) {
    return {
      title: "Jogador indisponivel",
      description: "Este jogador nao esta disponivel no produto agora.",
      detail:
        "Volte para a lista de jogadores ou abra competicoes para seguir por uma visao disponivel.",
    };
  }

  if (isError) {
    return {
      title: "Nao foi possivel abrir este jogador",
      description: "Nao conseguimos carregar o caminho certo para este perfil agora.",
      detail: "Tente novamente em instantes ou continue pela lista de jogadores.",
    };
  }

  return {
    title: "Nao foi possivel abrir este jogador",
    description: "Nao encontramos um caminho disponivel para abrir este perfil agora.",
    detail: "Abra a lista de jogadores ou competicoes para continuar a navegacao.",
  };
}

export function PlayerRouteResolver({ playerId }: PlayerRouteResolverProps) {
  const searchParams = useSearchParams();
  const globalContext = useResolvedCompetitionContext();
  const retainedFilterQueryString = useMemo(
    () => buildRetainedFilterQueryString(searchParams),
    [searchParams],
  );
  const currentQueryString = useMemo(() => {
    const serialized = searchParams.toString();
    return serialized.length > 0 ? `?${serialized}` : "";
  }, [searchParams]);

  const localContext = useMemo(
    () => resolveCompetitionSeasonContextFromSearchParams(searchParams) ?? globalContext,
    [globalContext, searchParams],
  );
  const preferredContextFilters = useMemo(
    () => ({
      competitionId: searchParams.get("competitionId")?.trim() || globalContext?.competitionId,
      seasonId: searchParams.get("seasonId")?.trim() || globalContext?.seasonId,
    }),
    [globalContext?.competitionId, globalContext?.seasonId, searchParams],
  );
  const contextsQuery = usePlayerContexts(playerId, preferredContextFilters, !localContext);
  const resolvedContext = useMemo(() => {
    const contextCandidate = localContext ?? contextsQuery.data?.defaultContext ?? null;

    if (!contextCandidate) {
      return null;
    }

    return (
      resolveCompetitionSeasonContext({
        competitionId: contextCandidate.competitionId,
        competitionKey: contextCandidate.competitionKey,
        seasonId: contextCandidate.seasonId,
        seasonLabel: contextCandidate.seasonLabel,
      }) ?? contextCandidate
    );
  }, [contextsQuery.data?.defaultContext, localContext]);

  const canonicalHref = useMemo(
    () =>
      resolvedContext
        ? `${buildCanonicalPlayerPath(resolvedContext, playerId)}${retainedFilterQueryString}`
        : null,
    [playerId, resolvedContext, retainedFilterQueryString],
  );

  useEffect(() => {
    if (!canonicalHref) {
      return;
    }

    const currentHref = `${window.location.pathname}${window.location.search}`;
    if (currentHref === canonicalHref) {
      return;
    }

    window.location.replace(canonicalHref);
  }, [canonicalHref]);

  if (canonicalHref) {
    return (
      <PlatformStateSurface
        description="Estamos levando voce para o perfil do jogador na melhor temporada disponivel."
        kicker="Abrindo perfil"
        loading
        title="Abrindo jogador"
      />
    );
  }

  if (!localContext && contextsQuery.isLoading) {
    return (
      <PlatformStateSurface
        description="Estamos encontrando a competicao e a temporada certas para abrir este jogador."
        kicker="Abrindo perfil"
        loading
        title="Preparando jogador"
      />
    );
  }

  const failureCopy = buildPlayerResolverFailureCopy(
    contextsQuery.isError,
    contextsQuery.error?.status,
  );

  return (
    <PlatformStateSurface
      actionHref={`/players${currentQueryString}`}
      actionLabel="Voltar para jogadores"
      description={failureCopy.description}
      detail={<p>{failureCopy.detail}</p>}
      kicker="Navegacao"
      secondaryAction={
        <Link
          className="inline-flex items-center rounded-full border border-[rgba(112,121,116,0.28)] bg-white/92 px-4 py-2 text-xs font-semibold uppercase tracking-[0.16em] text-[#1f2d40]"
          href={`/competitions${currentQueryString}`}
        >
          Abrir competições
        </Link>
      }
      title={failureCopy.title}
      tone={contextsQuery.isError ? "critical" : "warning"}
    />
  );
}
