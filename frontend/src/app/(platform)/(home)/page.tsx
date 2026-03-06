"use client";

import Link from "next/link";

import {
  CoverageSummarySection,
  HomeInsightsSection,
  HomeKpiSection,
  HomeSectionShell,
  StandingsEvolutionSection,
  TopPlayersSection,
  TopTeamsSection,
} from "@/features/home/components";
import { GlobalFilterBar } from "@/shared/components/filters/GlobalFilterBar";

export default function PlatformHomePage() {
  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-6">
        <header>
          <h1 className="text-2xl font-semibold tracking-tight text-slate-100">Visao Geral</h1>
          <p className="mt-1 text-sm text-slate-300">
            Resumo executivo da temporada em curso
          </p>
        </header>

        <GlobalFilterBar />

        <HomeKpiSection />

        <HomeSectionShell
          subtitle="Melhores ataques e defesas no recorte atual"
          title="Destaques de Times"
        >
          <TopTeamsSection />
        </HomeSectionShell>

        <HomeSectionShell
          subtitle="Pontos acumulados por rodada e comparacao entre clubes"
          title="Evolucao da Classificacao"
        >
          <StandingsEvolutionSection />
        </HomeSectionShell>

        <div className="grid gap-6 lg:grid-cols-[1fr_360px]">
          <HomeSectionShell
            subtitle="Lideres de gols e assistencias no recorte selecionado"
            title="Top Jogadores"
            action={
              <Link
                className="text-xs font-medium text-emerald-300 no-underline hover:text-emerald-200 hover:underline"
                href="/players"
              >
                Ver todos
              </Link>
            }
          >
            <TopPlayersSection />
          </HomeSectionShell>

          <HomeSectionShell
            subtitle="Alertas e tendencias detectados automaticamente"
            title="Insights da Rodada"
          >
            <HomeInsightsSection />
          </HomeSectionShell>
        </div>

        <HomeSectionShell
          subtitle="Qualidade e completude dos dados por modulo"
          title="Cobertura de Dados"
          action={
            <Link
              className="text-xs font-medium text-emerald-300 no-underline hover:text-emerald-200 hover:underline"
              href="/audit"
            >
              Ver painel completo
            </Link>
          }
        >
          <CoverageSummarySection />
        </HomeSectionShell>
      </div>
    </div>
  );
}
