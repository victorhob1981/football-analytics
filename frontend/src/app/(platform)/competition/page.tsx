"use client";

import { useEffect, useMemo, useState, type ElementType } from "react";

import { Activity, Shield, Swords, Trophy } from "lucide-react";
import {
  CartesianGrid,
  Line,
  LineChart as RechartsLineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { FixturesList } from "@/features/competition/components/FixturesList";
import { RoundNavigator } from "@/features/competition/components/RoundNavigator";
import { StandingsTable } from "@/features/competition/components/StandingsTable";
import type { StandingRow } from "@/features/competition/components/StandingsTable";
import { competitionQueryKeys } from "@/features/competition/queryKeys";
import { fetchCompetitionFixtures, fetchStandings, fetchStandingsEvolution } from "@/features/competition/services";
import type { CompetitionFilters, FixtureCard, StandingsEvolutionPoint, StandingsSnapshot } from "@/features/competition/types";
import { CoverageBadge } from "@/shared/components/coverage/CoverageBadge";
import { useGlobalFiltersState } from "@/shared/hooks/useGlobalFilters";
import { useQueryWithCoverage } from "@/shared/hooks/useQueryWithCoverage";
import { useTimeRange } from "@/shared/hooks/useTimeRange";
import type { CoverageState } from "@/shared/types/coverage.types";

const TEAM_COLORS = ["#10B981", "#1E40AF", "#F97316", "#06B6D4", "#EC4899", "#8B5CF6"];

type EvolutionMetric = "points" | "position";

type MiniKpiProps = {
  label: string;
  value: string | number;
  accent: string;
  icon: ElementType;
};

function MiniKpi({ label, value, accent, icon: Icon }: MiniKpiProps) {
  return (
    <article className="stat-card hover-lift" style={{ borderLeftColor: accent }}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-sm font-medium text-slate-400">{label}</p>
          <p className="mt-2 text-3xl font-semibold text-slate-100">{value}</p>
        </div>
        <div className="rounded-lg p-2" style={{ backgroundColor: `${accent}1f` }}>
          <Icon className="h-6 w-6" color={accent} />
        </div>
      </div>
    </article>
  );
}

function toPositiveInteger(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = typeof value === "number" ? value : Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function normalizeRecentForm(form?: string[] | null): Array<"W" | "D" | "L"> {
  if (!Array.isArray(form)) {
    return [];
  }

  const normalized: Array<"W" | "D" | "L"> = [];
  for (const result of form) {
    const parsed = String(result).trim().toUpperCase();
    if (parsed === "W" || parsed === "D" || parsed === "L") {
      normalized.push(parsed);
    }
  }

  return normalized.slice(0, 5);
}

function resolveZone(
  zone: StandingsSnapshot["zone"],
  position: number,
  totalTeams: number,
): StandingRow["zone"] {
  if (zone === "title" || zone === "libertadores" || zone === "sulamericana" || zone === "relegation") {
    return zone;
  }

  if (position === 1) {
    return "title";
  }

  if (position <= Math.min(6, totalTeams)) {
    return "libertadores";
  }

  if (position >= Math.max(1, totalTeams - 3)) {
    return "relegation";
  }

  if (position <= Math.min(12, totalTeams)) {
    return "sulamericana";
  }

  return null;
}

function toStandingRow(row: StandingsSnapshot, totalTeams: number): StandingRow {
  return {
    teamId: row.teamId,
    teamName: row.teamName,
    position: row.position,
    points: row.points,
    matchesPlayed: row.matchesPlayed,
    wins: row.wins,
    draws: row.draws,
    losses: row.losses,
    goalsFor: row.goalsFor,
    goalsAgainst: row.goalsAgainst,
    goalDifference: row.goalDifference,
    recentForm: normalizeRecentForm(row.recentForm),
    zone: resolveZone(row.zone, row.position, totalTeams),
  };
}

function buildEvolutionChartData(
  evolution: StandingsEvolutionPoint[],
  teamNames: string[],
  maxRound: number,
  metricMode: EvolutionMetric,
): Array<Record<string, string | number>> {
  const scopedTeamNames = new Set(teamNames);
  const roundMap = new Map<number, Record<string, string | number>>();

  for (let round = 1; round <= maxRound; round += 1) {
    roundMap.set(round, { roundLabel: `R${round}` });
  }

  for (const point of evolution) {
    if (!scopedTeamNames.has(point.teamName)) {
      continue;
    }

    if (point.roundNumber > maxRound || point.roundNumber <= 0) {
      continue;
    }

    const row = roundMap.get(point.roundNumber);
    if (!row) {
      continue;
    }

    row[point.teamName] = metricMode === "points" ? point.points : point.position;
  }

  return Array.from(roundMap.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([, value]) => value);
}

function buildRoundCoverage(fixtures: FixtureCard[]): CoverageState {
  if (fixtures.length === 0) {
    return { status: "unknown", label: "Cobertura round_id" };
  }

  const availableCount = fixtures.filter((fixture) => toPositiveInteger(fixture.roundId) !== null).length;
  const percentage = (availableCount / fixtures.length) * 100;

  if (availableCount <= 0) {
    return { status: "empty", label: "Cobertura round_id", percentage };
  }

  if (availableCount < fixtures.length) {
    return { status: "partial", label: "Cobertura round_id", percentage };
  }

  return { status: "complete", label: "Cobertura round_id", percentage: 100 };
}

export default function CompetitionPage() {
  const [selectedRound, setSelectedRound] = useState<number | null>(null);
  const [metricMode, setMetricMode] = useState<EvolutionMetric>("points");
  const [visibleTeams, setVisibleTeams] = useState<Set<string>>(new Set());

  const { competitionId, seasonId } = useGlobalFiltersState();
  const { params: timeRangeParams } = useTimeRange();

  const globalRound = useMemo(() => toPositiveInteger(timeRangeParams.roundId), [timeRangeParams.roundId]);

  useEffect(() => {
    if (globalRound !== null) {
      setSelectedRound(globalRound);
    }
  }, [globalRound]);

  const filters = useMemo<CompetitionFilters>(() => {
    const effectiveRound = selectedRound ?? globalRound;

    if (effectiveRound !== null) {
      return {
        competitionId,
        seasonId,
        roundId: String(effectiveRound),
      };
    }

    return {
      competitionId,
      seasonId,
      monthKey: timeRangeParams.monthKey,
      lastN: timeRangeParams.lastN,
      dateRangeStart: timeRangeParams.dateRangeStart,
      dateRangeEnd: timeRangeParams.dateRangeEnd,
    };
  }, [
    competitionId,
    globalRound,
    seasonId,
    selectedRound,
    timeRangeParams.dateRangeEnd,
    timeRangeParams.dateRangeStart,
    timeRangeParams.lastN,
    timeRangeParams.monthKey,
  ]);

  const standingsQuery = useQueryWithCoverage<{ items: StandingsSnapshot[] }>({
    queryKey: competitionQueryKeys.standings(filters),
    queryFn: () => fetchStandings(filters),
    staleTime: 5 * 60 * 1000,
    gcTime: 20 * 60 * 1000,
    isDataEmpty: (data) => !Array.isArray(data.items) || data.items.length === 0,
  });

  const evolutionQuery = useQueryWithCoverage<{ items: StandingsEvolutionPoint[] }>({
    queryKey: competitionQueryKeys.evolution(filters),
    queryFn: () => fetchStandingsEvolution(filters),
    staleTime: 5 * 60 * 1000,
    gcTime: 20 * 60 * 1000,
    isDataEmpty: (data) => !Array.isArray(data.items) || data.items.length === 0,
  });

  const fixturesQuery = useQueryWithCoverage<{ items: FixtureCard[] }>({
    queryKey: competitionQueryKeys.fixtures(filters),
    queryFn: () => fetchCompetitionFixtures(filters),
    staleTime: 2 * 60 * 1000,
    gcTime: 20 * 60 * 1000,
    isDataEmpty: (data) => !Array.isArray(data.items) || data.items.length === 0,
  });

  const rawStandings = useMemo(
    () => standingsQuery.data?.items ?? [],
    [standingsQuery.data?.items],
  );
  const evolution = useMemo(
    () => evolutionQuery.data?.items ?? [],
    [evolutionQuery.data?.items],
  );
  const fixtures = useMemo(
    () => fixturesQuery.data?.items ?? [],
    [fixturesQuery.data?.items],
  );

  const standings = useMemo<StandingRow[]>(() => {
    const teamCount = rawStandings.length;
    return rawStandings
      .map((row) => toStandingRow(row, teamCount))
      .sort((a, b) => a.position - b.position);
  }, [rawStandings]);

  const availableRounds = useMemo(() => {
    const rounds = new Set<number>();

    for (const point of evolution) {
      const round = toPositiveInteger(point.roundNumber);
      if (round !== null) {
        rounds.add(round);
      }
    }

    for (const fixture of fixtures) {
      const round = toPositiveInteger(fixture.roundId);
      if (round !== null) {
        rounds.add(round);
      }
    }

    return Array.from(rounds).sort((a, b) => a - b);
  }, [evolution, fixtures]);

  const fallbackRound = selectedRound ?? globalRound ?? 1;
  const currentRound = availableRounds.length > 0 ? availableRounds[availableRounds.length - 1] : fallbackRound;
  const selectedRoundValue = selectedRound ?? currentRound;
  const totalRounds = Math.max(currentRound, selectedRoundValue, 1);

  useEffect(() => {
    if (selectedRound === null && currentRound > 0) {
      setSelectedRound(currentRound);
    }
  }, [currentRound, selectedRound]);

  const topTeams = useMemo(() => standings.slice(0, 6).map((row) => row.teamName), [standings]);

  useEffect(() => {
    setVisibleTeams(new Set(topTeams));
  }, [topTeams]);

  const fixturesForRound = useMemo(() => {
    if (fixtures.length === 0) {
      return [];
    }

    return fixtures.filter((fixture) => {
      const fixtureRound = toPositiveInteger(fixture.roundId);
      if (fixtureRound === null) {
        return selectedRoundValue === currentRound;
      }
      return fixtureRound === selectedRoundValue;
    });
  }, [currentRound, fixtures, selectedRoundValue]);

  const evolutionChartData = useMemo(
    () => buildEvolutionChartData(evolution, topTeams, selectedRoundValue, metricMode),
    [evolution, metricMode, selectedRoundValue, topTeams],
  );

  const roundCoverage = useMemo(() => buildRoundCoverage(fixtures), [fixtures]);
  const leader = standings[0];

  const totalGoals = fixturesForRound.reduce(
    (sum, fixture) => sum + (fixture.homeGoals ?? 0) + (fixture.awayGoals ?? 0),
    0,
  );
  const yAxisMaxPosition = Math.max(1, ...standings.map((row) => row.position));

  const hasBlockingError =
    (standingsQuery.isError && standings.length === 0) ||
    (evolutionQuery.isError && evolution.length === 0) ||
    (fixturesQuery.isError && fixtures.length === 0);

  const toggleTeamVisibility = (teamName: string) => {
    setVisibleTeams((current) => {
      const next = new Set(current);

      if (next.has(teamName)) {
        if (next.size === 1) {
          return current;
        }

        next.delete(teamName);
        return next;
      }

      next.add(teamName);
      return next;
    });
  };

  return (
    <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6 md:-mx-6 md:-mt-6 md:px-6 md:pt-8">
      <div className="mx-auto max-w-7xl space-y-6">
        <header className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-slate-100">Competição</h1>
            <p className="mt-1 text-sm text-slate-400">
              Navegação por rodada, tabela, evolução de pontos e calendário
            </p>
          </div>
          <CoverageBadge coverage={standingsQuery.coverage} />
        </header>

        {hasBlockingError ? (
          <section className="rounded-xl border border-red-500/30 bg-red-500/10 p-4 text-sm text-red-300">
            <p className="font-medium">Falha ao carregar dados da competição.</p>
            <p className="mt-1 text-red-400/80">
              {standingsQuery.error?.message ?? evolutionQuery.error?.message ?? fixturesQuery.error?.message}
            </p>
          </section>
        ) : null}

        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <MiniKpi accent="#10B981" icon={Trophy} label="Líder" value={leader?.teamName ?? "-"} />
          <MiniKpi accent="#1E40AF" icon={Shield} label="Pontos do líder" value={leader?.points ?? "-"} />
          <MiniKpi accent="#F97316" icon={Swords} label={`Gols — Rodada ${selectedRoundValue}`} value={totalGoals} />
          <MiniKpi accent="#06B6D4" icon={Activity} label="Jogos da rodada" value={fixturesForRound.length} />
        </div>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <header className="mb-4">
            <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-400">Navegador de Rodadas</h2>
          </header>
          <RoundNavigator
            currentRound={currentRound}
            onSelect={setSelectedRound}
            selectedRound={selectedRoundValue}
            totalRounds={totalRounds}
          />
        </section>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <header className="mb-4 flex items-start justify-between gap-3">
            <div className="space-y-1">
              <h2 className="text-lg font-semibold text-slate-100">Classificação</h2>
              <p className="text-sm text-slate-400">Snapshot após a rodada selecionada</p>
            </div>
          </header>
          <StandingsTable loading={standingsQuery.isLoading} rows={standings} />
        </section>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <header className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-slate-100">Curva de Desempenho</h2>
              <p className="text-sm text-slate-400">Pontos acumulados ou posição no campeonato</p>
            </div>
            <div className="flex rounded-lg border border-slate-700 bg-slate-950/70 p-1">
              <button
                className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${metricMode === "points"
                  ? "bg-emerald-500/15 text-emerald-300"
                  : "text-slate-400 hover:text-slate-200"
                  }`}
                onClick={() => setMetricMode("points")}
                type="button"
              >
                Pontos acumulados
              </button>
              <button
                className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${metricMode === "position"
                  ? "bg-emerald-500/15 text-emerald-300"
                  : "text-slate-400 hover:text-slate-200"
                  }`}
                onClick={() => setMetricMode("position")}
                type="button"
              >
                Posição
              </button>
            </div>
          </header>

          {topTeams.length === 0 ? (
            <div className="rounded-lg border border-slate-700 bg-slate-900 p-6 text-center text-sm text-slate-400">
              Sem dados de evolução para o recorte atual.
            </div>
          ) : (
            <>
              <div className="mb-3 flex flex-wrap gap-2">
                {topTeams.map((teamName, index) => {
                  const isVisible = visibleTeams.has(teamName);
                  return (
                    <button
                      className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${isVisible
                        ? "border-emerald-400 bg-emerald-500/20 text-emerald-200"
                        : "border-slate-600 bg-slate-800 text-slate-300 hover:bg-slate-700"
                        }`}
                      key={teamName}
                      onClick={() => toggleTeamVisibility(teamName)}
                      type="button"
                    >
                      <span
                        className="mr-1.5 inline-block h-2 w-2 rounded-full"
                        style={{ backgroundColor: TEAM_COLORS[index % TEAM_COLORS.length] }}
                      />
                      {teamName}
                    </button>
                  );
                })}
              </div>

              <div className="h-[320px] rounded-lg border border-slate-700 bg-slate-900 p-3">
                <ResponsiveContainer height="100%" width="100%">
                  <RechartsLineChart data={evolutionChartData}>
                    <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                    <XAxis dataKey="roundLabel" stroke="#94A3B8" tick={{ fontSize: 12 }} />
                    <YAxis
                      allowDecimals={false}
                      domain={metricMode === "position" ? [1, yAxisMaxPosition] : ["auto", "auto"]}
                      reversed={metricMode === "position"}
                      stroke="#94A3B8"
                      tick={{ fontSize: 12 }}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: "#0F172A",
                        border: "1px solid #334155",
                        borderRadius: "8px",
                        color: "#F1F5F9",
                      }}
                      cursor={{ stroke: "#64748B", strokeWidth: 1 }}
                      itemStyle={{ color: "#F1F5F9" }}
                      labelStyle={{ color: "#CBD5E1" }}
                    />
                    {topTeams.map((teamName, index) =>
                      visibleTeams.has(teamName) ? (
                        <Line
                          dataKey={teamName}
                          dot={false}
                          key={teamName}
                          stroke={TEAM_COLORS[index % TEAM_COLORS.length]}
                          strokeWidth={2}
                          type="monotone"
                        />
                      ) : null,
                    )}
                  </RechartsLineChart>
                </ResponsiveContainer>
              </div>
            </>
          )}
        </section>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <header className="mb-4 flex items-start justify-between gap-3">
            <div className="space-y-1">
              <h2 className="text-lg font-semibold text-slate-100">Calendário da Rodada</h2>
              <p className="text-sm text-slate-400">Resultados e jogos agendados da rodada</p>
            </div>
          </header>
          <FixturesList fixtures={fixturesForRound} loading={fixturesQuery.isLoading} />
        </section>

        <section className="rounded-xl border border-slate-700/80 bg-slate-900/80 p-5 shadow-lg shadow-slate-950/30 backdrop-blur-sm">
          <header className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-slate-100">Qualidade do Recorte</h2>
              <p className="text-sm text-slate-400">Cobertura de classificação, evolução e calendário</p>
            </div>
          </header>

          <div className="flex flex-wrap items-center gap-2">
            <CoverageBadge coverage={standingsQuery.coverage} />
            <CoverageBadge coverage={evolutionQuery.coverage} />
            <CoverageBadge coverage={roundCoverage} />
          </div>

          <p className="mt-3 text-xs text-slate-500">
            Rodada selecionada: {selectedRoundValue} | Rodada mais recente encontrada: {currentRound}
          </p>
        </section>
      </div>
    </div>
  );
}
