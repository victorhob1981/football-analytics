"use client";

import { useEffect, useState } from "react";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart as RechartsLineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { EmptyState } from "@/shared/components/feedback/EmptyState";
import { LoadingSkeleton } from "@/shared/components/feedback/LoadingSkeleton";
import { useStandingsEvolution } from "@/features/home/hooks";

const TEAM_COLORS = ["#10B981", "#1E40AF", "#F97316", "#06B6D4", "#EC4899", "#8B5CF6"];

function EvolutionSkeleton() {
  return (
    <div className="space-y-3 rounded-lg border border-slate-700 bg-slate-900 p-4">
      <LoadingSkeleton className="bg-slate-700" height={28} width="60%" />
      <LoadingSkeleton className="bg-slate-700" height={300} />
    </div>
  );
}

export function StandingsEvolutionSection() {
  const { data, isLoading, isEmpty } = useStandingsEvolution();
  const [visibleTeams, setVisibleTeams] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!data) {
      return;
    }

    setVisibleTeams(new Set(data.teamNames));
  }, [data]);

  if (isLoading) {
    return <EvolutionSkeleton />;
  }

  if (isEmpty || !data || data.series.length === 0 || data.teamNames.length === 0) {
    return (
      <EmptyState
        className="rounded-xl border-slate-700 bg-slate-900 [&_h3]:text-slate-100 [&_p]:text-slate-400"
        description="Dados de evolucao de pontos nao disponiveis para o recorte atual."
        title="Sem dados de evolucao"
      />
    );
  }

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
    <div className="space-y-4">
      <div className="flex flex-wrap gap-2">
        {data.teamNames.map((teamName, index) => {
          const isVisible = visibleTeams.has(teamName);

          return (
            <button
              className={`rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
                isVisible
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

      <div className="h-[320px] w-full rounded-lg border border-slate-700 bg-slate-900 p-3">
        <ResponsiveContainer height="100%" width="100%">
          <RechartsLineChart data={data.series}>
            <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
            <XAxis dataKey="roundLabel" stroke="#94A3B8" tick={{ fontSize: 12 }} />
            <YAxis stroke="#94A3B8" tick={{ fontSize: 12 }} />
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
            <Legend wrapperStyle={{ color: "#CBD5E1", fontSize: 12 }} />
            {data.teamNames.map((teamName, index) => {
              if (!visibleTeams.has(teamName)) {
                return null;
              }

              return (
                <Line
                  dataKey={teamName}
                  dot={false}
                  key={teamName}
                  stroke={TEAM_COLORS[index % TEAM_COLORS.length]}
                  strokeWidth={2}
                  type="monotone"
                />
              );
            })}
          </RechartsLineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
