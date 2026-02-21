"use client";

import { useMemo } from "react";

import {
  Legend,
  PolarAngleAxis,
  PolarGrid,
  PolarRadiusAxis,
  Radar,
  RadarChart as RechartsRadarChart,
  ResponsiveContainer,
  Tooltip,
} from "recharts";

import { formatChartValue } from "@/shared/components/charts/chart-formatters";

type RadarSeriesDefinition = {
  dataKey: string;
  label?: string;
  color?: string;
  metricKey?: string;
};

type RadarChartProps<TData extends Record<string, unknown>> = {
  data: TData[];
  angleKey: keyof TData & string;
  radars: RadarSeriesDefinition[];
  className?: string;
  height?: number;
  showLegend?: boolean;
};

const DEFAULT_RADAR_COLORS = ["#0f172a", "#0369a1", "#16a34a", "#9333ea"];

export function RadarChart<TData extends Record<string, unknown>>({
  data,
  angleKey,
  radars,
  className,
  height = 320,
  showLegend = true,
}: RadarChartProps<TData>) {
  const classes = ["w-full rounded-lg border border-slate-200 bg-white p-3", className].filter(Boolean).join(" ");
  const seriesMap = useMemo(
    () =>
      new Map(
        radars.map((series, index) => [
          series.dataKey,
          {
            ...series,
            color: series.color ?? DEFAULT_RADAR_COLORS[index % DEFAULT_RADAR_COLORS.length],
          },
        ]),
      ),
    [radars],
  );

  if (data.length === 0) {
    return (
      <section className={classes}>
        <p className="text-sm text-slate-500">Sem dados para grafico.</p>
      </section>
    );
  }

  return (
    <section className={classes}>
      <div style={{ height }}>
        <ResponsiveContainer height="100%" width="100%">
          <RechartsRadarChart data={data}>
            <PolarGrid />
            <PolarAngleAxis dataKey={angleKey} tick={{ fontSize: 12 }} />
            <PolarRadiusAxis tick={{ fontSize: 11 }} />
            <Tooltip
              formatter={(value: unknown, name: string | undefined) => {
                const safeName = name ?? "";
                const series = seriesMap.get(safeName);
                return [formatChartValue(value, series?.metricKey), series?.label ?? safeName];
              }}
            />
            {showLegend ? <Legend /> : null}
            {Array.from(seriesMap.values()).map((series) => (
              <Radar
                dataKey={series.dataKey}
                fill={series.color}
                fillOpacity={0.2}
                key={series.dataKey}
                name={series.label ?? series.dataKey}
                stroke={series.color}
              />
            ))}
          </RechartsRadarChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
