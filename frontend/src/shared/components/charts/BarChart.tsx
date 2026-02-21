"use client";

import { useMemo } from "react";

import {
  Bar,
  BarChart as RechartsBarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { formatChartValue } from "@/shared/components/charts/chart-formatters";

type BarSeriesDefinition = {
  dataKey: string;
  label?: string;
  color?: string;
  metricKey?: string;
};

type BarChartProps<TData extends Record<string, unknown>> = {
  data: TData[];
  xKey: keyof TData & string;
  bars: BarSeriesDefinition[];
  className?: string;
  height?: number;
  showLegend?: boolean;
  yAxisMetricKey?: string;
};

const DEFAULT_BAR_COLORS = ["#0f172a", "#0369a1", "#16a34a", "#9333ea", "#dc2626"];

export function BarChart<TData extends Record<string, unknown>>({
  data,
  xKey,
  bars,
  className,
  height = 280,
  showLegend = true,
  yAxisMetricKey,
}: BarChartProps<TData>) {
  const classes = ["w-full rounded-lg border border-slate-200 bg-white p-3", className].filter(Boolean).join(" ");
  const seriesMap = useMemo(
    () =>
      new Map(
        bars.map((series, index) => [
          series.dataKey,
          {
            ...series,
            color: series.color ?? DEFAULT_BAR_COLORS[index % DEFAULT_BAR_COLORS.length],
          },
        ]),
      ),
    [bars],
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
          <RechartsBarChart data={data}>
            <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
            <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
            <YAxis
              tick={{ fontSize: 12 }}
              tickFormatter={(value: number) => {
                return formatChartValue(value, yAxisMetricKey);
              }}
            />
            <Tooltip
              formatter={(value: unknown, name: string | undefined) => {
                const safeName = name ?? "";
                const series = seriesMap.get(safeName);
                const metricKey = series?.metricKey ?? yAxisMetricKey;
                return [formatChartValue(value, metricKey), series?.label ?? safeName];
              }}
            />
            {showLegend ? <Legend /> : null}
            {Array.from(seriesMap.values()).map((series) => (
              <Bar dataKey={series.dataKey} fill={series.color} key={series.dataKey} name={series.label ?? series.dataKey} />
            ))}
          </RechartsBarChart>
        </ResponsiveContainer>
      </div>
    </section>
  );
}
