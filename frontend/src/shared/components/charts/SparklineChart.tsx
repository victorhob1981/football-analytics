"use client";

import { Line, LineChart as RechartsLineChart, ResponsiveContainer, Tooltip } from "recharts";

import { formatChartValue } from "@/shared/components/charts/chart-formatters";

type SparklineChartProps<TData extends Record<string, unknown>> = {
  data: TData[];
  dataKey: keyof TData & string;
  metricKey?: string;
  className?: string;
  color?: string;
  height?: number;
  showTooltip?: boolean;
};

export function SparklineChart<TData extends Record<string, unknown>>({
  data,
  dataKey,
  metricKey,
  className,
  color = "#0f172a",
  height = 40,
  showTooltip = false,
}: SparklineChartProps<TData>) {
  const classes = ["w-full", className].filter(Boolean).join(" ");

  if (data.length === 0) {
    return <span className="text-xs text-slate-500">-</span>;
  }

  return (
    <div className={classes} style={{ height }}>
      <ResponsiveContainer height="100%" width="100%">
        <RechartsLineChart data={data}>
          {showTooltip ? (
            <Tooltip
              formatter={(value: unknown) => {
                return [formatChartValue(value, metricKey)];
              }}
              labelFormatter={() => ""}
            />
          ) : null}
          <Line dataKey={dataKey} dot={false} isAnimationActive={false} stroke={color} strokeWidth={2} type="monotone" />
        </RechartsLineChart>
      </ResponsiveContainer>
    </div>
  );
}
