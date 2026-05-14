export function formatNumber(value: number, precision = 0): string {
  return new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: precision,
    maximumFractionDigits: precision,
  }).format(value);
}

export function formatPercentage(value: number, precision = 1): string {
  return `${formatNumber(value, precision)}%`;
}

export function formatRating(value: number, precision = 2): string {
  return formatNumber(value, precision);
}

export function formatSeconds(value: number, precision = 0): string {
  return `${formatNumber(value, precision)} s`;
}

export function formatMinutes(value: number, precision = 0): string {
  return `${formatNumber(value, precision)} min`;
}

export function formatWithUnit(value: string, unit?: string): string {
  if (!unit) {
    return value;
  }

  return `${value} ${unit}`;
}

export function formatDate(value: Date | string | null | undefined): string {
  if (!value) {
    return "-";
  }

  const parsedDate = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(parsedDate.getTime())) {
    return "-";
  }

  return new Intl.DateTimeFormat("pt-BR").format(parsedDate);
}
