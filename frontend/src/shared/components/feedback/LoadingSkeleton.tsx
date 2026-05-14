type LoadingSkeletonProps = {
  className?: string;
  width?: string | number;
  height?: string | number;
  rounded?: "none" | "sm" | "md" | "full";
};

const ROUNDED_CLASS_BY_SIZE: Record<NonNullable<LoadingSkeletonProps["rounded"]>, string> = {
  none: "rounded-none",
  sm: "rounded-sm",
  md: "rounded-md",
  full: "rounded-full",
};

export function LoadingSkeleton({
  className,
  width = "100%",
  height = 16,
  rounded = "md",
}: LoadingSkeletonProps) {
  const classes = ["animate-pulse bg-slate-200", ROUNDED_CLASS_BY_SIZE[rounded], className].filter(Boolean).join(" ");

  return (
    <span
      aria-label="Carregando"
      aria-live="polite"
      className={classes}
      role="status"
      style={{ width, height, display: "inline-block" }}
    />
  );
}
