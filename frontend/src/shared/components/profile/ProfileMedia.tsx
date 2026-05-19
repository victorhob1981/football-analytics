"use client";

import { useState } from "react";

import Image from "next/image";

type VisualAssetCategory = "clubs" | "competitions" | "players";

type ProfileMediaProps = {
  alt: string;
  assetId: number | string | null | undefined;
  category: VisualAssetCategory;
  fallback: string;
  className?: string;
  fallbackClassName?: string;
  imageClassName?: string;
  shape?: "rounded" | "circle";
  tone?: "base" | "contrast";
};

function joinClasses(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(" ");
}

export function buildVisualAssetUrl(
  category: VisualAssetCategory,
  assetId: number | string | null | undefined,
): string | null {
  if (assetId === null || assetId === undefined) {
    return null;
  }

  const normalizedAssetId = String(assetId).trim();

  if (normalizedAssetId.length === 0) {
    return null;
  }

  return `/api/visual-assets/${category}/${normalizedAssetId}`;
}

export function ProfileMedia({
  alt,
  assetId,
  category,
  fallback,
  className,
  fallbackClassName,
  imageClassName,
  shape = "rounded",
  tone = "base",
}: ProfileMediaProps) {
  const [hasError, setHasError] = useState(false);
  const assetUrl = buildVisualAssetUrl(category, assetId);

  return (
    <div
      className={joinClasses(
        "relative flex shrink-0 items-center justify-center overflow-hidden",
        shape === "circle" ? "rounded-full" : "rounded-[1.2rem]",
        tone === "contrast"
          ? "border border-white/12 bg-white/12 text-white"
          : "border border-white/60 bg-[rgba(240,243,255,0.9)] text-[#003526]",
        className,
      )}
    >
      {assetUrl && !hasError ? (
        <Image
          alt={alt}
          className={joinClasses("object-contain p-2", imageClassName)}
          fill
          onError={() => {
            setHasError(true);
          }}
          sizes="96px"
          src={assetUrl}
          unoptimized
        />
      ) : (
        <span
          className={joinClasses(
            "px-2 text-center font-[family:var(--font-app-headline)] text-xs font-extrabold uppercase tracking-[0.12em]",
            fallbackClassName,
          )}
        >
          {fallback}
        </span>
      )}
    </div>
  );
}
