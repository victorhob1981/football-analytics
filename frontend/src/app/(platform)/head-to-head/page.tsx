"use client";

import { Suspense } from "react";

import { Swords } from "lucide-react";

import { ComingSoonPage } from "@/shared/components/feedback/ComingSoonPage";

function H2HPageInner() {
  return (
    <ComingSoonPage
      description="Confrontos diretos, dominância e histórico entre dois clubes"
      icon={Swords}
      title="Head-to-Head"
    />
  );
}

export default function HeadToHeadPage() {
  return (
    <Suspense fallback={
      <div className="home-gradient-bg -mx-4 -mt-4 px-4 pb-8 pt-6">
        <div className="mx-auto max-w-3xl">
          <div className="h-[300px] animate-pulse rounded-xl border border-slate-700 bg-slate-900" />
        </div>
      </div>
    }>
      <H2HPageInner />
    </Suspense>
  );
}
