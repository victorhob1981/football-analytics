"use client";

import { ArrowRightLeft } from "lucide-react";

import { ComingSoonPage } from "@/shared/components/feedback/ComingSoonPage";

export default function MarketPage() {
  return (
    <ComingSoonPage
      description="Transferências, afastamentos e painel de disponibilidade"
      icon={ArrowRightLeft}
      title="Mercado e Disponibilidade"
    />
  );
}
