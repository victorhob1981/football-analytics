"use client";

import { GitCompare } from "lucide-react";

import { ComingSoonPage } from "@/shared/components/feedback/ComingSoonPage";

export default function PlayersComparePage() {
    return (
        <ComingSoonPage
            description="KPIs, radar de perfil, forma recente e contexto de escalação"
            icon={GitCompare}
            title="Comparativo de Jogadores"
        />
    );
}
